use std::{mem::replace, path::PathBuf, sync::Arc};

use rocket::{
    serde::{DeserializeOwned, Serialize},
    tokio::sync::RwLock,
};

use crate::sstables::{sstable_builder::SSTableBuilder, write_buffer::WriteBuffer};

use super::{
    lsm_tree::{Heap, LSMTree},
    sstable_node::SSTableNode,
};

pub(super) struct LSMTreeService<T: Serialize + DeserializeOwned> {
    tree: Arc<RwLock<LSMTree<T>>>,
}

async fn merge_into_node<T: Serialize + DeserializeOwned>(
    current: &RwLock<Arc<SSTableNode<T>>>,
    heap: &Heap<T>,
) -> Option<Arc<SSTableNode<T>>> {
    loop {
        let second = {
            let lock = current.read().await;

            let second = lock.next().await?;

            if (3 * lock.len()) / 4 <= second.len() {
                break Some(second);
            }

            second
        };
        {
            let mut lock = current.write().await;

            let merged = SSTableBuilder::merge(&lock, &second, PathBuf::from("./alsonew")).await;

            *lock = SSTableNode::new(merged, second.next().await.map(RwLock::new), heap);
        }
    }
}

impl<T: Serialize + DeserializeOwned + Clone> LSMTreeService<T> {
    fn check_deletion(self) -> Option<LSMTreeService<T>> {
        match Arc::try_unwrap(self.tree) {
            Ok(x) => {
                drop(x.into_inner());
                None
            }
            Err(x) => Some(LSMTreeService { tree: x }),
        }
    }

    pub async fn run(mut self) {
        loop {
            self = q!(self.check_deletion());
            self.merge().await;
            self.save().await;
            self.new_buffer().await;
            self.save().await;
            self.prune_dag().await;
            self.save().await;
        }
    }

    async fn prune_dag(&mut self) {
        loop {
            let garbage: Vec<_> = {
                let lock = self.tree.write().await;
                let mut nodes = lock.heap.lock().await;
                let keys: Vec<_> = nodes
                    .iter()
                    .filter(|x| Arc::strong_count(x.1) == 1)
                    .map(|x| x.0)
                    .cloned()
                    .collect();
                keys.iter()
                    .map(|x| {
                        Arc::try_unwrap(nodes.remove(x).unwrap())
                            .map_err(|_| ())
                            .unwrap()
                    })
                    .collect()
            };
            if garbage.is_empty() {
                break;
            }
            for x in garbage.into_iter() {
                x.delete().await;
            }
        }
    }

    async fn new_buffer(&mut self) {
        let dir = {
            let lock = self.tree.read().await;
            lock.dir.clone()
        };

        let new_wb = WriteBuffer::create(dir.join("wals")).await;

        {
            let mut lock = self.tree.write().await;
            let old_buffer = replace(&mut lock.buffer, new_wb);
            lock.builders.push_front(old_buffer.to_builder().await);
        }
    }

    async fn merge(&mut self) {
        let tree = &self.tree;

        let (builder, dir) = {
            let lock = tree.read().await;
            (q!(lock.builders.back()).clone(), lock.dir.clone())
        };

        let table = builder.build(&dir.join("tables")).await;

        {
            let mut lock = tree.write().await;
            let x = replace(&mut lock.first, None);
            let first = Some(RwLock::new(SSTableNode::new(table, x, &lock.heap)));

            lock.first = first;
            lock.builders.pop_back();
        }

        let mut current = {
            let lock = tree.read().await;
            q!(merge_into_node(q!(lock.first.as_ref()), &lock.heap).await)
        };

        loop {
            let tree = tree.read().await;
            let lock = q!(current.next_lock().await.as_ref());
            current = q!(merge_into_node(lock, &tree.heap).await);
        }
    }

    async fn save(&mut self) {
        let (state, dir) = {
            let lock = self.tree.read().await;
            (lock.state().await, lock.dir.clone())
        };
        state.save(&dir).await
    }

    pub(crate) fn new(tree: Arc<RwLock<LSMTree<T>>>) -> LSMTreeService<T> {
        LSMTreeService { tree }
    }
}
