use std::{mem::replace, path::PathBuf, sync::Arc};

use arc_swap::ArcSwap;
use parking_lot::RwLock;
use rocket::serde::{DeserializeOwned, Serialize};

use crate::sstables::{sstable_builder::SSTableBuilder, write_buffer::WriteBuffer};

use super::{
    lsm_tree::{Heap, LSMTree},
    sstable_node::SSTableNode,
};

pub(super) struct LSMTreeService<T: Serialize + DeserializeOwned> {
    tree: Arc<RwLock<LSMTree<T>>>,
}

async fn merge_into_node<T: Serialize + DeserializeOwned>(
    current: &ArcSwap<Option<SSTableNode<T>>>,
    heap: &Heap<T>,
) -> Arc<Option<SSTableNode<T>>> {
    loop {
        let first = current.load_full();
        let first = match first.as_ref() {
            Some(x) => x,
            None => return first,
        };

        let second = first.next();
        let second = match second.as_ref() {
            Some(x) => {
                if (3 * first.len()) / 4 <= x.len() {
                    return second.clone();
                }
                x
            }
            None => return second.clone(),
        };
        let merged = SSTableBuilder::merge(first, second, PathBuf::from("./alsonew")).await;
        {
            current.store(SSTableNode::new(merged, ArcSwap::from(second.next()), heap));
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
                let lock = self.tree.write();
                let mut nodes = lock.heap.lock();
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
            for x in garbage.into_iter().flatten() {
                x.delete().await;
            }
        }
    }

    async fn new_buffer(&mut self) {
        let dir = {
            let lock = self.tree.read();
            lock.dir.clone()
        };

        let new_wb = WriteBuffer::create(dir.join("wals")).await;

        let file = {
            let mut lock = self.tree.write();
            let old_buffer = replace(&mut lock.buffer, new_wb);
            let (builder, file) = old_buffer.to_builder();
            lock.builders.push_front(builder);
            file
        };
        file.close().await.unwrap();
    }

    async fn merge(&mut self) {
        let tree = &self.tree;

        let (builder, dir) = {
            let lock = tree.read();
            (q!(lock.builders.back()).clone(), lock.dir.clone())
        };

        let table = builder.build(&dir.join("tables")).await;

        {
            let mut lock = tree.write();
            {
                let x = lock.first.load_full();
                lock.first
                    .store(SSTableNode::new(table, ArcSwap::from(x), &lock.heap));
            }
            lock.builders.pop_back();
        }

        let (first, heap) = {
            let lock = tree.read();
            (lock.first.clone(), lock.heap.clone())
        };

        let mut current = merge_into_node(first.as_ref(), &heap).await;

        loop {
            current = match current.as_ref() {
                Some(current) => merge_into_node(current.next_lock(), &heap).await,
                None => return,
            };
        }
    }

    async fn save(&mut self) {
        let (state, dir) = {
            let lock = self.tree.read();
            (lock.state(), lock.dir.clone())
        };
        state.save(&dir).await
    }

    pub(crate) fn new(tree: Arc<RwLock<LSMTree<T>>>) -> LSMTreeService<T> {
        LSMTreeService { tree }
    }
}
