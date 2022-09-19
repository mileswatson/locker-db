use std::{mem::replace, path::Path, sync::Arc};

use arc_swap::ArcSwap;
use rocket::serde::{DeserializeOwned, Serialize};

use crate::sstables::{sstable_builder::SSTableBuilder, write_buffer::WriteBuffer};

use super::{
    lsm_tree::{Heap, LSMTree},
    sstable_node::SSTableNode,
};

pub(super) struct LSMTreeService<T: Serialize + DeserializeOwned> {
    tree: Arc<LSMTree<T>>,
}

async fn merge_into_node<T: Serialize + DeserializeOwned>(
    current: &ArcSwap<Option<SSTableNode<T>>>,
    heap: &Heap<T>,
    dir: &Path,
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
        let merged = SSTableBuilder::merge(first, second, dir).await;
        {
            current.store(SSTableNode::new(merged, ArcSwap::from(second.next()), heap));
        }
    }
}

impl<T: Serialize + DeserializeOwned + Clone> LSMTreeService<T> {
    fn check_deletion(self) -> Option<LSMTreeService<T>> {
        match Arc::try_unwrap(self.tree) {
            Ok(x) => {
                drop(x);
                None
            }
            Err(x) => Some(LSMTreeService { tree: x }),
        }
    }

    pub async fn run(mut self) {
        loop {
            self = q!(self.check_deletion());
            if self.merge().await || self.new_buffer().await {
                self.save().await
            }
            self.prune_dag().await;
        }
    }

    async fn prune_dag(&mut self) {
        loop {
            let garbage: Vec<_> = {
                let mut nodes = self.tree.heap.lock();
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

    async fn new_buffer(&mut self) -> bool {
        let dir = self.tree.dir.clone();
        {
            let lock = self.tree.buffers.read().await;
            if lock.buffer.size() <= 5 {
                return false;
            }
        }
        let new_wb = WriteBuffer::create(dir.join("wals")).await;
        {
            let mut lock = self.tree.buffers.write().await;
            let old_buffer = replace(&mut lock.buffer, new_wb);
            let builder = old_buffer.to_builder().await;
            lock.builders.push_front(builder);
        };
        true
    }

    async fn merge(&mut self) -> bool {
        let tree = &self.tree;

        let builder = {
            let lock = tree.buffers.read().await;
            match lock.builders.back() {
                Some(x) => x.clone(),
                None => return false,
            }
        };

        let table = builder.build(&tree.dir.join("tables")).await;

        {
            let mut lock = tree.buffers.write().await;
            let first = tree.first.load_full();
            tree.first
                .store(SSTableNode::new(table, ArcSwap::from(first), &tree.heap));
            lock.builders.pop_back();
        }

        let mut current = merge_into_node(tree.first.as_ref(), &tree.heap, &tree.dir).await;

        loop {
            match current.as_ref() {
                Some(c) => {
                    current = merge_into_node(c.next_lock(), &tree.heap, &tree.dir).await;
                }
                None => return true,
            };
        }
    }

    async fn save(&mut self) {
        self.tree.state().await.save(&self.tree.dir).await
    }

    pub(crate) fn new(tree: Arc<LSMTree<T>>) -> LSMTreeService<T> {
        LSMTreeService { tree }
    }
}
