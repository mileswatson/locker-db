use std::{mem::replace, path::PathBuf, sync::Arc};

use rocket::{
    serde::{DeserializeOwned, Serialize},
    tokio::sync::RwLock,
};

use crate::{
    core::key::Key,
    sstables::{sstable_builder::SSTableBuilder, write_buffer::WriteBuffer},
};

use super::{lsm_tree::LSMTree, sstable_node::SSTableNode};

pub(super) struct LSMTreeWriter<T: Serialize + DeserializeOwned> {
    tree: Arc<RwLock<LSMTree<T>>>,
}

async fn merge_into_node<T: Serialize + DeserializeOwned>(
    current: &RwLock<Arc<SSTableNode<T>>>,
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

            *lock = Arc::new(SSTableNode::new(
                merged,
                second.next().await.map(RwLock::new),
            ));
        }
    }
}

impl<T: Serialize + DeserializeOwned + Clone> LSMTreeWriter<T> {
    fn check_deletion(self) -> Option<LSMTreeWriter<T>> {
        match Arc::try_unwrap(self.tree) {
            Ok(x) => {
                drop(x.into_inner());
                None
            }
            Err(x) => Some(LSMTreeWriter { tree: x }),
        }
    }

    pub async fn run(mut self) {
        loop {
            self = q!(self.check_deletion());
            self.merge().await;
            self.new_buffer().await;
        }
    }

    async fn new_buffer(&mut self) {
        let dir = {
            let lock = self.tree.read().await;
            lock.dir.clone()
        };

        let new_wb = WriteBuffer::create(dir.join("wals").join(Key::new().hex())).await;

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
            lock.first = Some(RwLock::new(Arc::new(SSTableNode::new(
                table,
                replace(&mut lock.first, None),
            ))));
            lock.builders.pop_back();
        }

        let mut current = {
            let lock = tree.read().await;
            q!(merge_into_node(q!(lock.first.as_ref())).await)
        };

        loop {
            let lock = q!(current.next_lock().await.as_ref());
            current = q!(merge_into_node(lock).await);
        }
    }

    pub(crate) fn new(tree: Arc<RwLock<LSMTree<T>>>) -> LSMTreeWriter<T> {
        LSMTreeWriter { tree }
    }
}
