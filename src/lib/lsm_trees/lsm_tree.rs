use std::collections::VecDeque;
use std::mem::replace;
use std::path::PathBuf;
use std::sync::Arc;

use rocket::serde::{DeserializeOwned, Serialize};
use rocket::tokio::spawn;
use rocket::tokio::sync::RwLock;

use crate::core::key::Key;
use crate::core::rlock::RLock;
use crate::sstables::sstable_builder::SSTableBuilder;
use crate::sstables::write_buffer::WriteBuffer;

use super::sstable_node::{NextSSTable, SSTableNode};

macro_rules! q {
    ( $e:expr ) => {
        match $e {
            Some(x) => x,
            None => return,
        }
    };
}

struct LSMTree<T: Serialize + DeserializeOwned> {
    buffer: WriteBuffer<T>,
    builders: VecDeque<SSTableBuilder<T>>,
    first: NextSSTable<T>,
}

pub struct LSMTreeReader<T: Serialize + DeserializeOwned> {
    internal: RLock<LSMTree<T>>,
}

async fn merge_into_node<T: Serialize + DeserializeOwned>(
    current_lock: &RwLock<Arc<SSTableNode<T>>>,
) -> Option<Arc<SSTableNode<T>>> {
    loop {
        let current = current_lock.read().await;

        let second = current.next().await?;

        if (3 * current.len()) / 4 <= second.len() {
            break Some(second.clone());
        }

        drop(current);
        let mut current = current_lock.write().await;

        let merged = SSTableBuilder::merge(&current, &second, PathBuf::from("./alsonew")).await;

        *current = Arc::new(SSTableNode::new(
            merged,
            second.next().await.map(RwLock::new),
        ));
    }
}

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> LSMTreeReader<T> {
    pub async fn new() -> LSMTreeReader<T> {
        let lock = Arc::new(RwLock::new(LSMTree {
            buffer: WriteBuffer::create(PathBuf::from("./wb")).await,
            builders: VecDeque::new(),
            first: None,
        }));
        let t = LSMTreeReader {
            internal: RLock::new(lock.clone()),
        };
        let merger = LSMTreeWriter { tree: lock };
        spawn(merger.run());
        t
    }

    pub async fn read(&self, key: &Key) -> Option<T> {
        let internal = self.internal.read().await;
        if let Some(x) = internal.buffer.read(key) {
            return x.into_data();
        }
        for x in internal.builders.iter() {
            if let Some(x) = x.read(key) {
                return x.data().cloned();
            }
        }
        let mut current = internal.first.as_ref()?.blocking_read().clone();
        drop(internal);
        loop {
            if let Some(x) = current.reader().await.unwrap().read(key).await {
                break x.into_data();
            }
            current = current.next().await?
        }
    }
}

struct LSMTreeWriter<T: Serialize + DeserializeOwned> {
    tree: Arc<RwLock<LSMTree<T>>>,
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

    async fn run(mut self) {
        loop {
            self = q!(self.check_deletion());
            self.merge().await;
            self.new_buffer().await;
        }
    }

    pub async fn new_buffer(&mut self) {
        let new_wb = WriteBuffer::create(PathBuf::from("./new")).await;
        let mut internal = self.tree.write().await;
        let old_buffer = replace(&mut internal.buffer, new_wb);
        internal.builders.push_front(old_buffer.to_builder().await);
    }

    async fn merge(&mut self) {
        let tree = &self.tree;

        let internal = tree.read().await;
        let builder = q!(internal.builders.back()).clone();
        drop(internal);

        let table = builder.build().await;

        let mut internal = tree.write().await;
        internal.first = Some(RwLock::new(Arc::new(SSTableNode::new(
            table,
            replace(&mut internal.first, None),
        ))));
        internal.builders.pop_back();
        drop(internal);

        let internal = tree.read().await;
        let mut current = q!(merge_into_node(q!(internal.first.as_ref())).await);

        loop {
            let lock = q!(current.next_lock().await.as_ref());
            current = q!(merge_into_node(lock).await);
        }
    }
}
