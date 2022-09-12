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
    dir: PathBuf,
    buffer: WriteBuffer<T>,
    builders: VecDeque<SSTableBuilder<T>>,
    first: NextSSTable<T>,
}

pub struct LSMTreeReader<T: Serialize + DeserializeOwned> {
    internal: RLock<LSMTree<T>>,
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

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> LSMTreeReader<T> {
    pub async fn new(dir: PathBuf) -> LSMTreeReader<T> {
        let lock = Arc::new(RwLock::new(LSMTree {
            buffer: WriteBuffer::create(dir.join("wals").join(Key::new().hex())).await,
            dir,
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
        let mut current = {
            let lock = self.internal.read().await;
            if let Some(x) = lock.buffer.read(key) {
                return x.into_data();
            }
            for x in lock.builders.iter() {
                if let Some(x) = x.read(key) {
                    return x.data().cloned();
                }
            }
            let current = lock.first.as_ref()?.blocking_read().clone();
            current
        };
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

        let builder = {
            let lock = tree.read().await;
            q!(lock.builders.back()).clone()
        };

        let table = builder.build().await;

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
}
