use std::collections::VecDeque;
use std::mem::replace;
use std::path::PathBuf;
use std::sync::Arc;

use rocket::serde::{DeserializeOwned, Serialize};
use rocket::tokio::sync::RwLock;

use crate::core::key::Key;
use crate::sstables::sstable_builder::SSTableBuilder;
use crate::sstables::write_buffer::WriteBuffer;

use super::sstable_node::{NextSSTable, SSTableNode};

struct LSMTreeInternal<T: Serialize + DeserializeOwned> {
    buffer: WriteBuffer<T>,
    builders: VecDeque<SSTableBuilder<T>>,
    first: NextSSTable<T>,
}

pub struct LSMTree<T: Serialize + DeserializeOwned> {
    internal: RwLock<LSMTreeInternal<T>>,
}

impl<T: Serialize + DeserializeOwned + Clone> LSMTree<T> {
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

    pub async fn new_buffer(&mut self) {
        let new_wb = WriteBuffer::create(PathBuf::from("./new")).await;
        let mut internal = self.internal.blocking_write();
        let old_buffer = replace(&mut internal.buffer, new_wb);
        internal.builders.push_front(old_buffer.to_builder().await);
    }

    pub async fn merge(&mut self) {
        let internal = self.internal.read().await;
        let builder = match internal.builders.back() {
            Some(x) => x.clone(),
            None => return,
        };
        drop(internal);
        let table = builder.build().await;
        let mut internal = self.internal.write().await;

        internal.first = Some(RwLock::new(Arc::new(SSTableNode::new(
            table,
            replace(&mut internal.first, None),
        ))));

        let mut first = match &internal.first {
            None => return,
            Some(x) => x.write().await,
        };

        let next = first.next().await;

        let second = match next.as_ref() {
            None => return,
            Some(x) => x,
        };

        if second.len() < 2 * first.len() {
            let merged = SSTableBuilder::merge(&first, second, PathBuf::from("./alsonew")).await;
            *first = Arc::new(SSTableNode::new(
                merged,
                second.next().await.map(RwLock::new),
            ));
        }
    }
}
