use std::collections::VecDeque;

use rocket::serde::{DeserializeOwned, Serialize};
use rocket::tokio::sync::RwLock;

use crate::core::key::Key;
use crate::sstables::sstable_builder::SSTableBuilder;
use crate::sstables::write_buffer::WriteBuffer;

use super::sstable_node::NextSSTable;

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
        let internal = self.internal.blocking_read();
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
            if let Some(x) = current.table().reader().await.unwrap().read(key).await {
                break x.into_data()
            }
            current = current.next()?
        }
    }
}
