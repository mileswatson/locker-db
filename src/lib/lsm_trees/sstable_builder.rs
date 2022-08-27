use std::{marker::PhantomData, path::PathBuf};

use dashmap::ReadOnlyView;
use rocket::{
    serde::Serialize,
    tokio::{fs::remove_file},
};

use crate::{
    core::{entry::EntryData, key::Key},
    persistance::files::{AppendableFile},
};

use super::sstable::{OffsetEntry, SSTable};

pub struct SSTableBuilder<T: Serialize> {
    entries: ReadOnlyView<Key, EntryData<T>>,
    path: PathBuf,
    entry_type: PhantomData<T>,
}

impl<T: Serialize> SSTableBuilder<T> {
    pub fn read(&self, key: &Key) -> Option<&EntryData<T>> {
        self.entries.get(key)
    }

    pub async fn build(&self) -> SSTable<T> {
        let mut entries: Vec<_> = self.entries.iter().collect();
        entries.sort_by_key(|x| x.0);

        let mut offsets = AppendableFile::new(self.path.with_extension("offsets")).await.unwrap();
        let mut strings = AppendableFile::new(self.path.with_extension("strings")).await.unwrap();

        let mut offset = 0;
        for (k, v) in entries {
            let string_bytes = bincode::serialize(v).unwrap();
            let offset_entry = OffsetEntry {
                key: k.clone(),
                offset,
                length: string_bytes.len() as u64,
            };
            let offset_entry_bytes = bincode::serialize(&offset_entry).unwrap();
            strings.append(&string_bytes).await.unwrap();
            offsets.append(&offset_entry_bytes).await.unwrap();
            offset += string_bytes.len() as u64;
        }
        SSTable::new(
            offsets.close().await.unwrap(),
            strings.close().await.unwrap(),
        )
        .await
    }

    pub async fn delete(self) {
        remove_file(&self.path).await.unwrap();
    }
}
