use std::{marker::PhantomData, path::PathBuf};

use dashmap::ReadOnlyView;
use rocket::{
    serde::{DeserializeOwned, Serialize},
    tokio::fs::remove_file,
};

use crate::{
    core::{entry::EntryData, key::Key},
    persistance::files::AppendableFile,
};

use super::sstable::SSTable;

pub struct SSTableBuilder<T: Serialize> {
    entries: ReadOnlyView<Key, EntryData<T>>,
    path: PathBuf,
    entry_type: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> SSTableBuilder<T> {
    pub fn new(entries: ReadOnlyView<Key, EntryData<T>>, path: PathBuf) -> SSTableBuilder<T> {
        SSTableBuilder {
            entries,
            path,
            entry_type: PhantomData::default(),
        }
    }

    pub fn read(&self, key: &Key) -> Option<&EntryData<T>> {
        self.entries.get(key)
    }

    pub async fn build(&self) -> SSTable<T> {
        let mut entries: Vec<_> = self.entries.iter().collect();
        entries.sort_by_key(|x| x.0);

        let mut offsets = AppendableFile::new(self.path.with_extension("offsets"))
            .await
            .unwrap();
        let mut strings = AppendableFile::new(self.path.with_extension("strings"))
            .await
            .unwrap();

        let mut offset = 0u64;
        for (k, v) in entries {
            let string_bytes = bincode::serialize(v).unwrap();
            let offset_bytes = [
                k.bytes(),
                &offset.to_be_bytes()[..],
                &(string_bytes.len() as u64).to_be_bytes()[..],
            ]
            .concat();
            strings.append(&string_bytes).await.unwrap();
            offsets.append(&offset_bytes).await.unwrap();
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
