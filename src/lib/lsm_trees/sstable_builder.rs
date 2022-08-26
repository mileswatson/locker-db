use std::marker::PhantomData;
use std::path::Path;

use anyhow::Result;
use dashmap::DashMap;
use rocket::serde::{DeserializeOwned, Serialize};
use rocket::tokio::sync::Mutex;

use crate::encoding::entry::EntryData;
use crate::encoding::{entry::Entry, key::Key};
use crate::persistance::wal::WAL;

pub struct SSTableBuilder<T: Serialize + DeserializeOwned> {
    entries: DashMap<Key, EntryData<T>>,
    file: Mutex<WAL<Entry<T>>>,
    entry_type: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Clone> SSTableBuilder<T> {
    pub async fn write(&self, entry: Entry<T>) -> Result<()> {
        let mut file = self.file.lock().await;
        file.write(&entry).await?;
        self.entries.insert(entry.key, entry.data);
        drop(file);
        Ok(())
    }

    pub fn read(&self, key: &Key) -> Option<EntryData<T>> {
        self.entries.get(key).map(|x| x.clone())
    }

    pub async fn from(path: &Path) -> SSTableBuilder<T> {
        let (wal, existing) = WAL::<Entry<T>>::open(path).await.unwrap();
        SSTableBuilder {
            entries: existing.into_iter().map(|x| (x.key, x.data)).collect(),
            file: Mutex::new(wal),
            entry_type: PhantomData::default()
        }
    }

    pub async fn close(self) -> Result<()> {
        self.file.into_inner().close().await
    }
}
