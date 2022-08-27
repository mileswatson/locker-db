use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;

use anyhow::Result;
use dashmap::DashMap;
use rocket::serde::{DeserializeOwned, Serialize};
use rocket::tokio::sync::Mutex;

use crate::core::entry::EntryData;
use crate::core::{entry::Entry, key::Key};
use crate::persistance::wal::WAL;

use super::sstable_builder::SSTableBuilder;

pub struct WriteBuffer<T: Serialize + DeserializeOwned> {
    entries: DashMap<Key, EntryData<T>>,
    file: Mutex<WAL<Entry<T>>>,
    entry_type: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Clone + Debug + PartialEq> WriteBuffer<T> {
    pub async fn create(path: PathBuf) -> WriteBuffer<T> {
        let (wal, existing) = WAL::<Entry<T>>::open(path).await.unwrap();
        WriteBuffer {
            entries: existing.into_iter().map(|x| (x.key, x.data)).collect(),
            file: Mutex::new(wal),
            entry_type: PhantomData::default(),
        }
    }

    pub async fn to_builder(self) -> SSTableBuilder<T> {
        let x = self.file.into_inner();
        let path = x.close().await.unwrap();
        SSTableBuilder::new(self.entries.into_read_only(), path)
    }

    pub async fn write(&self, entry: Entry<T>) {
        let mut file = self.file.lock().await;
        file.write(&entry).await.unwrap();
        self.entries.insert(entry.key, entry.data);
        drop(file);
    }

    pub fn read(&self, key: &Key) -> Option<EntryData<T>> {
        self.entries.get(key).map(|x| x.clone())
    }

    pub async fn from(path: PathBuf) -> WriteBuffer<T> {
        let (wal, existing) = WAL::<Entry<T>>::open(path).await.unwrap();
        WriteBuffer {
            entries: existing.into_iter().map(|x| (x.key, x.data)).collect(),
            file: Mutex::new(wal),
            entry_type: PhantomData::default(),
        }
    }

    pub async fn close(self) -> Result<PathBuf> {
        self.file.into_inner().close().await
    }
}
