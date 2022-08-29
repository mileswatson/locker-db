use std::marker::PhantomData;
use std::path::PathBuf;

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

impl<T: Serialize + DeserializeOwned + Clone> WriteBuffer<T> {
    pub async fn create(path: PathBuf) -> WriteBuffer<T> {
        let (wal, existing) = WAL::<Entry<T>>::open(path.with_extension("wal"))
            .await
            .unwrap();
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

    pub async fn close(self) -> PathBuf {
        self.file.into_inner().close().await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rocket::tokio::{self, fs::remove_file};

    use crate::{
        core::{
            entry::{Entry, EntryData},
            key::Key,
        },
        sstables::write_buffer::WriteBuffer,
    };

    #[tokio::test]
    async fn test() {
        let wb = WriteBuffer::create(PathBuf::from("./9847248")).await;
        let (k1, k2, k3) = (Key::new(), Key::new(), Key::new());
        let sequence = vec![
            Entry::new(k1, EntryData::Data("okay1".to_string())),
            Entry::new(k2, EntryData::Data("ok2".into())),
            Entry::new(k1, EntryData::Deleted),
            Entry::new(k3, EntryData::Data("okayyy3".into())),
        ];
        for x in sequence {
            wb.write(x).await
        }
        assert_eq!(wb.read(&k1), Some(EntryData::Deleted));
        assert_eq!(wb.read(&k2), Some(EntryData::Data("ok2".into())));
        assert_eq!(wb.read(&k3), Some(EntryData::Data("okayyy3".into())));
        remove_file(wb.close().await).await.unwrap();
    }
}
