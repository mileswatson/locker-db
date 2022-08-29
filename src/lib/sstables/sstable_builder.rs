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

use super::sstable::{OffsetEntry, SSTable};

pub struct SSTableBuilder<T: Serialize> {
    entries: ReadOnlyView<Key, EntryData<T>>,
    path: PathBuf,
    entry_type: PhantomData<T>,
}

fn get_offset_bytes(entry: &OffsetEntry) -> Vec<u8> {
    let offset_bytes = [
        entry.key.bytes(),
        &entry.offset.to_be_bytes()[..],
        &entry.length.to_be_bytes()[..],
    ]
    .concat();
    offset_bytes
}

async fn copy_entry<T: Serialize>(
    offsets: &mut AppendableFile,
    strings: &mut AppendableFile,
    offset: &mut u64,
    key: Key,
    data: &EntryData<T>,
) {
    let string_bytes = bincode::serialize(&data).unwrap();
    let offset_bytes = get_offset_bytes(&OffsetEntry {
        key,
        offset: *offset,
        length: string_bytes.len() as u64,
    });
    strings.append(&string_bytes).await.unwrap();
    offsets.append(&offset_bytes).await.unwrap();
    *offset += string_bytes.len() as u64;
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
        for (&key, v) in entries {
            let string_bytes = bincode::serialize(v).unwrap();
            let offset_bytes = get_offset_bytes(&OffsetEntry {
                key,
                offset,
                length: string_bytes.len() as u64,
            });
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

    pub async fn merge(young: &SSTable<T>, old: &SSTable<T>, path: PathBuf) -> SSTable<T> {
        let mut offsets = AppendableFile::new(path.with_extension("offsets"))
            .await
            .unwrap();
        let mut strings = AppendableFile::new(path.with_extension("strings"))
            .await
            .unwrap();

        let mut young = young.reader().await.unwrap();
        let mut old = old.reader().await.unwrap();

        let mut offset = 0u64;
        let mut indexes = (0, 0);

        let mut entries = (
            young.read_index(indexes.0).await,
            old.read_index(indexes.1).await,
        );

        loop {
            match entries {
                (None, None) => break,
                (Some((key, ref data)), None) => {
                    copy_entry(&mut offsets, &mut strings, &mut offset, key, data).await;
                    indexes.0 += 1;
                    entries.0 = young.read_index(indexes.0).await;
                }
                (None, Some((key, ref data))) => {
                    copy_entry(&mut offsets, &mut strings, &mut offset, key, data).await;
                    indexes.1 += 1;
                    entries.1 = old.read_index(indexes.1).await;
                }
                (Some((k0, ref d0)), Some((k1, ref d1))) => {
                    if k0 < k1 {
                        copy_entry(&mut offsets, &mut strings, &mut offset, k0, d0).await;
                        indexes.0 += 1;
                        entries.0 = young.read_index(indexes.0).await;
                    } else {
                        copy_entry(&mut offsets, &mut strings, &mut offset, k1, d1).await;
                        indexes.1 += 1;
                        entries.1 = old.read_index(indexes.1).await;
                    }
                },
            }
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
