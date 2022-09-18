use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

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

#[derive(Clone, Debug)]
pub struct SSTableBuilder<T: Serialize> {
    entries: Arc<ReadOnlyView<Key, EntryData<T>>>,
    dir: PathBuf,
    id: String,
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
    pub fn new(
        entries: ReadOnlyView<Key, EntryData<T>>,
        dir: PathBuf,
        id: String,
    ) -> SSTableBuilder<T> {
        SSTableBuilder {
            entries: Arc::new(entries),
            dir,
            id,
            entry_type: PhantomData::default(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    fn path(&self) -> PathBuf {
        self.dir.join(&self.id).with_extension("wal")
    }

    pub fn read(&self, key: &Key) -> Option<&EntryData<T>> {
        self.entries.get(key)
    }

    pub async fn build(&self, dir: &Path) -> SSTable<T> {
        let mut entries: Vec<_> = self.entries.iter().collect();
        entries.sort_by_key(|x| x.0);

        let mut offsets = AppendableFile::new(dir.join(&self.id).with_extension("offsets"))
            .await
            .unwrap();
        let mut strings = AppendableFile::new(dir.join(&self.id).with_extension("strings"))
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
        SSTable::new(dir, self.id.to_string()).await
    }

    pub async fn merge(young: &SSTable<T>, old: &SSTable<T>, dir: PathBuf) -> SSTable<T> {
        let id = Key::new().hex();

        let mut offsets = AppendableFile::new(dir.join(&id).with_extension("offsets"))
            .await
            .unwrap();
        let mut strings = AppendableFile::new(dir.join(&id).with_extension("strings"))
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
                    if k0 <= k1 {
                        copy_entry(&mut offsets, &mut strings, &mut offset, k0, d0).await;
                        indexes.0 += 1;
                        entries.0 = young.read_index(indexes.0).await;
                        if k0 == k1 {
                            indexes.1 += 1;
                            entries.1 = old.read_index(indexes.1).await;
                        }
                    } else {
                        copy_entry(&mut offsets, &mut strings, &mut offset, k1, d1).await;
                        indexes.1 += 1;
                        entries.1 = old.read_index(indexes.1).await;
                    }
                }
            }
        }

        SSTable::new(&dir, id).await
    }

    pub async fn delete(self) {
        remove_file(&self.path()).await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rocket::tokio;

    use crate::{
        core::{
            entry::{Entry, EntryData},
            key::Key,
        },
        sstables::write_buffer::WriteBuffer,
    };

    #[tokio::test]
    async fn test() {
        let wb = WriteBuffer::create(PathBuf::from("./")).await;
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
        let (b, f) = wb.to_builder();
        f.close().await.unwrap();
        assert_eq!(b.read(&k1), Some(&EntryData::Deleted));
        assert_eq!(b.read(&k2), Some(&EntryData::Data("ok2".into())));
        assert_eq!(b.read(&k3), Some(&EntryData::Data("okayyy3".into())));
        b.delete().await;
    }
}
