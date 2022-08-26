use std::{cmp::Ordering, marker::PhantomData, path::PathBuf};

use anyhow::{anyhow, Result};
use rocket::{
    futures::future::join,
    serde::{Deserialize, DeserializeOwned, Serialize},
    tokio::join,
};

use crate::{
    encoding::{
        entry::EntryData,
        key::{Key, KEY_SIZE},
    },
    persistance::files::{FileReader, ImmutableFile},
};

const ENTRY_SIZE: usize = KEY_SIZE + 16;

#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
pub struct OffsetEntry {
    pub key: Key,
    pub offset: u64,
    pub length: u64,
}

pub struct SSTable<T> {
    offsets: ImmutableFile,
    strings: ImmutableFile,
    entry_type: PhantomData<T>,
}

impl<T> SSTable<T> {
    pub async fn new(offsets: PathBuf, strings: PathBuf) -> SSTable<T> {
        SSTable {
            offsets: ImmutableFile::from_existing(offsets).await.unwrap(),
            strings: ImmutableFile::from_existing(strings).await.unwrap(),
            entry_type: PhantomData::default(),
        }
    }

    pub async fn reader(&self) -> Result<SSTableReader<'_, T>> {
        let (offsets, strings) = join!(self.offsets.new_reader(), self.strings.new_reader());
        Ok(SSTableReader {
            offsets: offsets?,
            strings: strings?,
            entry_type: self.entry_type,
        })
    }

    pub async fn delete(self) -> Result<()> {
        let deletions = join(self.offsets.delete(), self.strings.delete()).await;
        match deletions {
            (Ok(_), Ok(_)) => Ok(()),
            _ => Err(anyhow!("Failed to delete SSTable!")),
        }
    }
}

pub struct SSTableReader<'a, T> {
    offsets: FileReader<'a>,
    strings: FileReader<'a>,
    entry_type: PhantomData<T>,
}

impl<'a, T: DeserializeOwned> SSTableReader<'a, T> {
    pub async fn read(&mut self, key: Key) -> Option<EntryData<T>> {
        let size: u64 = self.offsets.size() / (ENTRY_SIZE as u64);
        let mut lower = 0;
        let mut upper = size / (ENTRY_SIZE as u64);
        let mut found = None;
        while lower < upper {
            let mid = (lower + upper) / 2;
            let offset = self.read_offset(mid).await;
            match key.cmp(&offset.key) {
                Ordering::Equal => {
                    found = Some(offset);
                    break;
                }
                Ordering::Less => {
                    upper = mid;
                }
                Ordering::Greater => {
                    lower = mid + 1;
                }
            }
        }
        let offset = found?;
        self.read_string(&offset).await.unwrap()
    }

    async fn read_offset(&mut self, index: u64) -> OffsetEntry {
        let buf: [u8; ENTRY_SIZE] = self
            .offsets
            .read_fixed(index * ENTRY_SIZE as u64)
            .await
            .unwrap();
        let key: [u8; KEY_SIZE] = buf[..KEY_SIZE].try_into().unwrap();
        let offset = u64::from_be_bytes(buf[KEY_SIZE..KEY_SIZE + 4].try_into().unwrap());
        let length = u64::from_be_bytes(buf[KEY_SIZE + 4..].try_into().unwrap());
        OffsetEntry {
            key: Key::Key(key),
            offset,
            length,
        }
    }

    async fn read_string(
        &mut self,
        OffsetEntry { offset, length, .. }: &OffsetEntry,
    ) -> Result<Option<EntryData<T>>> {
        let buf = self.strings.read(*offset, *length).await?;
        Ok(bincode::deserialize(&buf)?)
    }
}
