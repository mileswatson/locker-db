use std::{cmp::Ordering, marker::PhantomData, path::PathBuf};

use anyhow::{anyhow, Result};
use rocket::{
    futures::future::join,
    serde::{Deserialize, DeserializeOwned, Serialize},
    tokio::join,
};

use crate::{
    core::{
        entry::EntryData,
        key::{Key, KEY_SIZE},
    },
    persistance::files::{FileReader, ImmutableFile},
};

const ENTRY_SIZE: usize = KEY_SIZE + 16;

#[derive(Serialize, Deserialize, Debug)]
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
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.offsets.size() / (ENTRY_SIZE as u64)
    }

    pub async fn read(&mut self, key: &Key) -> Option<EntryData<T>> {
        let mut lower = 0;
        let mut upper = self.offsets.size() / (ENTRY_SIZE as u64);
        let mut found = None;
        while lower < upper {
            let mid = (lower + upper) / 2;
            let offset = self.read_offset(mid).await.unwrap();
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
        Some(self.read_string(&offset).await.unwrap())
    }

    pub async fn read_index(&mut self, index: u64) -> Option<EntryData<T>> {
        if index < self.len() {
            let offset = self.read_offset(index).await.unwrap();
            Some(self.read_string(&offset).await.unwrap())
        } else {
            None
        }
    }

    async fn read_offset(&mut self, index: u64) -> Result<OffsetEntry> {
        let buf: [u8; ENTRY_SIZE] = self.offsets.read_fixed(index * ENTRY_SIZE as u64).await?;
        let key: [u8; KEY_SIZE] = buf[..KEY_SIZE].try_into().unwrap();
        let offset = u64::from_be_bytes(buf[KEY_SIZE..KEY_SIZE + 8].try_into().unwrap());
        let length = u64::from_be_bytes(buf[KEY_SIZE + 8..].try_into().unwrap());
        Ok(OffsetEntry {
            key: Key::Key(key),
            offset,
            length,
        })
    }

    async fn read_string(
        &mut self,
        OffsetEntry { offset, length, .. }: &OffsetEntry,
    ) -> Result<EntryData<T>> {
        let buf = self.strings.read(*offset, *length).await?;
        Ok(bincode::deserialize(&buf)?)
    }
}
