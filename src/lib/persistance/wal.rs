use std::{path::{Path}, mem::size_of};

use anyhow::{Ok, Result};

use crate::persistance::encoding::{Entry};

use super::{files::{AppendableFile, ImmutableFile}, encoding::{Key, KEY_SIZE}};

pub struct WAL {
    file: AppendableFile,
}

fn read_entry(mut remaining: &[u8]) -> Option<(Entry, &[u8])> {
    if remaining.len() < KEY_SIZE { return None; }
    let key_bytes: &[u8];
    (key_bytes, remaining) = remaining.split_at(KEY_SIZE);
    let key = Key::Key(key_bytes.try_into().unwrap());

    if remaining.len() < size_of::<u64>() {return None}
    let size_bytes: &[u8];
    (size_bytes, remaining) = remaining.split_at(size_of::<u64>());
    let size = u64::from_be_bytes(size_bytes.try_into().unwrap()) as usize;

    if remaining.len() < size {return None}
    let data: &[u8];
    (data, remaining) = remaining.split_at(size);

    Some((Entry {key, data: data.to_vec()}, remaining))
}

impl WAL {
    pub async fn write(&mut self, entry: &Entry) -> Result<()> {
        self.file.append(entry.key.as_ref()).await?;
        self.file.append(&entry.data.len().to_be_bytes()).await?;
        self.file.append(&entry.data).await?;
        Ok(())
    }

    pub async fn consume_existing(&mut self, dir: &Path) -> Result<Vec<Entry>> {
        let file = ImmutableFile::from_existing(dir.join("WAL")).await?;
        let mut reader = file.new_reader().await?;
        let bytes = reader.read_all().await?;
        let mut remaining = bytes.as_slice();

        let mut entries = Vec::new();
        while let Some((entry, r)) = read_entry(remaining) {
            entries.push(entry);
            remaining = r;
        }
        file.delete().await?;
        Ok(entries)
    }
}
