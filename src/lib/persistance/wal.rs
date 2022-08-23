use std::{marker::PhantomData, mem::size_of, path::Path};

use anyhow::{Ok, Result};
use rocket::serde::{Deserialize, DeserializeOwned, Serialize};

use super::files::{AppendableFile, ImmutableFile};
use crate::encoding::key::{Key, KEY_SIZE};

#[derive(Serialize, Deserialize, Debug)]
#[serde(crate = "rocket::serde")]
pub struct Entry<T> {
    pub key: Key,
    pub data: T,
}

pub struct WAL<T: Serialize + DeserializeOwned> {
    file: AppendableFile,
    log_type: PhantomData<T>,
}

fn read_entry<T: DeserializeOwned>(mut remaining: &[u8]) -> Option<(Entry<T>, &[u8])> {
    if remaining.len() < KEY_SIZE {
        dbg!(remaining.len());
        return None;
    }
    let key_bytes: &[u8];
    (key_bytes, remaining) = remaining.split_at(KEY_SIZE);
    let key = Key::Key(key_bytes.try_into().unwrap());

    if remaining.len() < size_of::<u64>() {
        dbg!(remaining.len());
        return None;
    }
    let size_bytes: &[u8];
    (size_bytes, remaining) = remaining.split_at(size_of::<u64>());
    let size = u64::from_be_bytes(size_bytes.try_into().unwrap()) as usize;

    if remaining.len() < size {
        dbg!(remaining.len(), size);
        return None;
    }
    let data: &[u8];
    (data, remaining) = remaining.split_at(size);

    Some((
        Entry {
            key,
            data: bincode::deserialize(data).ok()?,
        },
        remaining,
    ))
}

impl<T: Serialize + DeserializeOwned> WAL<T> {
    pub async fn write(&mut self, entry: &Entry<T>) -> Result<()> {
        let bytes = bincode::serialize(&entry.data)?;
        self.file.append(entry.key.as_ref()).await?;
        self.file.append(&bytes.len().to_be_bytes()).await?;
        self.file.append(&bytes).await?;
        Ok(())
    }

    pub async fn open(dir: &Path) -> Result<(WAL<T>, Vec<Entry<T>>)> {
        let mut entries = Vec::new();

        if let Result::Ok(file) = ImmutableFile::from_existing(dir.join("WAL")).await {
            let mut reader = file.new_reader().await?;
            let bytes = reader.read_all().await?;
            let mut remaining = bytes.as_slice();

            while let Some((entry, r)) = read_entry(remaining) {
                entries.push(entry);
                remaining = r;
            }
        };

        let wal = WAL {
            file: AppendableFile::new(dir.join("WAL")).await?,
            log_type: PhantomData::default()
        };

        Ok((wal, entries))
    }

    pub async fn clear(&mut self) -> Result<()> {
        self.file.clear().await
    }

    pub async fn close(self) -> Result<()> {
        self.file.close().await?;
        Ok(())
    }

    pub async fn delete(self) -> Result<()> {
        self.file.delete().await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use rocket::tokio;

    use crate::{
        encoding::key::Key,
        persistance::wal::{Entry, WAL},
    };

    #[tokio::test]
    pub async fn wal_test() {
        let (mut wal, remaining) = WAL::<String>::open(Path::new("./")).await.unwrap();
        assert_eq!(remaining.len(), 0);
        wal.write(&Entry {
            key: Key::new(),
            data: "Hi!".to_string(),
        })
        .await
        .unwrap();
        wal.write(&Entry {
            key: Key::new(),
            data: "Hello there!".to_string(),
        })
        .await
        .unwrap();
        wal.write(&Entry {
            key: Key::new(),
            data: "Sup bro".to_string(),
        })
        .await
        .unwrap();
        wal.close().await.unwrap();
        let (w, remaining) = WAL::<String>::open(Path::new("./")).await.unwrap();
        w.delete().await.unwrap();
        dbg!(remaining);
    }
}
