use std::{marker::PhantomData, mem::size_of, path::PathBuf};

use anyhow::{Ok, Result};
use rocket::serde::{DeserializeOwned, Serialize};

use super::files::{AppendableFile, ImmutableFile};

#[derive(Debug)]
pub struct WAL<T: Serialize + DeserializeOwned> {
    file: AppendableFile,
    log_type: PhantomData<T>,
}

fn read_entry<T: DeserializeOwned>(mut remaining: &[u8]) -> Option<(T, &[u8])> {
    if remaining.len() < size_of::<u64>() {
        return None;
    }
    let size_bytes: &[u8];
    (size_bytes, remaining) = remaining.split_at(size_of::<u64>());
    let size = u64::from_be_bytes(size_bytes.try_into().unwrap()) as usize;

    if remaining.len() < size {
        return None;
    }
    let data: &[u8];
    (data, remaining) = remaining.split_at(size);

    Some((bincode::deserialize(data).ok()?, remaining))
}

impl<T: Serialize + DeserializeOwned> WAL<T> {
    pub async fn write(&mut self, item: &T) -> Result<()> {
        let bytes = bincode::serialize(&item)?;
        self.file.append(&bytes.len().to_be_bytes()).await?;
        self.file.append(&bytes).await?;
        Ok(())
    }

    pub async fn open(path: PathBuf) -> Result<(WAL<T>, Vec<T>)> {
        let mut entries = Vec::new();

        if let Result::Ok(file) = ImmutableFile::from_existing(path.clone()).await {
            let mut reader = file.new_reader().await?;
            let bytes = reader.read_all().await?;
            let mut remaining = bytes.as_slice();

            while let Some((entry, r)) = read_entry(remaining) {
                entries.push(entry);
                remaining = r;
            }
        };

        let wal = WAL {
            file: AppendableFile::new(path).await?,
            log_type: PhantomData::default(),
        };

        Ok((wal, entries))
    }

    pub async fn clear(&mut self) -> Result<()> {
        self.file.clear().await
    }

    pub async fn close(self) -> Result<PathBuf> {
        self.file.close().await
    }

    pub async fn delete(self) -> Result<()> {
        self.file.delete().await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use rocket::tokio;

    use crate::persistance::wal::WAL;

    #[tokio::test]
    pub async fn wal_test() {
        let (mut wal, remaining) = WAL::<String>::open(PathBuf::from("./983724.wal"))
            .await
            .unwrap();
        assert_eq!(remaining.len(), 0);
        wal.write(&"Hi!".to_string()).await.unwrap();
        wal.write(&"Hello there!".to_string()).await.unwrap();
        wal.write(&"Sup bro".to_string()).await.unwrap();
        wal.close().await.unwrap();
        let (w, remaining) = WAL::<String>::open(PathBuf::from("./983724.wal"))
            .await
            .unwrap();
        w.delete().await.unwrap();
        assert_eq!(remaining, vec!["Hi!", "Hello there!", "Sup bro"]);
    }
}
