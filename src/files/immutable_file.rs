use std::marker::PhantomData;

use anyhow::{Error, Result};
use rocket::tokio::{
    fs::{remove_file, OpenOptions},
    io::AsyncWriteExt,
};

use super::FileReader;

pub struct ImmutableFile {
    path: String,
}

impl ImmutableFile {
    pub async fn new(path: String, data: &[u8]) -> Result<ImmutableFile> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        file.write_all(data).await?;
        file.shutdown().await?;
        Ok(ImmutableFile { path })
    }

    pub async fn from_existing(path: String) -> Result<ImmutableFile> {
        OpenOptions::new().read(true).open(&path).await?;
        Ok(ImmutableFile { path })
    }

    pub async fn open(self: &ImmutableFile) -> Result<FileReader> {
        let file = OpenOptions::new().read(true).open(&self.path).await?;
        Ok(FileReader::new(file, PhantomData::default()))
    }

    pub async fn delete(self) -> Result<()> {
        remove_file(self.path).await.map_err(Error::from)
    }
}
