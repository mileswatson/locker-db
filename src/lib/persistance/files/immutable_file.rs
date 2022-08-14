use std::{io::SeekFrom, path::PathBuf};

use anyhow::{Error, Result};
use rocket::tokio::{
    fs::{remove_file, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use super::file_id::FileID;

pub struct ImmutableFile {
    pub(super) path: PathBuf,
}

impl ImmutableFile {
    pub async fn new(dir: PathBuf, data: &[u8]) -> Result<ImmutableFile> {
        let path = FileID::new().filepath(dir);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        file.write_all(data).await?;
        file.shutdown().await?;
        Ok(ImmutableFile { path })
    }

    pub async fn from_existing(path: PathBuf) -> Result<ImmutableFile> {
        OpenOptions::new()
            .read(true)
            .open(&path)
            .await?;
        Ok(ImmutableFile { path })
    }

    pub async fn new_reader(self: &ImmutableFile) -> Result<FileReader> {
        let file = OpenOptions::new().read(true).open(&self.path).await?;
        Ok(FileReader::new(file, self))
    }

    pub async fn delete(self) -> Result<()> {
        remove_file(self.path).await.map_err(Error::from)
    }
}

pub struct FileReader<'a> {
    #[allow(dead_code)]
    owner: &'a ImmutableFile,
    file: File,
}

impl<'a> FileReader<'a> {
    pub fn new(file: File, owner: &'a ImmutableFile) -> Self {
        FileReader { owner, file }
    }

    pub async fn size(&mut self) -> Result<u64> {
        Ok(self.file.metadata().await.map(|m| m.len())?)
    }

    pub async fn read(&mut self, offset: u64, buffer: &mut [u8]) -> Result<()> {
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.read_exact(buffer).await?;
        Ok(())
    }

    pub async fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(self.size().await? as usize);
        self.file.seek(SeekFrom::Start(0)).await?;
        self.file.read_exact(&mut buf).await?;
        Ok(buf)
    }
}
