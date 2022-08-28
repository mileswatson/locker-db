use std::{io::SeekFrom, path::PathBuf};

use anyhow::{Error, Result};
use rocket::tokio::{
    fs::{remove_file, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

pub struct ImmutableFile {
    pub(super) path: PathBuf,
    pub(super) size: u64,
}

impl ImmutableFile {
    pub async fn create(path: PathBuf, data: &[u8]) -> Result<ImmutableFile> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        file.write_all(data).await?;
        file.sync_all().await?;
        let size = file.metadata().await?.len();
        Ok(ImmutableFile { path, size })
    }

    pub async fn from_existing(path: PathBuf) -> Result<ImmutableFile> {
        let file = OpenOptions::new().read(true).open(&path).await?;
        let size = file.metadata().await?.len();
        Ok(ImmutableFile { path, size })
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

    pub fn size(&self) -> u64 {
        self.owner.size
    }

    pub async fn read_fixed<const N: usize>(&mut self, offset: u64) -> Result<[u8; N]> {
        let mut buffer = [0; N];
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    pub async fn read(&mut self, offset: u64, size: u64) -> Result<Vec<u8>> {
        let mut buffer = vec![0; size.try_into().unwrap()];
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    pub async fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut buf = vec![0; self.size() as usize];
        self.file.seek(SeekFrom::Start(0)).await?;
        self.file.read_exact(&mut buf).await?;
        Ok(buf)
    }
}
