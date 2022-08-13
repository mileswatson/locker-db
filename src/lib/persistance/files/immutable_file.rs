use std::{io::SeekFrom, path::PathBuf};

use anyhow::{Error, Result};
use rocket::tokio::{
    fs::{remove_file, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use super::random::random_filename;

pub struct ImmutableFile {
    pub(super) path: PathBuf,
}

impl ImmutableFile {
    pub async fn new(dir: PathBuf, data: &[u8]) -> Result<ImmutableFile> {
        let path = dir.join(random_filename());
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
            .append(true)
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
}
