use std::io::SeekFrom;

use anyhow::Result;
use rocket::tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use super::RWFile;

pub struct FileReader<'a> {
    #[allow(dead_code)]
    owner: &'a RWFile,
    file: File,
}

impl<'a> FileReader<'a> {
    pub fn new(file: File, owner: &'a RWFile) -> Self {
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
