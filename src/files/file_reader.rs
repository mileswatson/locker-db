use std::{io::SeekFrom, marker::PhantomData};

use anyhow::Result;
use rocket::tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use super::ImmutableFile;

pub struct FileReader<'a> {
    lifetime: PhantomData<&'a ImmutableFile>,
    file: File,
}

impl<'a> FileReader<'a> {
    pub fn new(file: File, lifetime: PhantomData<&'a ImmutableFile>) -> Self {
        FileReader { lifetime, file }
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
