use std::path::PathBuf;

use anyhow::Result;
use rocket::tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};

pub struct AppendableFile {
    file: File,
}

impl AppendableFile {
    pub async fn new(dir: PathBuf, data: &[u8]) -> Result<AppendableFile> {
        let path = dir.join("WAL");
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create_new(true)
            .open(&path)
            .await?;
        file.write_all(data).await?;
        file.sync_all().await?;
        Ok(AppendableFile { file })
    }

    pub async fn size(&mut self) -> Result<u64> {
        Ok(self.file.metadata().await.map(|m| m.len())?)
    }

    pub async fn append(&mut self, buffer: &[u8]) -> Result<()> {
        self.file.write_all(buffer).await?;
        self.file.flush().await?;
        Ok(())
    }
}
