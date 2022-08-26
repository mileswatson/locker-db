use std::path::{PathBuf};

use anyhow::Result;
use rocket::tokio::{
    fs::{File, OpenOptions, remove_file},
    io::AsyncWriteExt,
};

pub struct AppendableFile {
    file: File,
    path: PathBuf,
}

impl AppendableFile {
    pub async fn new(path: PathBuf) -> Result<AppendableFile> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .await?;
        Ok(AppendableFile { file, path })
    }

    pub async fn size(&mut self) -> Result<u64> {
        Ok(self.file.metadata().await.map(|m| m.len())?)
    }

    pub async fn append(&mut self, buffer: &[u8]) -> Result<()> {
        self.file.write_all(buffer).await?;
        self.file.sync_data().await?;
        Ok(())
    }

    pub async fn clear(&mut self) -> Result<()> {
        self.file.set_len(0).await?;
        self.file.sync_all().await?;
        Ok(())
    }

    pub async fn close(self) -> Result<PathBuf> {
        self.file.sync_all().await?;
        Ok(self.path)
    }

    pub async fn delete(self) -> Result<()> {
        remove_file(self.path).await?;
        Ok(())
    }
}
