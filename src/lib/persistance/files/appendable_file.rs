use std::path::PathBuf;

use anyhow::Result;
use rocket::tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};

use super::{random::random_filename};

pub struct AppendableFile {
    file: File,
}

impl AppendableFile {
    pub async fn new(dir: PathBuf, data: &[u8]) -> Result<AppendableFile> {
        let path = dir.join(random_filename());
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        file.write_all(data).await?;
        Ok(AppendableFile { file })
    }

    pub async fn append(&mut self, buffer: &mut [u8]) -> Result<()> {
        self.file.write_all(buffer).await?;
        self.file.flush().await?;
        Ok(())
    }
}

impl Drop for AppendableFile {
    fn drop(&mut self) {
        panic!("Appendable file should not be dropped!")
    }
}
