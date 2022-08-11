use std::path::PathBuf;

use anyhow::{Error, Result};
use hex::ToHex;
use rand::Rng;
use rocket::tokio::{
    fs::{remove_file, OpenOptions},
    io::AsyncWriteExt,
};

use super::{file_appender::FileAppender, FileReader};

pub struct RWFile {
    pub(super) path: PathBuf,
}

fn random_filename() -> String {
    let mut bytes: [u8; 16] = [0; 16];
    rand::thread_rng().fill(&mut bytes);
    let mut filename = bytes.encode_hex::<String>();
    filename.push_str(".db");
    filename
}

impl RWFile {
    pub async fn new(dir: PathBuf, data: &[u8]) -> Result<RWFile> {
        let path = dir.join(random_filename());
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        file.write_all(data).await?;
        file.shutdown().await?;
        Ok(RWFile { path })
    }

    pub async fn from_existing(path: PathBuf) -> Result<RWFile> {
        OpenOptions::new()
            .append(true)
            .read(true)
            .open(&path)
            .await?;
        Ok(RWFile { path })
    }

    pub async fn open(self: &RWFile) -> Result<FileReader> {
        let file = OpenOptions::new().read(true).open(&self.path).await?;
        Ok(FileReader::new(file, self))
    }

    pub async fn open_append(self: &mut RWFile) -> Result<FileAppender> {
        let file = OpenOptions::new().append(true).open(&self.path).await?;
        Ok(FileAppender::new(file, self))
    }

    pub async fn delete(self) -> Result<()> {
        remove_file(self.path).await.map_err(Error::from)
    }
}
