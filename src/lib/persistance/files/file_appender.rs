use anyhow::Result;
use rocket::tokio::{fs::File, io::AsyncWriteExt};

use super::RWFile;

pub struct FileAppender<'a> {
    #[allow(dead_code)]
    owner: &'a mut RWFile,
    file: File,
}

impl<'a> FileAppender<'a> {
    pub fn new(file: File, owner: &'a mut RWFile) -> Self {
        FileAppender { owner, file }
    }

    pub async fn append(&mut self, buffer: &mut [u8]) -> Result<()> {
        self.file.write_all(buffer).await?;
        Ok(())
    }
}
