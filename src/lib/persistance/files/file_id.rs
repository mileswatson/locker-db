use std::path::PathBuf;

use hex::ToHex;
use rand::Rng;

pub struct FileID {
    id: String,
}

impl FileID {
    pub fn new() -> FileID {
        let mut bytes: [u8; 16] = [0; 16];
        rand::thread_rng().fill(&mut bytes);
        FileID { id: bytes.encode_hex::<String>() }
    }

    pub fn filepath(&self, dir: PathBuf) -> PathBuf {
        dir.join(&self.id)
    }
}

impl Default for FileID {
    fn default() -> Self {
        Self::new()
    }
}