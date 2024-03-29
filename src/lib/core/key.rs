use hex::ToHex;
use rand::Rng;
use rocket::serde::{Deserialize, Serialize};

pub const KEY_SIZE: usize = 16;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[serde(crate = "rocket::serde")]
pub enum Key {
    Key([u8; KEY_SIZE]),
}

impl Key {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Key {
        let mut bytes: [u8; 16] = [0; 16];
        rand::thread_rng().fill(&mut bytes);
        Key::Key(bytes)
    }

    pub fn bytes(&self) -> &[u8; KEY_SIZE] {
        match self {
            Key::Key(b) => b,
        }
    }

    pub fn hex(&self) -> String {
        self.bytes().encode_hex()
    }
}

impl AsRef<[u8; KEY_SIZE]> for Key {
    fn as_ref(&self) -> &[u8; KEY_SIZE] {
        match self {
            Key::Key(x) => x,
        }
    }
}
