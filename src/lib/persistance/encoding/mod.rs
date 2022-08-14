use rocket::serde::{Deserialize, Serialize};

pub const KEY_SIZE: usize = 16;

#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
pub enum Key {
    Key([u8; KEY_SIZE]),
}

impl AsRef<[u8; KEY_SIZE]> for Key {
    fn as_ref(&self) -> &[u8; KEY_SIZE] {
        match self {
            Key::Key(x) => x,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
pub struct KeyOffset {
    pub key: Key,
    pub offset: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
pub struct Entry {
    pub key: Key,
    pub data: Vec<u8>,
}
