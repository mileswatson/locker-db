use rocket::serde::{Deserialize, Serialize};

use super::key::Key;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(crate = "rocket::serde")]
pub enum EntryData<T> {
    Data(T),
    Deleted,
}

impl<T> EntryData<T> {
    pub fn into_data(self) -> Option<T> {
        match self {
            EntryData::Data(x) => Some(x),
            EntryData::Deleted => None,
        }
    }

    pub fn data(&self) -> Option<&T> {
        match self {
            EntryData::Data(x) => Some(x),
            EntryData::Deleted => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(crate = "rocket::serde")]
pub struct Entry<T> {
    pub key: Key,
    pub data: EntryData<T>,
}

impl<T> Entry<T> {
    pub fn new(key: Key, data: EntryData<T>) -> Entry<T> {
        Entry { key, data }
    }
}
