use rocket::serde::{Serialize, Deserialize};

use super::key::Key;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "rocket::serde")]
pub enum EntryData<T> {
    Data(T),
    Deleted,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(crate = "rocket::serde")]
pub struct Entry<T> {
    pub key: Key,
    pub data: EntryData<T>,
}
