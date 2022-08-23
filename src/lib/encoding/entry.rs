use rocket::serde::{Serialize, Deserialize};

use super::key::Key;

#[derive(Serialize, Deserialize, Debug)]
#[serde(crate = "rocket::serde")]
pub struct Entry<T> {
    pub key: Key,
    pub data: T,
}
