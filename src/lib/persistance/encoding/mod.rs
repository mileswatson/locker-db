use rocket::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
pub struct KeyOffset {
    id: [u8; 16],
    offset: u64,
}
