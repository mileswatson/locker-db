use rocket::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
pub struct State {
    wal: String,
    builders: Vec<String>,
    tables: Vec<String>,
}

impl State {
    pub fn new(wal: String, builders: Vec<String>, tables: Vec<String>) -> State {
        State {
            wal,
            builders,
            tables,
        }
    }
}
