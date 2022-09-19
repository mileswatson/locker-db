use std::path::Path;

use rocket::{
    serde::{Deserialize, Serialize},
    tokio::fs::rename,
};

use crate::{core::key::Key, persistance::files::ImmutableFile};

#[derive(Serialize, Deserialize, Debug)]
#[serde(crate = "rocket::serde")]
pub struct State {
    pub wal: String,
    pub builders: Vec<String>,
    pub tables: Vec<String>,
}

impl State {
    pub fn new(wal: String, builders: Vec<String>, tables: Vec<String>) -> State {
        State {
            wal,
            builders,
            tables,
        }
    }

    pub async fn load(dir: &Path) -> State {
        let file = ImmutableFile::from_existing(dir.join("state"))
            .await
            .unwrap();
        let mut reader = file.new_reader().await.unwrap();
        let bytes = reader.read_all().await.unwrap();
        bincode::deserialize(&bytes).unwrap()
    }

    pub async fn save(&self, dir: &Path) {
        let temp_path = dir.join(Key::new().hex()).with_extension("state");
        let bytes = bincode::serialize(self).unwrap();
        ImmutableFile::create(temp_path.clone(), &bytes)
            .await
            .unwrap();
        rename(temp_path, dir.join("state")).await.unwrap();
    }
}
