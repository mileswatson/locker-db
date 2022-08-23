use std::path::Path;

use locker_db::encoding::key::Key;
use locker_db::persistance::wal::{WAL, Entry};
use rocket::tokio;

#[tokio::main]
async fn main() {
    let (mut wal, _) = WAL::<String>::open(Path::new("./")).await.unwrap();
    wal.write(&Entry {
        key: Key::new(),
        data: "Hi!".to_string(),
    })
    .await
    .unwrap();
    wal.write(&Entry {
        key: Key::new(),
        data: "Hello there!".to_string(),
    })
    .await
    .unwrap();
    wal.write(&Entry {
        key: Key::new(),
        data: "Sup bro".to_string(),
    })
    .await
    .unwrap();
    wal.close().await.unwrap();
    let (_, remaining) = WAL::<String>::open(Path::new("./")).await.unwrap();
    dbg!(remaining);
}
