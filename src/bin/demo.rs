use std::path::Path;

use locker_db::encoding::key::Key;
use locker_db::persistance::wal::{WAL, Entry};
use rocket::tokio;

#[tokio::main]
async fn main() {
    let (mut wal, _) = WAL::open(Path::new("./")).await.unwrap();
    wal.write(&Entry {
        key: Key::new(),
        data: "Hi!".bytes().collect(),
    })
    .await
    .unwrap();
    wal.write(&Entry {
        key: Key::new(),
        data: "Hello there!".bytes().collect(),
    })
    .await
    .unwrap();
    wal.write(&Entry {
        key: Key::new(),
        data: "Sup bro".bytes().collect(),
    })
    .await
    .unwrap();
    wal.close().await.unwrap();
    let (_, remaining) = WAL::open(Path::new("./")).await.unwrap();
    dbg!(remaining);
}
