use std::path::PathBuf;

use locker_db::lsm_trees::reader::LSMTreeReader;
use rocket::tokio;

#[tokio::main]
async fn main() {
    let x = LSMTreeReader::<String>::new(PathBuf::from("./testing")).await;
}
