use std::path::PathBuf;

use locker_db::lsm_trees::client::LSMTreeClient;
use rocket::tokio;

#[tokio::main]
async fn main() {
    let x = LSMTreeClient::<String>::new(PathBuf::from("./testing")).await;
}
