use locker_db::{
    core::key::{Key, KEY_SIZE},
    lsm_trees::client::LSMTreeClient,
};
use log::LevelFilter;
use pretty_env_logger::env_logger::Target;
use rocket::{http::Status, State};

#[macro_use]
extern crate rocket;

#[get("/get/<key>")]
async fn get(key: &str, map: &State<LSMTreeClient<String>>) -> Result<String, Status> {
    let mut slice = [0u8; KEY_SIZE];
    hex::decode_to_slice(key, &mut slice).map_err(|_| Status::BadRequest)?;
    map.read(&Key::Key(slice)).await.ok_or(Status::NotFound)
}

#[post("/set/<key>", data = "<value>")]
async fn set(key: String, value: String, map: &State<LSMTreeClient<String>>) -> Status {
    let mut slice = [0u8; KEY_SIZE];
    if hex::decode_to_slice(key, &mut slice).is_err() {
        return Status::BadRequest;
    }
    map.write(Key::Key(slice), Some(value)).await;
    Status::Ok
}

#[post("/delete/<key>")]
async fn delete(key: String, map: &State<LSMTreeClient<String>>) -> Status {
    let mut slice = [0u8; KEY_SIZE];
    if hex::decode_to_slice(key, &mut slice).is_err() {
        return Status::BadRequest;
    }
    map.write(Key::Key(slice), None).await;
    Status::Ok
}

#[launch]
async fn rocket() -> _ {
    pretty_env_logger::formatted_builder()
        .target(Target::Stdout)
        .filter_level(LevelFilter::Debug)
        .init();
    let map = LSMTreeClient::<String>::new("./testing".into()).await;
    rocket::build()
        .manage(map)
        .mount("/", routes![get, set, delete])
}
