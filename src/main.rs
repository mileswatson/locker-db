use std::{collections::HashMap, sync::RwLock};

use rocket::{http::Status, State};

#[macro_use]
extern crate rocket;

#[get("/<key>")]
async fn get(key: &str, map: &State<RwLock<HashMap<String, String>>>) -> Result<String, Status> {
    map.read()
        .map_err(|_| Status::InternalServerError)
        .and_then(|m| m.get(key).cloned().ok_or(Status::NotFound))
}

#[post("/<key>", data = "<value>")]
fn set(key: String, value: String, map: &State<RwLock<HashMap<String, String>>>) -> Status {
    map.write()
        .map(|mut m| {
            m.insert(key, value);
            Status::Ok
        })
        .unwrap_or(Status::InternalServerError)
}

#[launch]
fn rocket() -> _ {
    let map = RwLock::new(HashMap::<String, String>::new());
    rocket::build().manage(map).mount("/", routes![get, set])
}
