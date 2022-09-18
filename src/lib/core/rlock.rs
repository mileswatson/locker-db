use parking_lot::{RwLock, RwLockReadGuard};
use std::sync::Arc;

pub struct RLock<T> {
    lock: Arc<RwLock<T>>,
}

impl<T> RLock<T> {
    pub fn new(lock: Arc<RwLock<T>>) -> RLock<T> {
        RLock { lock }
    }

    pub async fn read(&self) -> RwLockReadGuard<T> {
        self.lock.read()
    }
}
