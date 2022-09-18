use std::{path::PathBuf, sync::Arc};

use parking_lot::RwLock;
use rocket::{
    serde::{DeserializeOwned, Serialize},
    tokio::{fs::metadata, spawn},
};

use crate::core::{key::Key, rlock::RLock};

use super::{lsm_tree::LSMTree, service::LSMTreeService};

pub struct LSMTreeClient<T: Serialize + DeserializeOwned> {
    internal: RLock<LSMTree<T>>,
}

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> LSMTreeClient<T> {
    pub async fn new(dir: PathBuf) -> LSMTreeClient<T> {
        let tree = if metadata(&dir).await.is_ok() {
            LSMTree::load(dir).await
        } else {
            LSMTree::new(dir).await
        };
        let tree = Arc::new(RwLock::new(tree));
        let t = LSMTreeClient {
            internal: RLock::new(tree.clone()),
        };
        let service = LSMTreeService::new(tree);
        spawn(service.run());
        t
    }

    pub async fn read(&self, key: &Key) -> Option<T> {
        let mut current = {
            let lock = self.internal.read().await;
            if let Some(x) = lock.buffer.read(key) {
                return x.into_data();
            }
            for x in lock.builders.iter() {
                if let Some(x) = x.read(key) {
                    return x.data().cloned();
                }
            }
            {
                let lock = lock.first.read();
                lock.as_ref()?.clone()
            }
        };
        loop {
            if let Some(x) = current.reader().await.unwrap().read(key).await {
                break x.into_data();
            }
            current = current.next()?
        }
    }
}
