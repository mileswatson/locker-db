use std::{path::PathBuf, sync::Arc};

use rocket::{
    serde::{DeserializeOwned, Serialize},
    tokio::{fs::metadata, spawn, sync::RwLock},
};

use crate::core::{key::Key, rlock::RLock};

use super::{lsm_tree::LSMTree, writer::LSMTreeWriter};

pub struct LSMTreeReader<T: Serialize + DeserializeOwned> {
    internal: RLock<LSMTree<T>>,
}

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> LSMTreeReader<T> {
    pub async fn new(dir: PathBuf) -> LSMTreeReader<T> {
        let tree = if metadata(&dir).await.is_ok() {
            LSMTree::load(dir).await
        } else {
            LSMTree::new(dir).await
        };
        let tree = Arc::new(RwLock::new(tree));
        let t = LSMTreeReader {
            internal: RLock::new(tree.clone()),
        };
        let merger = LSMTreeWriter::new(tree);
        spawn(merger.run());
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
            let current = lock.first.as_ref()?.blocking_read().clone();
            current
        };
        loop {
            if let Some(x) = current.reader().await.unwrap().read(key).await {
                break x.into_data();
            }
            current = current.next().await?
        }
    }
}
