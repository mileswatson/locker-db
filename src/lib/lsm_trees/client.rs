use std::{fmt::Debug, path::PathBuf, sync::Arc};

use log::{debug, info};
use rocket::{
    serde::{DeserializeOwned, Serialize},
    tokio::{fs::metadata, spawn},
};

use crate::core::{
    entry::{Entry, EntryData},
    key::Key,
};

use super::{lsm_tree::LSMTree, service::LSMTreeService};

pub struct LSMTreeClient<T: Serialize + DeserializeOwned> {
    tree: Arc<LSMTree<T>>,
}

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static> LSMTreeClient<T> {
    pub async fn new(dir: PathBuf) -> LSMTreeClient<T> {
        let tree = if metadata(&dir).await.is_ok() {
            LSMTree::load(dir).await
        } else {
            LSMTree::new(dir).await
        };
        let tree = Arc::new(tree);
        let t = LSMTreeClient { tree: tree.clone() };
        let service = LSMTreeService::new(tree);
        spawn(service.run());
        t
    }

    pub async fn write(&self, key: Key, data: Option<T>) {
        info!("Setting {} to {:?}", key.hex(), data);
        self.tree
            .buffers
            .read()
            .await
            .buffer
            .write(Entry::new(
                key,
                data.map(EntryData::Data).unwrap_or(EntryData::Deleted),
            ))
            .await;
    }

    pub async fn read(&self, key: &Key) -> Option<T> {
        {
            let lock = self.tree.buffers.read().await;
            if let Some(x) = lock.buffer.read(key) {
                debug!("Found {} in main buffer {}.", key.hex(), lock.buffer.id());
                return x.into_data();
            }
            for b in lock.builders.iter() {
                if let Some(x) = b.read(key) {
                    debug!("Found {} in builder {}.", key.hex(), b.id());
                    return x.data().cloned();
                }
            }
        }
        let mut current = self.tree.first.load_full();
        loop {
            let c = match current.as_ref().as_ref() {
                Some(c) => c,
                None => {
                    debug!("Did not find {}.", key.hex());
                    return None;
                }
            };
            if let Some(x) = c.reader().await.unwrap().read(key).await {
                let data = x.into_data();
                debug!("Found {}={:?} in table {}.", key.hex(), &data, c.id());
                break data;
            }
            current = c.next()
        }
    }
}
