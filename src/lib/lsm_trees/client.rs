use std::{path::PathBuf, sync::Arc};

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

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> LSMTreeClient<T> {
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
                return x.into_data();
            }
            for x in lock.builders.iter() {
                if let Some(x) = x.read(key) {
                    return x.data().cloned();
                }
            }
        }
        let mut current = self.tree.first.load_full();
        loop {
            let c = current.as_ref().as_ref()?;
            if let Some(x) = c.reader().await.unwrap().read(key).await {
                break x.into_data();
            }
            current = c.next()
        }
    }
}
