use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::{collections::VecDeque, sync::Arc};

use rocket::serde::{DeserializeOwned, Serialize};
use rocket::tokio::fs::{read_dir, remove_dir, remove_file};
use rocket::tokio::sync::RwLock;

use crate::sstables::sstable::SSTable;
use crate::sstables::sstable_builder::SSTableBuilder;
use crate::sstables::write_buffer::WriteBuffer;

use super::sstable_node::{NextSSTable, SSTableNode};
use super::state::State;

#[derive(Debug)]
pub struct LSMTree<T: Serialize + DeserializeOwned> {
    pub(super) dir: PathBuf,
    pub(super) buffer: WriteBuffer<T>,
    pub(super) builders: VecDeque<SSTableBuilder<T>>,
    pub(super) first: NextSSTable<T>,
    pub(super) nodes: HashMap<String, Arc<SSTableNode<T>>>,
}

async fn drain_dir(dir: &Path, allowed: &[String]) {
    let mut files = read_dir(dir).await.unwrap();
    while let Some(x) = files.next_entry().await.unwrap() {
        let name = PathBuf::from(x.file_name()).with_extension("");
        let name = name.file_name().unwrap();

        if !allowed.contains(&name.to_str().unwrap().to_string()) {
            if x.file_type().await.unwrap().is_dir() {
                remove_dir(x.path()).await.unwrap()
            } else {
                remove_file(x.path()).await.unwrap()
            }
        }
    }
}

impl<T: Serialize + DeserializeOwned + Clone> LSMTree<T> {
    pub(super) async fn load(dir: PathBuf) -> LSMTree<T> {
        let s = State::load(&dir).await;

        drain_dir(&dir.join("tables"), &s.tables).await;
        let mut wals = s.builders.clone();
        wals.push(s.wal.clone());
        drain_dir(&dir.join("wals"), &wals).await;
        drain_dir(
            &dir,
            &["state".to_string(), "trees".to_string(), "wals".to_string()],
        )
        .await;

        let mut builders = VecDeque::new();
        for id in s.builders {
            builders.push_back(WriteBuffer::from(dir.clone(), id).await.to_builder().await)
        }
        let mut first = None;
        let mut nodes = HashMap::new();
        for id in s.tables.into_iter().rev() {
            let table = Arc::new(SSTableNode::new(
                SSTable::new(&dir.join("tables"), id.clone()).await,
                first,
            ));
            nodes.insert(id, table.clone());
            first = Some(RwLock::new(table));
        }

        LSMTree {
            buffer: WriteBuffer::create(dir.join("wals").join(s.wal)).await,
            builders,
            first,
            nodes,
            dir,
        }
    }

    pub(super) async fn state(&self) -> State {
        let mut nodes = Vec::new();
        let mut current = match &self.first {
            Some(x) => Some(x.read().await.clone()),
            None => None,
        };
        loop {
            current = match current {
                None => break,
                Some(x) => {
                    nodes.push(x.table().id().to_string());
                    x.next().await
                }
            };
        }

        State::new(
            self.buffer.id().to_string(),
            self.builders.iter().map(|x| x.id().to_string()).collect(),
            nodes,
        )
    }
}
