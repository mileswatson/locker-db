use std::collections::HashMap;
use std::path::PathBuf;
use std::{collections::VecDeque, sync::Arc};

use rocket::serde::{DeserializeOwned, Serialize};

use crate::sstables::sstable_builder::SSTableBuilder;
use crate::sstables::write_buffer::WriteBuffer;

use super::sstable_node::{NextSSTable, SSTableNode};
use super::state::State;

pub struct LSMTree<T: Serialize + DeserializeOwned> {
    pub(super) dir: PathBuf,
    pub(super) buffer: WriteBuffer<T>,
    pub(super) builders: VecDeque<SSTableBuilder<T>>,
    pub(super) first: NextSSTable<T>,
    pub(super) nodes: HashMap<String, Arc<SSTableNode<T>>>,
}

impl<T: Serialize + DeserializeOwned + Clone> LSMTree<T> {
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
