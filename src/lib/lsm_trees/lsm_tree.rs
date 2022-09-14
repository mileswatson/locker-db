use std::collections::HashMap;
use std::path::PathBuf;
use std::{collections::VecDeque, sync::Arc};

use rocket::serde::{DeserializeOwned, Serialize};

use crate::sstables::sstable_builder::SSTableBuilder;
use crate::sstables::write_buffer::WriteBuffer;

use super::sstable_node::{NextSSTable, SSTableNode};

pub struct LSMTree<T: Serialize + DeserializeOwned> {
    pub(super) dir: PathBuf,
    pub(super) buffer: WriteBuffer<T>,
    pub(super) builders: VecDeque<SSTableBuilder<T>>,
    pub(super) first: NextSSTable<T>,
    pub(super) nodes: HashMap<String, Arc<SSTableNode<T>>>,
}
