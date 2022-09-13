use std::collections::VecDeque;
use std::path::PathBuf;

use rocket::serde::{DeserializeOwned, Serialize};

use crate::sstables::sstable_builder::SSTableBuilder;
use crate::sstables::write_buffer::WriteBuffer;

use super::sstable_node::NextSSTable;

pub struct LSMTree<T: Serialize + DeserializeOwned> {
    pub(super) dir: PathBuf,
    pub(super) buffer: WriteBuffer<T>,
    pub(super) builders: VecDeque<SSTableBuilder<T>>,
    pub(super) first: NextSSTable<T>,
}
