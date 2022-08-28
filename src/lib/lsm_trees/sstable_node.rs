use std::sync::Arc;

use rocket::tokio::sync::RwLock;

use crate::sstables::sstable::SSTable;

pub struct SSTableNode<T> {
    table: SSTable<T>,
    next: NextSSTable<T>,
}

impl<T> SSTableNode<T> {
    pub fn table(&self) -> &SSTable<T> {
        &self.table
    }
    pub fn next(&self) -> Option<Arc<SSTableNode<T>>> {
        Some(self.next.as_ref()?.blocking_read().clone())
    }
}

pub type NextSSTable<T> = Option<RwLock<Arc<SSTableNode<T>>>>;
