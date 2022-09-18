use std::{ops::Deref, sync::Arc};

use parking_lot::RwLock;

use crate::sstables::sstable::SSTable;

use super::lsm_tree::Heap;

#[derive(Debug)]
pub struct SSTableNode<T> {
    table: SSTable<T>,
    next: NextSSTable<T>,
}

impl<T> SSTableNode<T> {
    pub fn new(table: SSTable<T>, next: NextSSTable<T>, heap: &Heap<T>) -> Arc<SSTableNode<T>> {
        let node = Arc::new(SSTableNode { table, next });
        heap.lock()
            .insert(node.table.id().to_string(), node.clone());
        node
    }

    pub fn table(&self) -> &SSTable<T> {
        &self.table
    }

    pub fn next_lock(&self) -> &NextSSTable<T> {
        &self.next
    }

    pub fn next(&self) -> Option<Arc<SSTableNode<T>>> {
        Some(self.next.read().as_ref()?.clone())
    }

    pub async fn delete(self) {
        self.table.delete().await.unwrap();
    }
}

impl<T> Deref for SSTableNode<T> {
    type Target = SSTable<T>;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

pub type NextSSTable<T> = RwLock<Option<Arc<SSTableNode<T>>>>;
