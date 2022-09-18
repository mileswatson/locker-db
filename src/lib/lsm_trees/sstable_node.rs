use std::{ops::Deref, sync::Arc};

use arc_swap::ArcSwap;

use crate::sstables::sstable::SSTable;

use super::lsm_tree::Heap;

#[derive(Debug)]
pub struct SSTableNode<T> {
    table: SSTable<T>,
    next: NextSSTable<T>,
}

impl<T> SSTableNode<T> {
    pub fn new(
        table: SSTable<T>,
        next: NextSSTable<T>,
        heap: &Heap<T>,
    ) -> Arc<Option<SSTableNode<T>>> {
        let id = table.id().to_string();
        let node = Arc::new(Some(SSTableNode { table, next }));
        heap.lock().insert(id, node.clone());
        node
    }

    pub fn table(&self) -> &SSTable<T> {
        &self.table
    }

    pub fn next_lock(&self) -> &NextSSTable<T> {
        &self.next
    }

    pub fn next(&self) -> Arc<Option<SSTableNode<T>>> {
        self.next.load_full()
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

pub type NextSSTable<T> = ArcSwap<Option<SSTableNode<T>>>;
