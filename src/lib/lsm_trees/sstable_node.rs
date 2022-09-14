use std::{ops::Deref, sync::Arc};

use rocket::tokio::sync::RwLock;

use crate::sstables::sstable::SSTable;

pub struct SSTableNode<T> {
    table: SSTable<T>,
    next: NextSSTable<T>,
}

impl<T> SSTableNode<T> {
    pub fn new(table: SSTable<T>, next: NextSSTable<T>) -> SSTableNode<T> {
        SSTableNode { table, next }
    }

    pub fn table(&self) -> &SSTable<T> {
        &self.table
    }

    pub async fn next_lock(&self) -> &NextSSTable<T> {
        &self.next
    }

    pub async fn next(&self) -> Option<Arc<SSTableNode<T>>> {
        Some(self.next.as_ref()?.read().await.clone())
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

pub type NextSSTable<T> = Option<RwLock<Arc<SSTableNode<T>>>>;
