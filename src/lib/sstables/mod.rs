pub mod sstable;
pub mod sstable_builder;
pub mod write_buffer;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rocket::tokio;

    use crate::core::{
        entry::{Entry, EntryData},
        key::Key,
    };

    use super::write_buffer::WriteBuffer;

    #[tokio::test]
    async fn test() {
        let wb = WriteBuffer::create(PathBuf::from("./12398123.wal")).await;
        let (k1, k2, k3) = (Key::new(), Key::new(), Key::new());
        let sequence = vec![
            Entry::new(k1.clone(), EntryData::Data("okay1".to_string())),
            Entry::new(k2.clone(), EntryData::Data("ok2".into())),
            Entry::new(k1.clone(), EntryData::Deleted),
            Entry::new(k3.clone(), EntryData::Data("okayyy3".into())),
        ];
        for x in sequence {
            wb.write(x).await
        }
        assert_eq!(wb.read(&k1), Some(EntryData::Deleted));
        assert_eq!(wb.read(&k2), Some(EntryData::Data("ok2".into())));
        assert_eq!(wb.read(&k3), Some(EntryData::Data("okayyy3".into())));
        let b = wb.to_builder().await;
        assert_eq!(b.read(&k1), Some(&EntryData::Deleted));
        assert_eq!(b.read(&k2), Some(&EntryData::Data("ok2".into())));
        assert_eq!(b.read(&k3), Some(&EntryData::Data("okayyy3".into())));
        let t = b.build().await;
        let mut r = t.reader().await.unwrap();
        assert_eq!(r.read(&k1).await, Some(EntryData::Deleted));
        assert_eq!(r.read(&k2).await, Some(EntryData::Data("ok2".into())));
        assert_eq!(r.read(&k3).await, Some(EntryData::Data("okayyy3".into())));
        t.delete().await.unwrap();
        b.delete().await;
    }
}
