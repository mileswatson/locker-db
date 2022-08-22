mod appendable_file;
mod file_id;
mod immutable_file;

pub use appendable_file::AppendableFile;
pub use immutable_file::{FileReader, ImmutableFile};

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rocket::tokio;

    use super::immutable_file::ImmutableFile;

    #[tokio::test]
    async fn it_works() {
        let file = ImmutableFile::new(PathBuf::from("./"), "hello".as_bytes())
            .await
            .unwrap();
        let mut reader = file.new_reader().await.unwrap();
        assert_eq!(reader.size(), 5);
        let buf = reader.read_fixed::<3>(2).await.unwrap();
        assert_eq!(buf, "llo".as_bytes());

        let file = ImmutableFile::from_existing(file.path.clone())
            .await
            .unwrap();
        let mut reader = file.new_reader().await.unwrap();
        assert_eq!(reader.size(), 5);
        let buf = reader.read_fixed::<3>(2).await.unwrap();
        assert_eq!(buf, "llo".as_bytes());

        file.delete().await.unwrap();
    }
}
