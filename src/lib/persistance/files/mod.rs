mod appendable_file;
mod immutable_file;
mod file_id;

pub use appendable_file::AppendableFile;
pub use immutable_file::ImmutableFile;

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
        assert_eq!(reader.size().await.unwrap(), 5);
        let buf = &mut [0; 3];
        reader.read(2, buf).await.unwrap();
        assert_eq!(buf, "llo".as_bytes());

        let file = ImmutableFile::from_existing(file.path.clone())
            .await
            .unwrap();
        let mut reader = file.new_reader().await.unwrap();
        assert_eq!(reader.size().await.unwrap(), 5);
        let buf = &mut [0; 3];
        reader.read(2, buf).await.unwrap();
        assert_eq!(buf, "llo".as_bytes());

        file.delete().await.unwrap();
    }
}
