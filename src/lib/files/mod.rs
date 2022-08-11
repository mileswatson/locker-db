mod file_reader;
mod immutable_file;

pub use file_reader::FileReader;
pub use immutable_file::ImmutableFile;

#[cfg(test)]
mod tests {
    use rocket::tokio;

    use super::ImmutableFile;

    #[tokio::test]
    async fn it_works() {
        let file = ImmutableFile::new("./temp.txt".to_string(), "hello".as_bytes())
            .await
            .unwrap();
        let mut reader = file.open().await.unwrap();
        assert_eq!(reader.size().await.unwrap(), 5);
        let buf = &mut [0; 3];
        reader.read(2, buf).await.unwrap();
        assert_eq!(buf, "llo".as_bytes());

        let file = ImmutableFile::from_existing("./temp.txt".to_string())
            .await
            .unwrap();
        let mut reader = file.open().await.unwrap();
        assert_eq!(reader.size().await.unwrap(), 5);
        let buf = &mut [0; 3];
        reader.read(2, buf).await.unwrap();
        assert_eq!(buf, "llo".as_bytes());

        file.delete().await.unwrap();
    }
}
