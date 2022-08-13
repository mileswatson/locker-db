mod file_appender;
mod file_reader;
mod rw_file;

pub use file_appender::FileAppender;
pub use file_reader::FileReader;
pub use rw_file::RWFile;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rocket::tokio;

    use super::RWFile;

    #[tokio::test]
    async fn it_works() {
        let file = RWFile::new(PathBuf::from("./"), "hello".as_bytes())
            .await
            .unwrap();
        let mut reader = file.open().await.unwrap();
        assert_eq!(reader.size().await.unwrap(), 5);
        let buf = &mut [0; 3];
        reader.read(2, buf).await.unwrap();
        assert_eq!(buf, "llo".as_bytes());

        let file = RWFile::from_existing(file.path.clone()).await.unwrap();
        let mut reader = file.open().await.unwrap();
        assert_eq!(reader.size().await.unwrap(), 5);
        let buf = &mut [0; 3];
        reader.read(2, buf).await.unwrap();
        assert_eq!(buf, "llo".as_bytes());

        file.delete().await.unwrap();
    }
}
