use hex::ToHex;
use rand::Rng;

pub fn random_filename() -> String {
    let mut bytes: [u8; 16] = [0; 16];
    rand::thread_rng().fill(&mut bytes);
    let mut filename = bytes.encode_hex::<String>();
    filename.push_str(".db");
    filename
}
