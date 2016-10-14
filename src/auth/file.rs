use auth::Auth;
use std::fs::File;
use std::io::prelude::*;
use crypto::digest::Digest;
use crypto::sha2::Sha256;

#[derive(Debug,Clone)]
pub struct FileAuth;

impl Auth<String> for FileAuth {
    fn generate(plain: &str) -> String {
        let mut sha = Sha256::new();
        sha.input_str(plain);
        sha.result_str()
    }

    fn compare(hash1: &str, hash2: &str) -> bool {
        if hash1.trim() == hash2.trim() {
            true
        } else {
            false
        }
    }

    fn find(user: &str) -> String {
        let mut file = File::open("user_creds.creds").unwrap();

        let mut s = String::new();

        file.read_to_string(&mut s);

        s
    }
}
