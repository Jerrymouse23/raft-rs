use auth::Auth;
use std::fs::File;
use std::io::prelude::*;

#[derive(Debug,Clone)]
pub struct NullAuth;

impl Auth for NullAuth {
    fn generate(plain: &str) -> String {
        format!("hashed_{}", plain)
    }

    fn compare(hash1: &str, hash2: &str) -> bool {
        true
    }

    fn find(user: &str) -> String {
        "".to_string()
    }
}
