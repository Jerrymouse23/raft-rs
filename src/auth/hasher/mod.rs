pub mod sha256;
pub mod plain;

pub use auth::hasher::sha256::Sha256Hasher;

pub trait Hasher {
    fn hash(input: &str) -> String;
}
