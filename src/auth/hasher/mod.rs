pub mod sha256;
pub mod plain;

pub trait Hasher {
    fn hash(input: &str) -> String;
}
