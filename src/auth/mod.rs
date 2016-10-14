use std::fmt::Debug;

pub mod file;

/// A trait to do authentification
pub trait Auth<T>: Clone + Debug + Send + 'static {
    /// Generates hash of type T
    fn generate(plain: &str) -> String;

    /// Checks hash and returns whether it was successful or not
    fn compare(plain: &str, hash: &str) -> bool;

    /// Returns hash of given user
    fn find(user: &str) -> String;
}
