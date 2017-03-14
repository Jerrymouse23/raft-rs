use std::fmt::Debug;

pub mod null;
pub mod credentials;
pub mod simple;
pub mod sha256;

/// A trait to do authentification
pub trait Auth: Clone + Debug + Send + 'static {
    /// Generates hash of type T
    fn hash(&self,plain: &str) -> String;

    /// Checks hash and returns whether it was successful or not
    fn compare(&self, plain: &str, hash: &str) -> bool;

    fn find(&self, user: &str, hash: &str) -> bool;
}
