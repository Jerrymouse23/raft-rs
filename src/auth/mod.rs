use std::fmt::Debug;

pub mod null;
pub mod credentials;
pub mod simple;
pub mod multi;
pub mod hasher;

use auth::hasher::Hasher;

/// A trait to do authentification
pub trait Auth: Clone + Send + 'static {
    /// Checks hash and returns whether it was successful or not
    fn compare(&self, hash1: &str, hash2: &str) -> bool;

    /// Checks hash with given username
    fn find(&self, user: &str, hash: &str) -> bool;

    fn get_community_string(&self) -> &str;

    fn set_community_string(&mut self, cstr: String);

    fn compare_community_string(&self, cstr: &str) -> bool {
        if self.get_community_string() == cstr {
            true
        } else {
            false
        }
    }
}
