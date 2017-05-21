#![allow(unused_variables)]

mod plain;
mod hashed;

pub use auth::credentials::plain::PlainCredentials;
pub use auth::credentials::hashed::BasicCredentials;

use auth::hasher::Hasher;

pub trait Credentials: Clone + Send + 'static {
    fn new<H: Hasher>(username: &str, password: &str) -> Self;
    fn get_username(&self) -> &str;
    fn get_password(&self) -> &str;
}
