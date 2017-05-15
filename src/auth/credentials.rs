#![allow(unused_variables)]

use std::fmt::Debug;

pub trait Credentials: Debug + Clone + Send + 'static {
    fn new(username: &str, password: &str) -> Self;
    fn get_username(&self) -> &str;
    fn get_password(&self) -> &str;
}

/// TODO remove Debug
#[derive(Debug,Clone)]
pub struct SingleCredentials {
    username: String,
    password: String,
}

impl Credentials for SingleCredentials {
    fn new(username: &str, password: &str) -> Self {
        SingleCredentials {
            username: username.to_string(),
            password: password.to_string(),
        }
    }

    fn get_username(&self) -> &str {
        &self.username
    }

    fn get_password(&self) -> &str {
        &self.password
    }
}
