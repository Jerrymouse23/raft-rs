#![allow(dead_code)]
#![allow(unused_variables)]

use auth::Auth;
use auth::credentials::Credentials;

#[derive(Clone)]
pub struct NullAuth<C>
    where C: Credentials
{
    credentials: C,
    community_string: String,
}

impl<C> NullAuth<C>
    where C: Credentials
{
    pub fn new(credentials: C, community_string: String) -> Self {
        NullAuth { credentials, community_string  }
    }
}

impl<C> Auth for NullAuth<C>
    where C: Credentials
{
    fn compare(&self, hash1: &str, hash2: &str) -> bool {
        true
    }

    fn find(&self, user: &str, hash: &str) -> bool {
        true
    }

    fn get_community_string(&self) -> &str {
        &self.community_string
    }

    fn set_community_string(&mut self, cstr: String) {
        self.community_string = cstr;
    }
}
