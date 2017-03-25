#![allow(dead_code)]
use auth::Auth;
use auth::credentials::Credentials;

use crypto::digest::Digest;
use crypto::sha2::Sha256;

#[derive(Debug,Clone)]
pub struct Sha256Auth<C>
    where C: Credentials
{
    credentials: C,
}

impl<C> Sha256Auth<C>
    where C: Credentials
{
    pub fn new(credentials: C) -> Self {
        Sha256Auth { credentials: credentials }
    }
}

impl<C> Auth for Sha256Auth<C>
    where C: Credentials
{
    fn hash(&self, plain: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.input_str(plain);
        hasher.result_str()
    }

    fn compare(&self, hash1: &str, hash2: &str) -> bool {
        hash1 == hash2
    }

    fn find(&self, user: &str, hash: &str) -> bool {
        let real_user_password = self.credentials.get_password(user);

        self.compare(hash, real_user_password)
    }
}
