#[allow(dead_code)]
use auth::Auth;
use auth::credentials::Credentials;

#[derive(Debug,Clone)]
pub struct NullAuth<C>
    where C: Credentials
{
    credentials: C,
}

impl<C> NullAuth<C>
    where C: Credentials
{
    pub fn new(credentials: C) -> Self {
        NullAuth { credentials: credentials }
    }
}

impl<C> Auth for NullAuth<C>
    where C: Credentials
{
    fn hash(&self,plain: &str) -> String {
        format!("hashed_{}", plain)
    }

    fn compare(&self, hash1: &str, hash2: &str) -> bool {
        true
    }

    fn find(&self, user: &str, hash: &str) -> bool {
        true
    }
}
