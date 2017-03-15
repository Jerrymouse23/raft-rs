#[allow(dead_code)]
use auth::Auth;
use auth::credentials::Credentials;

#[derive(Debug,Clone)]
pub struct SimpleAuth<C>
    where C: Credentials
{
    credentials: C,
}

impl<C> SimpleAuth<C>
    where C: Credentials
{
    pub fn new(credentials: C) -> Self {
        SimpleAuth { credentials: credentials }
    }
}

impl<C> Auth for SimpleAuth<C>
    where C: Credentials
{
    fn hash(&self, plain: &str) -> String {
        plain.to_string()
    }

    fn compare(&self, hash1: &str, hash2: &str) -> bool {
        if hash1 == hash2 {
            return true;
        } else {
            return false;
        }
    }

    fn find(&self, user: &str, hash: &str) -> bool {
        let real_user_password = self.credentials.get_password(user);

        self.compare(hash, real_user_password)
    }
}
