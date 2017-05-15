#[allow(dead_code)]
use auth::Auth;
use auth::credentials::Credentials;

#[derive(Debug,Clone)]
pub struct SimpleAuth<C>
    where C: Credentials
{
    credentials: C,
    community_string: String,
}

impl<C> SimpleAuth<C>
    where C: Credentials
{
    pub fn new(credentials: C, community_string: String) -> Self {
        SimpleAuth { credentials, community_string}
    }
}

impl<C> Auth for SimpleAuth<C>
    where C: Credentials
{
    fn compare(&self, hash1: &str, hash2: &str) -> bool {
        hash1 == hash2
    }

    fn find(&self, _: &str, hash: &str) -> bool {
        let real_user_password = self.credentials.get_password();

        self.compare(hash, real_user_password)
    }

    fn get_community_string(&self) -> &str {
        &self.community_string
    }

    fn set_community_string(&mut self, cstr: String) {
        self.community_string = cstr;
    }
}
