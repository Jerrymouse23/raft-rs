#[allow(dead_code)]
use auth::Auth;
use auth::credentials::Credentials;
use auth::hasher::Hasher;

use std::mem::replace;

#[derive(Debug,Clone)]
pub struct MultiAuth<C>
    where C: Credentials
{
    credentials: Vec<C>,
    community_string: String
}

pub struct MultiAuthBuilder<C>
    where C: Credentials{
        community_string: Option<String>,
        credentials: Vec<C>
}

impl<C> MultiAuthBuilder<C>
    where C: Credentials{

    fn new() -> MultiAuthBuilder<C>{
        Self{
            community_string: None,
            credentials: Vec::new()
        }
    }

    pub fn with_community_string(mut self,community_string: &str) -> Self{
        self.community_string = Some(community_string.to_string());
        self
    }

    pub fn add_user_plain<H: Hasher>(mut self,username: &str, plain_password: &str) -> Self{
        let hashed_password = H::hash(plain_password);

        self.credentials.push(C::new(username, &hashed_password));
        self
    }

    pub fn add_user_hashed(mut self,username: &str, hashed_password: &str) -> Self{
        self.credentials.push(C::new(username, hashed_password));
        self
    }

    pub fn finalize(&mut self) -> MultiAuth<C>{
        MultiAuth::new(
            self.credentials.clone(),
            replace(&mut self.community_string, None).unwrap_or(String::new())
        )
    }
}

impl<C> MultiAuth<C>
    where C: Credentials
{
    pub fn new(credentials: Vec<C>, community_string: String) -> Self {
        Self { credentials, community_string}
    }

    pub fn build() -> MultiAuthBuilder<C>{
        MultiAuthBuilder::new()
    }
}

impl<C> Auth for MultiAuth<C>
    where C: Credentials
{
    fn compare(&self, hash1: &str, hash2: &str) -> bool {
        hash1 == hash2
    }

    fn find(&self, username: &str, hash: &str) -> bool {
        let user = self.credentials.iter().find(|x| x.get_username() == username);

        if let Some(user) = user {
            self.compare(user.get_password(), hash)
        }
        else{
            false
        }
    }

    fn get_community_string(&self) -> &str {
        &self.community_string
    }

    fn set_community_string(&mut self, cstr: String){
        self.community_string = cstr;
    }
}

#[cfg(test)]
mod tests{
    use super::*;
    use auth::credentials::SingleCredentials;
    use auth::hasher::sha256::Sha256Hasher;

    #[test]
    fn test_MultiAuthBuilder(){
        let mut builder = MultiAuth::<SingleCredentials>::build();
        let mut auth = builder
            .with_community_string("test")
            .add_user_plain::<Sha256Hasher>("kper","123")
            .add_user_hashed("kper2","a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3")
            .finalize();

        let cstr = auth.get_community_string();

        assert_eq!(cstr, "test");
        assert!(auth.find("kper", "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3"));
        assert!(auth.find("kper2","a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3"));

    }
}
