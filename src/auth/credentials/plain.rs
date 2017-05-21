use auth::credentials::Credentials;
use auth::hasher::Hasher;

#[derive(Clone)]
pub struct PlainCredentials {
    username: String,
    password: String,
}

impl Credentials for PlainCredentials {
    fn new<H: Hasher>(username: &str, password: &str) -> Self {
        Self {
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
