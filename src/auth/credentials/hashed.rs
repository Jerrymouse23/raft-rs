use auth::credentials::Credentials;
use auth::hasher::Hasher;

#[derive(Clone)]
pub struct BasicCredentials {
    username: String,
    password: String,
}

impl Credentials for BasicCredentials {
    fn new<H: Hasher>(username: &str, password: &str) -> Self {
        Self {
            username: username.to_string(),
            password: H::hash(password),
        }
    }

    fn get_username(&self) -> &str {
        &self.username
    }

    fn get_password(&self) -> &str {
        &self.password
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::hasher::Sha256Hasher;

    #[test]
    fn test_hashing() {
        let username = "kper";
        let plain_password = "123";

        let credentials = BasicCredentials::new::<Sha256Hasher>(username, plain_password);

        assert!(credentials.get_password() != plain_password);
    }
}
