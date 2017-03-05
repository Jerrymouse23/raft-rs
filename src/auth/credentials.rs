use std::fmt::Debug;

pub trait Credentials: Debug + Clone + Send + 'static{
    fn get_password(&self, username: &str) -> &str;
}

#[derive(Debug,Clone)]
pub struct SingleCredentials {
    username: String,
    password: String,
}

impl SingleCredentials{
    pub fn new(username: String,password: String) -> Self{
       SingleCredentials{
            username: username,
            password: password
       }
    }
}

impl Credentials for SingleCredentials {
    fn get_password(&self,username: &str) -> &str {
        &self.password
    }
}
