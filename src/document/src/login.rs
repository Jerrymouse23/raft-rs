use base64::{encode, decode};
use bincode::serde::{serialize,deserialize};
use bincode::SizeLimit;

#[derive(Clone, Serialize, Deserialize)]
pub struct Login {
    pub username: String,
    pub hashed_password : String
}

impl Login{
    pub fn new(username: String, hashed_password: String) -> Self{
        Login{
            username,
            hashed_password
        }
    }
}

impl ::iron_sessionstorage::Value for Login {
    fn get_key() -> &'static str {
        "logged_in_user"
    }
    fn into_raw(self) -> String {
        encode(&serialize(&self,SizeLimit::Infinite).unwrap())
    }
    fn from_raw(value: String) -> Option<Self> {
        if value.is_empty() {
            None
        } else {
            let decoded = decode(&value).unwrap();
            let d = deserialize(&decoded).unwrap();

            Some(d)
        }
    }
}
