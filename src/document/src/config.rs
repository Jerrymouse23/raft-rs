use std::fs::File;
use std::io::Read;
use toml::{Parser, Decoder, Value, DecodeError};
use rustc_serialize::Decodable;

#[derive(Debug,RustcDecodable,Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub peers: Vec<PeerConfig>,
    pub logs: Vec<LogConfig>,
}

#[derive(Debug,RustcDecodable,Clone)]
pub struct ServerConfig {
    pub rest_port: u64,
    pub node_id: u64,
    pub node_address: String,
    pub community_string: String,
    pub binding_addr: String,
    pub volume: String,
}

#[derive(Debug,RustcDecodable,Clone)]
pub struct PeerConfig {
    pub node_id: u64,
    pub node_address: String,
}

#[derive(Debug,RustcDecodable,Clone)]
pub struct LogConfig {
    pub path: String,
    pub lid: String,
}

impl Config {
    pub fn init(file: String) -> Result<Self, DecodeError> {
        let mut config_file = File::open(file.clone())
            .expect(&format!("Unable to read the config {}", file));

        let mut config = String::new();
        config_file.read_to_string(&mut config).expect("Unable to read the config");

        let toml =
            Parser::new(&config).parse().expect("An error occurred while parsing the config");

        let mut decoder = Decoder::new(Value::Table(toml));
        let config = try!(Config::decode(&mut decoder));

        Ok(config)
    }
}
