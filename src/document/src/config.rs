use std::fs::File;
use std::io::Read;
use raft::ServerId;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};

use toml::{Parser, Decoder, Value, DecodeError};

use rustc_serialize::Decodable;

use std::collections::BTreeMap;

#[derive(Debug,RustcDecodable,Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug,RustcDecodable,Clone)]
pub struct ServerConfig {
    pub rest_port: u64,
    pub node_id: u64,
    pub node_address: String,
    pub community_string: String,
}

#[derive(Debug,RustcDecodable,Clone)]
pub struct PeerConfig {
    pub node_id: u64,
    pub node_address: String,
}

impl Config {
    pub fn init(file: String) -> Result<Self, DecodeError> {
        let mut config_file = File::open(file).unwrap();

        let mut config = String::new();
        config_file.read_to_string(&mut config);

        let toml = Parser::new(&config).parse().unwrap();

        let mut decoder = Decoder::new(Value::Table(toml));
        let config = try!(Config::decode(&mut decoder));

        Ok(config)
    }
}
