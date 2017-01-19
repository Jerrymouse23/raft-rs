use std::fs::File;
use std::io::Read;
use rustc_serialize::Decodable;
use std::net::SocketAddr;
use uuid::Uuid;
use parser::Parser;
use parser::toml::Parser as tParser;
use toml::DecodeError;

#[derive(Debug,RustcDecodable,Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub peers: Vec<PeerConfig>,
    pub logs: Vec<LogConfig>,
}

#[derive(Debug,RustcDecodable,Clone)]
pub struct ServerConfig {
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
    pub fn init(file: &str) -> Result<Self, DecodeError> {
        let mut config_file = File::open(file)
            .expect(&format!("Unable to read the config {}", file));

        let mut config = String::new();
        config_file.read_to_string(&mut config).expect("Unable to read the config");

        tParser::parse(&config)
    }

    pub fn get_node_addr(&self) -> SocketAddr {
        self.server.node_address.parse().expect("Node address is invalid")
    }

    pub fn get_binding_addr(&self) -> SocketAddr {
        self.server.binding_addr.parse().expect("Binding address is invalid")
    }

    pub fn get_peers_id(&self) -> Vec<u64> {
        self.peers.iter().map(|x| x.node_id).collect()
    }

    pub fn get_nodes(&self) -> (Vec<u64>, Vec<String>) {
        let mut node_ids: Vec<u64> = Vec::new();
        let mut node_addresses: Vec<String> = Vec::new();

        for peer in self.peers.clone().into_iter() {
            node_ids.push(peer.node_id);
            node_addresses.push(peer.node_address);
        }

        (node_ids, node_addresses)
    }
}

impl PeerConfig {
    pub fn get_node_addr(&self) -> SocketAddr {
        self.node_address.parse().expect("Node address of a peer is invalid")
    }
}

impl LogConfig {
    pub fn get_log_id(&self) -> Uuid {
        self.lid.parse().expect("LogId of a log is invalid")
    }
}
