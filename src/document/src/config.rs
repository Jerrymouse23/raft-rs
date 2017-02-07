use std::fs::File;
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
use uuid::Uuid;
use parser::Parser;
use parser::toml::Parser as tParser;
use toml::DecodeError;
use raft::LogId;

#[derive(Debug,Deserialize,Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub peers: Vec<PeerConfig>,
    pub logs: Vec<LogConfig>,
}

#[derive(Debug,Deserialize,Clone)]
pub struct ServerConfig {
    pub node_id: u64,
    pub node_address: String,
    pub community_string: String,
    pub binding_addr: String,
    pub dynamic_peering: Option<String>,
}

#[derive(Debug,Deserialize,Clone)]
pub struct PeerConfig {
    pub node_id: u64,
    pub node_address: String,
}

#[derive(Debug,Deserialize,Clone)]
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
        let server: Vec<SocketAddr> = self.server.node_address.to_socket_addrs().unwrap().collect();
        server[0]
    }

    pub fn get_binding_addr(&self) -> SocketAddr {
        self.server.binding_addr.parse().expect("Binding address is invalid")
    }

    pub fn get_dynamic_peering(&self) -> Option<SocketAddr> {
        match self.server.dynamic_peering {
            Some(ref leader_str) => {
                let server: Vec<SocketAddr> = leader_str.to_socket_addrs().unwrap().collect();

                Some(server[0])
            }
            None => None,
        }
    }

    pub fn get_peers_id(&self) -> Vec<u64> {
        self.peers.iter().map(|x| x.node_id).collect()
    }

    pub fn get_nodes(&self) -> (Vec<u64>, Vec<SocketAddr>) {
        let mut node_ids: Vec<u64> = Vec::new();
        let mut node_addresses: Vec<SocketAddr> = Vec::new();

        for peer in self.peers.clone().into_iter() {
            node_ids.push(peer.node_id);

            let addr: Vec<SocketAddr> = peer.node_address.to_socket_addrs().unwrap().collect();

            node_addresses.push(addr[0]);
        }

        (node_ids, node_addresses)
    }
}

impl PeerConfig {
    pub fn get_node_addr(&self) -> SocketAddr {
        let addr: Vec<SocketAddr> = self.node_address
            .to_socket_addrs()
            .unwrap()
            .collect();

        addr[0]
    }
}

impl LogConfig {
    pub fn get_log_id(&self) -> Uuid {
        self.lid.parse().expect("LogId of a log is invalid")
    }
}
