#![feature(plugin)]
#![feature(custom_derive)]
#![feature(drop_types_in_const)]

extern crate raft;

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate iron;
extern crate router;
extern crate params;

extern crate docopt;
extern crate rustc_serialize;
extern crate bincode;

extern crate uuid;

extern crate toml;

pub mod document;
pub mod io_handler;
pub mod http_handler;
pub mod handler;
pub mod config;

use std::net::{SocketAddr, ToSocketAddrs, SocketAddrV4, Ipv4Addr};
use bincode::rustc_serialize::{encode, decode, encode_into, decode_from};
use bincode::SizeLimit;
use rustc_serialize::json;
use bincode::rustc_serialize::{EncodingError, DecodingError};
use docopt::Docopt;

use raft::{Client, state_machine, persistent_log, ServerId};
use std::collections::HashMap;
use std::collections::HashSet;

use std::fs::File;
use std::fs::OpenOptions;
use std::fs::remove_file;
use std::io::Read;
use std::io::Write;

use uuid::Uuid;

use std::error::Error;
use std::io::ErrorKind;

use raft::Server;
use raft::RaftError;
use raft::persistent_log::doc::DocLog;

use document::*;
use config::*;
use handler::Handler;

static USAGE: &'static str = "
A replicated document database.

Commands:

    get     Return document

    put     Set document

    server  Start server

Usage:
    document get <doc-id> <node-address> <username> <password>
    document post <node-address> <filepath> <username> <password> 
    document remove <doc-id> <node-address> <username> <password>
    document server  <config-path>
";

#[derive(Debug,RustcDecodable,Clone)]
struct Args {
    cmd_server: bool,
    cmd_get: bool,
    cmd_post: bool,
    cmd_remove: bool,
    arg_id: Option<u64>,
    arg_doc_id: Option<String>,
    arg_node_id: Vec<u64>,
    arg_node_address: Option<String>,
    arg_filepath: String,
    arg_config_path: Option<String>,
    arg_addr: Option<String>,
    arg_password: Option<String>,
    arg_username: Option<String>,
}
fn main() {
    env_logger::init().unwrap();

    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());

    if args.cmd_server {

        let config = match Config::init(args.arg_config_path.unwrap()) {
            Ok(config) => config,
            Err(err) => panic!("{}", err),
        };

        let mut node_ids: Vec<u64> = Vec::new();
        let mut node_addresses: Vec<String> = Vec::new();

        for peer in config.peers {
            node_ids.push(peer.node_id);
            node_addresses.push(peer.node_address);
        }

        let local_addr = parse_addr(&config.server.node_address);

        let node_addr = match parse_addr(&config.server.node_address) {
            SocketAddr::V4(v) => v,
            _ => panic!("The node address given must be IPv4"),
        };

        http_handler::init(parse_addr(&config.server.binding_addr), node_addr);
        server(ServerId::from(config.server.node_id),
               local_addr,
               node_ids,
               node_addresses,
               config.server.community_string.to_string());
    } else if args.cmd_get {
        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(err) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        get(parse_addr(&args.arg_node_address.unwrap()),
            id,
            args.arg_username.unwrap(),
            args.arg_password.unwrap());
    } else if args.cmd_post {
        post(parse_addr(&args.arg_node_address.unwrap()),
             &args.arg_filepath,
             args.arg_username.unwrap(),
             args.arg_password.unwrap());
    } else if args.cmd_remove {
        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(err) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        remove(parse_addr(&args.arg_node_address.unwrap()),
               id,
               args.arg_username.unwrap(),
               args.arg_password.unwrap());
    }
}

fn server(serverId: ServerId,
          addr: SocketAddr,
          node_id: Vec<u64>,
          node_address: Vec<String>,
          community_string: String) {

    let persistent_log = DocLog::new();

    let mut peers = node_id.iter()
        .zip(node_address.iter())
        .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
        .collect::<HashMap<_, _>>();

    let mut state_machine = DocumentStateMachine::new();
    Server::run(serverId,
                addr,
                peers,
                persistent_log,
                state_machine,
                community_string)
        .unwrap();
}

fn get(addr: SocketAddr, doc_id: Uuid, username: String, password: String) {
    let document = Handler::get(addr, &username, &password, doc_id);
    println!("{:?}", document);
}

fn post(addr: SocketAddr, filepath: &str, username: String, password: String) {
    let mut handler = File::open(&filepath).expect(&format!("Could not find file {}", filepath));
    let mut buffer: Vec<u8> = Vec::new();

    handler.read_to_end(&mut buffer);

    let document = Document { payload: buffer };

    let id = Handler::post(addr, &username, &password, document).unwrap();

    println!("{}", id);
}

fn remove(addr: SocketAddr, doc_id: Uuid, username: String, password: String) {
    Handler::remove(addr, &username, &password, doc_id).unwrap();

    println!("Ok");
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
