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

extern crate scoped_log;

extern crate docopt;
extern crate rustc_serialize;
extern crate bincode;

extern crate uuid;

extern crate toml;

use std::net::{SocketAddr, ToSocketAddrs, SocketAddrV4, Ipv4Addr};
use bincode::rustc_serialize::{encode, decode, encode_into, decode_from};
use bincode::SizeLimit;
use rustc_serialize::json;
use bincode::rustc_serialize::{EncodingError, DecodingError};
use docopt::Docopt;

use raft::{Client, state_machine, persistent_log, ServerId};
use std::collections::HashMap;

use std::fs::File;
use std::fs::OpenOptions;
use std::fs::remove_file;
use std::io::Read;
use std::io::Write;

use Message::*;

use uuid::Uuid;

use std::error::Error;
use std::io::ErrorKind;

mod handler;
mod http_handler;
mod config;

use handler::Handler;
use config::Config;

static USAGE: &'static str = "
A replicated document database.

Commands:

    get     Returns document

    put     Set document

    server  Starts server

Usage:
    document get <doc-id> <node-address>
    document put <node-address> <filepath> <filename>
    document remove <doc-id> <node-address>
    document server <id> <addr> <rest-port> [<node-id> <node-address>]...
    document server --config <config-path>
";

#[derive(Debug,RustcDecodable,Clone)]
struct Args {
    cmd_server: bool,
    cmd_get: bool,
    cmd_put: bool,
    cmd_remove: bool,
    arg_id: Option<u64>,
    arg_doc_id: Option<String>,
    arg_filename: Option<String>,
    arg_node_id: Vec<u64>,
    arg_node_address: Vec<String>,
    arg_filepath: String,
    arg_rest_port: Option<u64>,
    flag_config: bool,
    arg_config_path: Option<String>,
    arg_addr: Option<String>,
}

#[derive(RustcEncodable,RustcDecodable)]
pub enum Message {
    Get(Uuid),
    Put(Document),
    Remove(Uuid),
}

#[derive(RustcEncodable,RustcDecodable,Debug)]
pub struct Document {
    filename: String,
    payload: Vec<u8>,
}

fn main() {
    env_logger::init().unwrap();

    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());

    if args.cmd_server {

        if args.flag_config {
            let config = match Config::init(args.arg_config_path.unwrap()) {
                Ok(config) => config,
                Err(err) => panic!("{}", err),
            };

            let mut node_ids: Vec<u64> = Vec::new();
            let mut node_addreses: Vec<String> = Vec::new();

            for peer in config.peers {
                node_ids.push(peer.node_id);
                node_addreses.push(peer.node_address);
            }

            http_handler::init(config.server.rest_port as u16);
            server(ServerId::from(config.server.node_id),
                   parse_addr(&config.server.node_address),
                   node_ids,
                   node_addreses)
        } else {
            http_handler::init(args.arg_rest_port.unwrap() as u16);
            server(ServerId::from(args.arg_id.unwrap()),
                   parse_addr(&args.arg_addr.unwrap()),
                   args.arg_node_id,
                   args.arg_node_address);
        }
    } else if args.cmd_get {
        get(&args);
    } else if args.cmd_put {
        put(&args);
    } else if args.cmd_remove {
        remove(&args);
    }
}

fn server(serverId: ServerId, addr: SocketAddr, node_id: Vec<u64>, node_address: Vec<String>) {

    let persistent_log = persistent_log::doc::DocLog::new();

    let mut peers = node_id.iter()
        .zip(node_address.iter())
        .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
        .collect::<HashMap<_, _>>();

    let mut state_machine = DocumentStateMachine::new();
    raft::Server::run(serverId, addr, peers, persistent_log, state_machine).unwrap();
}

fn get(args: &Args) {
    let cluster = args.arg_node_address.iter().map(|v| parse_addr(&v)).collect();

    let mut client = Client::new(cluster);

    let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
        Ok(id) => id,
        Err(err) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
    };

    let payload = encode(&Message::Get(id), SizeLimit::Infinite).unwrap();

    let response = client.query(payload.as_slice()).unwrap();

    let document: Document = decode(response.as_slice()).unwrap();

    let mut handler = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .open(&document.filename)
        .unwrap();

    handler.write_all(document.payload.as_slice());

    println!("Written");
}

fn put(args: &Args) {
    let cluster = args.arg_node_address.iter().map(|v| parse_addr(&v)).collect();

    let mut client = Client::new(cluster);

    let filename = (&args.arg_filename).clone().unwrap();
    let mut handler = File::open(&args.arg_filepath).unwrap();
    let mut buffer: Vec<u8> = Vec::new();

    handler.read_to_end(&mut buffer);

    let document = Document {
        filename: filename,
        payload: buffer,
    };

    let payload = encode(&Message::Put(document), SizeLimit::Infinite).unwrap();

    let response = client.propose(payload.as_slice()).unwrap();

    println!("{}", String::from_utf8(response).unwrap())
}

fn remove(args: &Args) {
    let cluster = args.arg_node_address.iter().map(|v| parse_addr(&v)).collect();

    let mut client = Client::new(cluster);

    let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
        Ok(id) => id,
        Err(err) => panic!("{} is not a valid id", &args.arg_doc_id.clone().unwrap()),
    };

    let payload = encode(&Message::Remove(id), SizeLimit::Infinite).unwrap();

    let response = client.propose(payload.as_slice()).unwrap();

    println!("{}", String::from_utf8(response).unwrap())
}

fn parse_addr(addr: &str) -> SocketAddr {
    addr.to_socket_addrs()
        .ok()
        .expect(&format!("unable to parse socket address: {}", addr))
        .next()
        .unwrap()
}

#[derive(Debug,Clone)]
pub struct DocumentStateMachine;

impl DocumentStateMachine {
    pub fn new() -> Self {
        DocumentStateMachine
    }
}

impl state_machine::StateMachine for DocumentStateMachine {
    fn apply(&mut self, new_value: &[u8]) -> Vec<u8> {
        let message = decode(&new_value).unwrap();

        let response = match message {
            Get(id) => self.query(new_value),
            Put(document) => {
                match Handler::put(document) {
                    Ok(id) => encode(&id, SizeLimit::Infinite).unwrap(),
                    Err(err) => encode(&err.description(), SizeLimit::Infinite).unwrap(),
                }
            }
            Remove(id) => {
                match Handler::remove(id) {
                    Ok(id) => encode(&id, SizeLimit::Infinite).unwrap(),
                    Err(err) => encode(&err.description(), SizeLimit::Infinite).unwrap(),
                }
            }
        };

        response
    }

    fn query(&self, query: &[u8]) -> Vec<u8> {
        let message = decode(&query).unwrap();

        let response = match message {
            Get(id) => {
                match Handler::get(id) {
                    Ok(document) => encode(&document, SizeLimit::Infinite).unwrap(),
                    Err(err) => encode(&err.description(), SizeLimit::Infinite).unwrap(),
                }
            }
            _ => {
                let response = encode(&"Wrong usage of .query()", SizeLimit::Infinite);

                response.unwrap()
            }
        };

        response
    }

    fn snapshot(&self) -> Vec<u8> {
        unimplemented!()
    }

    fn restore_snapshot(&mut self, snapshot_value: Vec<u8>) {}
}
