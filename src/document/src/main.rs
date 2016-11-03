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
use raft::persistent_log::mem::MemLog;

use document::*;
use config::*;
use handler::Handler;

use raft::auth::null::NullAuth;
use raft::auth::Auth;

use http_handler::*;

static USAGE: &'static str = "
A replicated document database.

Commands:

    get     Return document

    put     Set document

    server  Start server

Usage:
    document get <doc-id> <node-address> <username> <password>
    document put <doc-id> <node-address> <filepath> <username> <password>
    document post <node-address> <filepath> <username> <password> 
    document remove <doc-id> <node-address> <username> <password>
    document server  <config-path>
    document begintrans <node-address> <username> <password>
    document endtrans <node-address> <username> <password>
    document rollback <node-address> <username> <password>
    document transpost <node-address> <filepath> <username> <password> <transid>
    document transremove <node-address> <filepath> <username> <password> <transid>
";

#[derive(Debug,RustcDecodable,Clone)]
struct Args {
    cmd_server: bool,
    cmd_get: bool,
    cmd_post: bool,
    cmd_remove: bool,
    cmd_put: bool,
    cmd_begintrans: bool,
    cmd_endtrans: bool,
    cmd_rollback: bool,
    cmd_transpost: bool,
    cmd_transremove: bool,
    arg_id: Option<u64>,
    arg_doc_id: Option<String>,
    arg_node_id: Vec<u64>,
    arg_node_address: Option<String>,
    arg_filepath: String,
    arg_config_path: Option<String>,
    arg_addr: Option<String>,
    arg_password: Option<String>,
    arg_username: Option<String>,
    arg_transid: Option<String>,
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

        for peer in config.peers.clone() {
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
               config.server.community_string.to_string(),
               parse_addr(&config.server.binding_addr),
               &config);
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
             args.arg_password.unwrap(),
             Uuid::new_v4());
    } else if args.cmd_remove {
        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(err) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        remove(parse_addr(&args.arg_node_address.unwrap()),
               id,
               args.arg_username.unwrap(),
               args.arg_password.unwrap(),
               Uuid::new_v4());
    } else if args.cmd_put {

        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(err) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        put(parse_addr(&args.arg_node_address.unwrap()),
            id,
            args.arg_filepath,
            args.arg_username.unwrap(),
            args.arg_password.unwrap());
    } else if args.cmd_begintrans {
        let res = Handler::begin_transaction(parse_addr(&args.arg_node_address.unwrap()),
                                             &args.arg_username.unwrap(),
                                             &args.arg_password.unwrap(),
                                             Uuid::new_v4());

        println!("{}", res.unwrap());
    } else if args.cmd_endtrans {
        let res = Handler::end_transaction(parse_addr(&args.arg_node_address.unwrap()),
                                           &args.arg_username.unwrap(),
                                           &args.arg_password.unwrap());
        println!("{}", res.unwrap());
    } else if args.cmd_rollback {
        let res = Handler::rollback_transaction(parse_addr(&args.arg_node_address.unwrap()),
                                                &args.arg_username.unwrap(),
                                                &args.arg_password.unwrap());

        println!("{}", res.unwrap());
    } else if args.cmd_transpost {
        post(parse_addr(&args.arg_node_address.unwrap()),
             &args.arg_filepath,
             args.arg_username.unwrap(),
             args.arg_password.unwrap(),
             Uuid::parse_str(&args.arg_transid.unwrap()).unwrap());

    } else if args.cmd_transremove {
        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(err) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        remove(parse_addr(&args.arg_node_address.unwrap()),
               id,
               args.arg_username.unwrap(),
               args.arg_password.unwrap(),
               Uuid::parse_str(&args.arg_transid.unwrap()).unwrap());
    }
}

fn server(serverId: ServerId,
          addr: SocketAddr,
          node_id: Vec<u64>,
          node_address: Vec<String>,
          community_string: String,
          binding_addr: SocketAddr,
          config: &Config) {

    let persistent_log = MemLog::new();

    let mut peers = node_id.iter()
        .zip(node_address.iter())
        .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
        .collect::<HashMap<_, _>>();

    let mut state_machine = DocumentStateMachine::new(config.server.volume.clone());

    let node_addr = match parse_addr(&node_address[0]) {
        SocketAddr::V4(b) => b,
        _ => panic!("The node_address must be IPv4"),
    };

    init(binding_addr, node_addr);

    Server::run(serverId,
                addr,
                peers,
                persistent_log,
                state_machine,
                community_string,
                NullAuth)
        .unwrap();
}

fn get(addr: SocketAddr, doc_id: Uuid, username: String, password: String) {
    let document = Handler::get(addr, &username, &password, doc_id);
    println!("{:?}", document);
}

fn post(addr: SocketAddr, filepath: &str, username: String, password: String, session: Uuid) {
    let mut handler = File::open(&filepath).expect(&format!("Could not find file {}", filepath));
    let mut buffer: Vec<u8> = Vec::new();

    handler.read_to_end(&mut buffer);

    let id = Uuid::new_v4();

    let document = Document {
        id: id,
        payload: buffer,
        version: 1,
    };

    let id = Handler::post(addr, &username, &password, document, session).unwrap();

    println!("{}", id);
}

fn put(addr: SocketAddr, doc_id: Uuid, filepath: String, username: String, password: String) {
    let mut handler = File::open(&filepath).expect(&format!("Could not find file {}", filepath));
    let mut buffer: Vec<u8> = Vec::new();

    handler.read_to_end(&mut buffer);

    Handler::put(addr, &username, &password, doc_id, buffer);
}

fn remove(addr: SocketAddr, doc_id: Uuid, username: String, password: String, session: Uuid) {
    Handler::remove(addr, &username, &password, doc_id, session).unwrap();

    println!("Ok");
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
