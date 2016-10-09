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
pub mod handler;
pub mod http_handler;
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

use raft::RaftError;

use handler::Handler;
use document::*;
use config::*;


static USAGE: &'static str = "
A replicated document database.

Commands:

    get     Return document

    put     Set document

    server  Start server

Usage:
    document get <doc-id> <node-address>
    document put <node-address> <filepath> 
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
    arg_node_id: Vec<u64>,
    arg_node_address: Vec<String>,
    arg_filepath: String,
    arg_rest_port: Option<u64>,
    flag_config: bool,
    arg_config_path: Option<String>,
    arg_addr: Option<String>,
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
        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(err) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        get(args.arg_node_address.iter().map(|v| parse_addr(&v)).collect(),
            id);
    } else if args.cmd_put {
        put(args.arg_node_address.iter().map(|v| parse_addr(&v)).collect(),
            &args.arg_filepath);
    } else if args.cmd_remove {
        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(err) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        remove(args.arg_node_address.iter().map(|v| parse_addr(&v)).collect(),
               id);
    }
}



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
