#![feature(plugin)]
#![feature(custom_derive)]
#![plugin(serde_macros)]

extern crate raft;

#[macro_use]
extern crate rustful;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate scoped_log;

extern crate docopt;
extern crate serde;
extern crate serde_json;
extern crate rustc_serialize;
extern crate bincode;

use std::error::Error;
use rustful::{Server, Context, Response, TreeRouter, Router};
use rustful::Method;
use std::net::{SocketAddr, ToSocketAddrs};
use serde_json::Value;
use docopt::Docopt;

use raft::{Client, state_machine, persistent_log, ServerId};
use std::collections::HashMap;

use Message::*;

static USAGE: &'static str = "
A replicated document database.

Commands:

    get     Returns document

    put     Set document

    server  Starts server

Usage:
    document get <doc_id> <node-address>
    document put <node-address>
    document remove <node-address>
    document server <id> [<node-id> <node-address>]...
";

#[derive(Debug,RustcDecodable)]
struct Args {
    cmd_server: bool,
    cmd_get: bool,
    cmd_put: bool,
    cmd_remove: bool,
    arg_id: Option<u64>,
    arg_node_id: Vec<u64>,
    arg_node_address: Vec<String>,
}

#[derive(Serialize,Deserialize)]
pub enum Message {
    Get(u64),
    Put,
}

fn main() {
    env_logger::init().unwrap();

    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());

    if args.cmd_server {


        let mut router = TreeRouter::new();

        router.insert(Method::Get,
                      "documents",
                      get_all_documents as fn(Context, Response));
        router.insert(Method::Post,
                      "documents",
                      post_new_document as fn(Context, Response));

        let server_result = Server {
                host: 9000.into(),
                handlers: router,
                ..Server::default()
            }
            .run();

        match server_result {
            Ok(_server) => println!("Server started"),
            Err(e) => error!("could not start server: {}", e.description()),
        }

        server(&args);

    } else if args.cmd_get {
        get(&args);
    } else if args.cmd_put {
        put(&args);
    } else if args.cmd_remove {
        remove(&args);
    }
}

fn get_all_documents(context: Context, response: Response) {
    unimplemented!()
}
fn post_new_document(context: Context, response: Response) {
    unimplemented!()
}

fn server(args: &Args) {

    let persistent_log = persistent_log::doc::DocLog::new();

    let state_machine = DocumentStateMachine::new();

    let id = ServerId::from(args.arg_id.unwrap());

    let mut peers = args.arg_node_id
        .iter()
        .zip(args.arg_node_address.iter())
        .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
        .collect::<HashMap<_, _>>();

    let addr = peers.remove(&id).unwrap();

    raft::Server::run(id, addr, peers, persistent_log, state_machine).unwrap();
}

fn get(args: &Args) {
    unimplemented!()
}

fn put(args: &Args) {
    unimplemented!()
}

fn remove(args: &Args) {
    unimplemented!()
}

fn parse_addr(addr: &str) -> SocketAddr {
    addr.to_socket_addrs()
        .ok()
        .expect(&format!("unable to parse socket address: {}", addr))
        .next()
        .unwrap()
}

#[derive(Debug)]
pub struct DocumentStateMachine;

impl DocumentStateMachine {
    pub fn new() -> Self {
        DocumentStateMachine
    }
}

impl state_machine::StateMachine for DocumentStateMachine {
    fn apply(&mut self, new_value: &[u8]) -> Vec<u8> {
        unimplemented!()
    }

    fn query(&self, query: &[u8]) -> Vec<u8> {
        unimplemented!()
    }

    fn snapshot(&self) -> Vec<u8> {
        unimplemented!()
    }

    fn restore_snapshot(&mut self, snaphost_value: Vec<u8>) {
        unimplemented!()
    }
}
