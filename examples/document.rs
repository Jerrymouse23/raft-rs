#![feature(plugin)]
#![feature(custom_derive)]
#![plugin(serde_macros)]

extern crate raft;

#[macro_use]
extern crate iron;
extern crate router;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate scoped_log;

extern crate docopt;
extern crate rustc_serialize;
extern crate bincode;

extern crate uuid;

use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use bincode::rustc_serialize::{encode, decode};
use bincode::SizeLimit;
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

use iron::prelude::*;
use iron::status;
use router::Router;

static USAGE: &'static str = "
A replicated document database.

Commands:

    get     Returns document

    put     Set document

    server  Starts server

Usage:
    document get <doc-id> <node-address>
    document put <node-address> <filename> <filepath>
    document remove <doc-id> <node-address>
    document server <id> [<node-id> <node-address>]...
";

static mut state_machine: DocumentStateMachine = DocumentStateMachine::new();

#[derive(Debug,RustcDecodable)]
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
}

#[derive(RustcEncodable,RustcDecodable)]
pub enum Message {
    Get(Uuid),
    Put(Document),
    Remove(Uuid),
}

#[derive(RustcEncodable,RustcDecodable)]
pub struct Document {
    filename: String,
    payload: Vec<u8>,
}

fn handler(req: &mut Request) -> IronResult<Response> {
    let ref query = req.extensions.get::<Router>().unwrap().find("query").unwrap();
    Ok(Response::with((status::Ok, *query)))
}

fn main() {
    env_logger::init().unwrap();

    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());

    if args.cmd_server {

        let mut router = Router::new();
        router.get("/:query", handler, "index");

        Iron::new(router).http("localhost:3000").unwrap();

        server(&args);

    } else if args.cmd_get {
        get(&args);
    } else if args.cmd_put {
        put(&args);
    } else if args.cmd_remove {
        remove(&args);
    }
}

fn server(args: &Args) {

    let persistent_log = persistent_log::doc::DocLog::new();

    // let state_machine = DocumentStateMachine::new();

    let id = ServerId::from(args.arg_id.unwrap());

    let mut peers = args.arg_node_id
        .iter()
        .zip(args.arg_node_address.iter())
        .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
        .collect::<HashMap<_, _>>();

    let addr = peers.remove(&id).unwrap();

    unsafe {
        raft::Server::run(id, addr, peers, persistent_log, state_machine).unwrap();
    }
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

#[derive(Debug)]
pub struct DocumentStateMachine {
    map: HashMap<Uuid, String>,
}

impl DocumentStateMachine {
    pub fn new() -> Self {
        DocumentStateMachine { map: HashMap::new() }
    }
}

impl state_machine::StateMachine for DocumentStateMachine {
    fn apply(&mut self, new_value: &[u8]) -> Vec<u8> {
        let message = decode(&new_value).unwrap();

        let response = match message {
            Get(id) => self.query(new_value),
            Put(document) => {

                let id = Uuid::new_v4();
                self.map.insert(id, document.filename.clone());

                let mut handler = OpenOptions::new()
                    .read(false)
                    .write(true)
                    .create(true)
                    .open(format!("data/{}", id));

                let response = match handler {
                    Ok(mut h) => {
                        h.write_all(document.payload.as_slice());

                        encode(&format!("{}", id), SizeLimit::Infinite)
                    }
                    Err(err) => encode(&err.description(), SizeLimit::Infinite),
                };

                response.unwrap()
            }
            Remove(id) => {
                self.map.remove(&id);

                let response = match remove_file(format!("data/{}", id)) {
                    Ok(()) => encode(&"Document deleted!", SizeLimit::Infinite),
                    Err(err) => encode(&err.description(), SizeLimit::Infinite),
                };

                response.unwrap()
            }
        };

        response
    }

    fn query(&self, query: &[u8]) -> Vec<u8> {
        let message = decode(&query).unwrap();

        let response = match message {
            Get(id) => {
                let handler = File::open(format!("data/{}", id));

                let response = match handler {
                    Ok(mut h) => {
                        let mut content: Vec<u8> = Vec::new();
                        h.read_to_end(&mut content);

                        match self.map.get(&id) {
                            Some(name) => {
                                let document = Document {
                                    filename: name.clone(),
                                    payload: content,
                                };

                                encode(&document, SizeLimit::Infinite)
                            }
                            None => encode(&"Entry not found in our hashmap", SizeLimit::Infinite), 
                        }

                    }
                    Err(err) => encode(&"Entry not found", SizeLimit::Infinite),
                };

                response.unwrap()

            } 
            _ => {
                let response = encode(&"Wrong usage of .query()", SizeLimit::Infinite);

                response.unwrap()
            }
        };

        response
    }

    fn snapshot(&self) -> Vec<u8> {
        encode(&self.map, SizeLimit::Infinite).unwrap()
    }

    fn restore_snapshot(&mut self, snapshot_value: Vec<u8>) {
        self.map = decode(&snapshot_value).unwrap();
        ()
    }
}
