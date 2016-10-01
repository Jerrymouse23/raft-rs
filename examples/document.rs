#![feature(plugin)]
#![feature(custom_derive)]
#![feature(drop_types_in_const)]

extern crate raft;

#[macro_use]
extern crate iron;
extern crate router;
extern crate params;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate scoped_log;

extern crate docopt;
extern crate rustc_serialize;
extern crate bincode;

extern crate uuid;

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

use iron::prelude::*;
use iron::status;
use router::Router;

use std::error::Error;
use std::io::ErrorKind;

use params::{Params, Value};

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
    document server <rest-port> <id> [<node-id> <node-address>]...
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

        let mut router = Router::new();
        router.get("/document/:fileId", http_get, "get_document");
        router.post("/document/", http_post, "post_document");
        router.delete("/document/:fileId", http_delete, "delete_document");

        let rest_port = args.arg_rest_port.unwrap() as u16;

        std::thread::spawn(move || {
            Iron::new(router).http(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), rest_port));
        });

        fn http_get(req: &mut Request) -> IronResult<Response> {
            let ref fileId = req.extensions
                .get::<Router>()
                .unwrap()
                .find("fileId")
                .unwrap();

            let document = _get(Uuid::parse_str(*fileId).unwrap()).unwrap();

            let encoded = json::encode(&document).unwrap();

            Ok(Response::with((status::Ok, encoded)))
        }

        fn http_post(req: &mut Request) -> IronResult<Response> {

            let map = req.get_ref::<Params>().unwrap();

            match map.find(&["payload"]) {
                Some(&Value::String(ref p)) => {
                    let mut bytes = Vec::new();
                    bytes.extend_from_slice(p.as_bytes());

                    let document = Document {
                        filename: "".to_string(),
                        payload: bytes,
                    };
                    match _put(document) {
                        Ok(id) => Ok(Response::with((status::Ok, format!("{}", id)))),
                        Err(err) => {
                            Ok(Response::with((status::InternalServerError, err.description())))
                        }
                    }
                } 
                _ => Ok(Response::with((status::InternalServerError, "No payload defined"))), 
            }
        }

        fn http_delete(req: &mut Request) -> IronResult<Response> {
            let ref fileId = req.extensions
                .get::<Router>()
                .unwrap()
                .find("fileId")
                .unwrap();

            let res = match _remove(Uuid::parse_str(*fileId).unwrap()) {
                Ok(_) => Response::with((status::Ok, "Ok")),
                Err(err) => Response::with((status::InternalServerError, err.description())),
            };

            Ok(res)
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

fn server(args: &Args) {

    let persistent_log = persistent_log::doc::DocLog::new();

    let id = ServerId::from(args.arg_id.unwrap());

    let mut peers = args.arg_node_id
        .iter()
        .zip(args.arg_node_address.iter())
        .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
        .collect::<HashMap<_, _>>();

    let addr = peers.remove(&id).unwrap();

    let mut state_machine = DocumentStateMachine::new();
    raft::Server::run(id, addr, peers, persistent_log, state_machine).unwrap();
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
                match _put(document) {
                    Ok(id) => encode(&id, SizeLimit::Infinite).unwrap(),
                    Err(err) => encode(&err.description(), SizeLimit::Infinite).unwrap(),
                }
            }
            Remove(id) => {
                match _remove(id) {
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
                match _get(id) {
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

fn _get(id: Uuid) -> Result<Document, DecodingError> {
    let mut handler = try!(File::open(format!("data/{}", id)));

    let mut decoded: Document = try!(decode_from(&mut handler, SizeLimit::Infinite));

    Ok(decoded)
}

fn _put(document: Document) -> Result<Uuid, EncodingError> {
    let id = Uuid::new_v4();

    // TODO implement error-handling
    let mut handler = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .open(format!("data/{}", id))
        .unwrap();

    try!(encode_into(&document, &mut handler, SizeLimit::Infinite));

    Ok(id)
}

fn _remove(id: Uuid) -> Result<String, std::io::Error> {
    try!(remove_file(format!("data/{}", id)));
    Ok("Document deleted".to_string())
}
