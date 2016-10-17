use bincode::rustc_serialize::{encode, encode_into, decode, decode_from};
use bincode::SizeLimit;
use uuid::Uuid;
use std::net::{SocketAddr, ToSocketAddrs};
use std::collections::{HashSet, HashMap};
use std::error::Error;

use std::fs::File;
use std::fs::OpenOptions;
use std::fs::remove_file;
use std::io::Read;
use std::io::Write;

use raft::state_machine;
use raft::RaftError;
use raft::Error as RError;
use raft::Client;
use raft::ServerId;
use raft::Server;

use raft::persistent_log::doc::DocLog;
use handler::Message;
use io_handler::ioHandler as Handler;

#[derive(RustcEncodable,RustcDecodable,Debug,Clone)]
pub struct Document {
    pub payload: Vec<u8>,
}
pub fn parse_addr(addr: &str) -> SocketAddr {
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
            Message::Get(id) => self.query(new_value),
            Message::Post(document) => {
                match Handler::post(document) {
                    Ok(id) => encode(&id, SizeLimit::Infinite).unwrap(),
                    Err(err) => encode(&err.description(), SizeLimit::Infinite).unwrap(),
                }
            }
            Message::Remove(id) => {
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
            Message::Get(id) => {
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
