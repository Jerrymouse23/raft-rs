use handler::Handler;
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

#[derive(RustcEncodable,RustcDecodable)]
pub enum Message {
    Get(Uuid),
    Put(Document),
    Remove(Uuid),
}

#[derive(RustcEncodable,RustcDecodable,Debug)]
pub struct Document {
    pub payload: Vec<u8>,
}

pub fn server(serverId: ServerId, addr: SocketAddr, node_id: Vec<u64>, node_address: Vec<String>) {

    let persistent_log = DocLog::new();

    let mut peers = node_id.iter()
        .zip(node_address.iter())
        .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
        .collect::<HashMap<_, _>>();

    let mut state_machine = DocumentStateMachine::new();
    Server::run(serverId, addr, peers, persistent_log, state_machine).unwrap();
}

pub fn get(node_addr: HashSet<SocketAddr>, doc_id: Uuid) {
    let mut client = Client::new(node_addr);

    let payload = encode(&Message::Get(doc_id), SizeLimit::Infinite).unwrap();

    let response = match client.query(payload.as_slice()) {
        Ok(res) => res,
        Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
            let mut cluster = HashSet::new();
            cluster.insert(parse_addr(&leader_str));
            return get(cluster, doc_id);
        } 
        Err(err) => panic!(err),
    };

    let document: Document = decode(response.as_slice()).unwrap();

    println!("{:?}", document);
}

pub fn put(node_addr: HashSet<SocketAddr>, filepath: &str) {
    let mut client = Client::new(node_addr);

    let mut handler = File::open(&filepath).unwrap();
    let mut buffer: Vec<u8> = Vec::new();

    handler.read_to_end(&mut buffer);

    let document = Document { payload: buffer };

    let payload = encode(&Message::Put(document), SizeLimit::Infinite).unwrap();

    let response = match client.propose(payload.as_slice()) {
        Ok(res) => res,
        Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
            let mut cluster = HashSet::new();
            cluster.insert(parse_addr(&leader_str));
            return put(cluster, filepath);
        } 
        Err(err) => panic!(err),
    };

    println!("{}", String::from_utf8(response).unwrap())
}

pub fn remove(node_addr: HashSet<SocketAddr>, doc_id: Uuid) {
    let mut client = Client::new(node_addr);

    let payload = encode(&Message::Remove(doc_id), SizeLimit::Infinite).unwrap();

    let response = match client.propose(payload.as_slice()) {
        Ok(res) => res,
        Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
            let mut cluster = HashSet::new();
            cluster.insert(parse_addr(&leader_str));
            return remove(cluster, doc_id);
        } 
        Err(err) => panic!(err),
    };

    println!("{}", String::from_utf8(response).unwrap())
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
            Message::Put(document) => {
                match Handler::put(document) {
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
