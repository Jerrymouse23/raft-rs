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

use handler::Message;
use io_handler::ioHandler as Handler;

#[derive(RustcEncodable,RustcDecodable,Debug,Clone,Eq,PartialEq)]
pub struct Document {
    pub id: Uuid,
    pub payload: Vec<u8>,
    pub version: usize,
}

impl Document {
    pub fn put(&mut self, new_payload: Vec<u8>) {
        self.payload = new_payload;

        self.version += 1;
    }
}

pub fn parse_addr(addr: &str) -> SocketAddr {
    addr.to_socket_addrs()
        .ok()
        .expect(&format!("unable to parse socket address: {}", addr))
        .next()
        .unwrap()
}

#[derive(Debug,Clone,RustcEncodable,RustcDecodable)]
pub enum ActionType {
    Get,
    Put,
    Post,
    Remove,
}

#[derive(Debug,Clone,RustcDecodable,RustcEncodable)]
struct DocumentRecord {
    id: Uuid,
    path: String,
    method: ActionType,
    old: Option<Vec<u8>>,
}

impl DocumentRecord {
    pub fn new(id: Uuid, path: String, method: ActionType) -> Self {
        DocumentRecord {
            id: id,
            path: path,
            method: method,
            old: None,
        }
    }

    pub fn set_old_payload(&mut self, old: Vec<u8>) {
        self.old = Some(old);
    }
}

#[derive(Debug,Clone)]
pub struct DocumentStateMachine {
    volume: String,
    map: Vec<DocumentRecord>,
    transaction_offset: usize,
}

impl DocumentStateMachine {
    pub fn new(volume: String) -> Self {
        DocumentStateMachine {
            volume: volume,
            map: Vec::new(),
            transaction_offset: 0,
        }
    }
}

impl state_machine::StateMachine for DocumentStateMachine {
    fn apply(&mut self, new_value: &[u8]) -> Vec<u8> {
        let message = decode(&new_value).unwrap();

        let response = match message {
            Message::Get(id) => self.query(new_value),
            Message::Post(document) => {
                match Handler::post(document, &self.volume) {
                    Ok(id) => {
                        self.map.push(DocumentRecord::new(id.clone(),
                                                          format!("{}/{}",
                                                                  &self.volume,
                                                                  &id.to_string()),
                                                          ActionType::Post));
                        self.snapshot();

                        encode(&id, SizeLimit::Infinite).unwrap()
                    }
                    Err(err) => encode(&err.description(), SizeLimit::Infinite).unwrap(),
                }
            }
            Message::Remove(id) => {
                match Handler::remove(id, &self.volume) {
                    Ok(id) => {
                        let mut record = DocumentRecord::new(Uuid::parse_str(&id.clone()).unwrap(),

                                                             format!("{}/{}", &self.volume, &id),
                                                             ActionType::Remove);

                        let mut document = File::open(format!("{}/{}", &self.volume, &id)).unwrap();

                        record.set_old_payload(decode_from(&mut document, SizeLimit::Infinite)
                            .unwrap());

                        self.map.push(record);

                        self.snapshot();

                        encode(&id, SizeLimit::Infinite).unwrap()
                    }

                    Err(err) => encode(&err.description(), SizeLimit::Infinite).unwrap(),
                }
            }
            Message::Put(id, new_payload) => {
                match Handler::put(id, new_payload.as_slice(), &self.volume) {
                    Ok(_) => {
                        let mut record = DocumentRecord::new(id,
                                                             format!("{}/{}", &self.volume, &id),
                                                             ActionType::Remove);

                        let mut document = File::open(format!("{}/{}", &self.volume, &id)).unwrap();

                        record.set_old_payload(decode_from(&mut document, SizeLimit::Infinite)
                            .unwrap());

                        self.map.push(record);
                        self.snapshot();

                        encode(&id, SizeLimit::Infinite).unwrap()
                    }
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
                match Handler::get(id, &self.volume) {
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
        let encoded = encode(&self.map, SizeLimit::Infinite).unwrap();

        let mut file = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open("./snapshot")
            .unwrap();

        file.write_all(&encoded);

        let v: Vec<u8> = Vec::new();

        v
    }

    fn restore_snapshot(&mut self, snapshot_value: Vec<u8>) {
        let map: Vec<DocumentRecord> = match decode(&snapshot_value) {
            Ok(m) => m,
            Err(_) => Vec::new(),
        };

        self.map = map;
    }

    fn revert(&mut self, command: &[u8]) {
        let message = decode(&command).unwrap();

        match message {
            Message::Get(id) => return, //cannot revert
            Message::Post(document) => {
                match Handler::remove(document.id, &self.volume) {
                    Ok(_) => {
                        let mut record =
                            DocumentRecord::new(document.id,
                                                format!("{}/{}", &self.volume, &document.id),
                                                ActionType::Remove);

                        record.set_old_payload(encode(&document, SizeLimit::Infinite).unwrap());

                        self.snapshot();
                    }
                    Err(_) => panic!(),
                }
            }
            Message::Remove(id) => {
                for record in self.map.clone()[self.transaction_offset..].iter().rev() {
                    if record.id == id {
                        match Handler::post(decode(&mut record.clone().old.unwrap()).unwrap(),
                                            &self.volume) {
                            Ok(id) => {
                                let mut new_record =
                                    DocumentRecord::new(record.id.clone(),
                                                        format!("{}/{}", &self.volume, &record.id),
                                                        ActionType::Post);

                                new_record.set_old_payload(record.clone().old.unwrap());

                                self.map.push(new_record);

                                self.snapshot();

                                self.transaction_offset += 1;
                                break;
                            }
                            Err(err) => panic!(),
                        }
                    }
                }
            }
            Message::Put(id, new_payload) => {
                for record in self.map.clone()[self.transaction_offset..].iter().rev() {

                    if record.id == id {
                        match Handler::put(id,
                                           record.clone().old.unwrap().as_slice(),
                                           &self.volume) {
                            Ok(_) => {
                                let mut new_record =
                                    DocumentRecord::new(record.id.clone(),
                                                        format!("{}/{}", &self.volume, &record.id),
                                                        ActionType::Put);

                                new_record.set_old_payload(new_payload.clone());

                                self.map.push(new_record);

                                self.snapshot();
                                self.transaction_offset += 1;
                                break;
                            } 
                            Err(err) => panic!(err),
                        }
                    }
                }
            }
        }
    }

    fn rollback(&mut self) {
        self.transaction_offset = 0;
    }
}
