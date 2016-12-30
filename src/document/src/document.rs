use bincode::rustc_serialize::{encode, decode, decode_from};
use bincode::SizeLimit;
use uuid::Uuid;
use std::net::{SocketAddr, ToSocketAddrs};
use std::error::Error;

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;

use raft::state_machine;

use handler::Message;
use io_handler::Handler;
use doclog::DocLog;

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
    map: Vec<DocumentRecord>,
    transaction_offset: usize,
    log: DocLog,
}

impl DocumentStateMachine {
    // We get a clone from DocLog. Which is ok because we are only using the .get_volume() which
    // won't be changed during the entire execution of the application
    pub fn new(log: DocLog) -> Self {
        DocumentStateMachine {
            log: log,
            map: Vec::new(),
            transaction_offset: 0,
        }
    }
}

impl state_machine::StateMachine for DocumentStateMachine {
    fn apply(&mut self, new_value: &[u8]) -> Vec<u8> {
        let message = decode(&new_value).unwrap();

        let response = match message {
            Message::Get(_) => self.query(new_value),
            Message::Post(document) => {
                match Handler::post(document, &self.log.get_volume()) {
                    Ok(id) => {

                        self.map.push(DocumentRecord::new(id.clone(),
                                                          format!("{}/{}",
                                                                  &self.log.get_volume(),
                                                                  &id.to_string()),
                                                          ActionType::Post));
                        self.snapshot();

                        encode(&id, SizeLimit::Infinite).unwrap()
                    }
                    Err(err) => encode(&err.description(), SizeLimit::Infinite).unwrap(),
                }
            }
            Message::Remove(id) => {
                let mut document = File::open(format!("{}/{}", &self.log.get_volume(), &id))
                    .expect(&format!("{}/{}", &self.log.get_volume(), &id));

                let old_payload = decode_from(&mut document, SizeLimit::Infinite).unwrap();

                match Handler::remove(id, &self.log.get_volume()) {
                    Ok(_) => {

                        let mut record =
                            DocumentRecord::new(id,
                                                format!("{}/{}", &self.log.get_volume(), &id),
                                                ActionType::Remove);

                        record.set_old_payload(old_payload);

                        self.map.push(record);

                        self.snapshot();

                        encode(&id, SizeLimit::Infinite).unwrap()
                    }

                    Err(err) => encode(&err.description(), SizeLimit::Infinite).unwrap(),
                }
            }
            Message::Put(id, new_payload) => {
                match Handler::put(id, new_payload.as_slice(), &self.log.get_volume()) {
                    Ok(_) => {
                        let mut record =
                            DocumentRecord::new(id,
                                                format!("{}/{}", &self.log.get_volume(), &id),
                                                ActionType::Remove);

                        let mut document =
                            File::open(format!("{}/{}", &self.log.get_volume(), &id)).unwrap();

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
                match Handler::get(id, &self.log.get_volume()) {
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
            .expect("Unable to create snapshot file");

        file.write_all(&encoded).expect("Unable to write to the snapshot file");

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
            Message::Get(_) => return, //cannot revert
            Message::Post(document) => {
                match Handler::remove(document.id, &self.log.get_volume()) {
                    Ok(_) => {
                        let mut record = DocumentRecord::new(document.id,
                                                             format!("{}/{}",
                                                                     &self.log.get_volume(),
                                                                     &document.id),
                                                             ActionType::Remove);

                        record.set_old_payload(encode(&document, SizeLimit::Infinite).unwrap());

                        self.snapshot();
                    }
                    Err(_) => panic!(),
                }
            }
            Message::Remove(id) => {
                for record in self.map.clone()[0..(self.map.clone().len() -
                                                   self.transaction_offset)]
                    .iter()
                    .rev() {
                    if record.id == id {

                        let document = Document {
                            id: record.id,
                            payload: record.clone().old.unwrap(),
                            version: 0,
                        };

                        match Handler::post(document, &self.log.get_volume()) {
                            Ok(_) => {

                                let mut new_record = DocumentRecord::new(record.id.clone(),
                                                                         format!("{}/{}",
                                                                &self.log.get_volume(),
                                                                &record.id),
                                                                         ActionType::Post);

                                new_record.set_old_payload(record.clone().old.unwrap());

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
            Message::Put(id, new_payload) => {
                for record in self.map.clone()[0..(self.map.clone().len() -
                                                   self.transaction_offset)]
                    .iter()
                    .rev() {

                    if record.id == id {

                        match Handler::put(id,
                                           record.clone().old.unwrap().as_slice(),
                                           &self.log.get_volume()) {
                            Ok(_) => {
                                let mut new_record = DocumentRecord::new(record.id.clone(),
                                                                         format!("{}/{}",
                                                                &self.log.get_volume(),
                                                                &record.id),
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
