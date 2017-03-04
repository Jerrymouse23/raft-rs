use document::*;
use raft::state_machine;

use bincode::serde::serialize as encode;
use bincode::serde::deserialize as decode;
use bincode::serde::deserialize_from as decode_from;
use bincode::SizeLimit;
use uuid::Uuid;
use std::net::{SocketAddr, ToSocketAddrs};
use std::error::Error;

use std::fs::File;
use std::fs::OpenOptions;
use std::fs::read_dir;
use std::io::Write;

use io_handler::Handler;
use doclog::DocLog;
use handler::Message;

//TODO implement transaction_offset reset when peer panics
#[derive(Debug,Clone)]
pub struct DocumentStateMachine {
    map: Vec<DocumentRecord>,
    transaction_offset: usize, //savepoint
    volume: String,
}

impl DocumentStateMachine {
    // We get a clone from DocLog. Which is ok because we are only using the .get_volume() which
    // won't be changed during the entire execution of the application
    pub fn new(volume: &str) -> Self {
        DocumentStateMachine {
            volume: volume.to_string(),
            map: Vec::new(),
            transaction_offset: 0,
        }
    }

    // TODO implement it without IO
    // TODO remove expect
    pub fn get_documents(&self) -> Vec<DocumentId> {
        let mut result = Vec::new();

        let files = read_dir(&self.volume).unwrap();

        for f in files {
            let f = f.unwrap();
            let filename = f.file_name();

            let id = match Uuid::parse_str(&filename.to_str().unwrap().trim()) {
                Ok(id) => id,
                Err(_) => continue,
            };

            result.push(id);
        }

        result
    }
}

impl state_machine::StateMachine for DocumentStateMachine {
    fn apply(&mut self, new_value: &[u8]) -> Vec<u8> {
        let message = decode(&new_value).unwrap();

        let response = match message {
            Message::Get(_) => self.query(new_value),
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
                let mut document = File::open(format!("{}/{}", &self.volume, &id))
                    .expect(&format!("{}/{}", &self.volume, &id));

                let old_payload = decode_from(&mut document, SizeLimit::Infinite).unwrap();

                match Handler::remove(id, &self.volume) {
                    Ok(_) => {

                        let mut record = DocumentRecord::new(id,
                                                             format!("{}/{}", &self.volume, &id),
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

        println!("{:?}", self.get_documents());

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
                for record in self.map.clone()[0..(self.map.clone().len() -
                                                   self.transaction_offset)]
                    .iter()
                    .rev() {
                    if record.get_id() == id {

                        let document = Document {
                            id: record.get_id(),
                            payload: record.get_old_payload().unwrap(),
                            version: 0,
                        };

                        match Handler::post(document, &self.volume) {
                            Ok(_) => {

                                let mut new_record =
                                    DocumentRecord::new(record.get_id().clone(),
                                                        format!("{}/{}", &self.volume, &record.get_id()),
                                                        ActionType::Post);

                                new_record.set_old_payload(record.get_old_payload().unwrap());

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

                    if record.get_id() == id {

                        match Handler::put(id,
                                           record.get_old_payload().unwrap().as_slice(),
                                           &self.volume) {
                            Ok(_) => {
                                let mut new_record =
                                    DocumentRecord::new(record.get_id().clone(),
                                                        format!("{}/{}", &self.volume, &record.get_id()),
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