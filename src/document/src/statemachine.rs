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
use std::io::Read;
use std::io::Write;
use std::io::Error as IoError;

use doclog::DocLog;
use handler::Message;
use document::DocumentId;
use std::collections::HashMap;

use serde_json::to_string;

// TODO implement transaction_offset reset when peer panics
#[derive(Debug,Clone)]
pub struct DocumentStateMachine {
    log: Vec<DocumentRecord>,
    map: HashMap<DocumentId, Document>,
    volume: String,
    transaction_offset: usize,
}

impl DocumentStateMachine {
    // We get a clone from DocLog. Which is ok because we are only using the .get_volume() which
    // won't be changed during the entire execution of the application
    pub fn new(volume: &str) -> Self {
        DocumentStateMachine {
            volume: volume.to_string(),
            map: HashMap::new(),
            log: Vec::new(),
            transaction_offset: 0,
        }
    }

    pub fn get_documents(&self) -> Vec<DocumentId> {
        self.map.keys().into_iter().map(|id| *id).collect()
    }

    fn post(&mut self, document: Document) -> Vec<u8> {
        let record = DocumentRecord::new(document.id,
                                         format!("{}/{}", &self.volume, &document.id.to_string()),
                                         ActionType::Post);

        self.log.push(record);
        self.map.insert(document.id, document.clone());

        encode(&document, SizeLimit::Infinite).unwrap()
    }

    fn remove(&mut self, id: DocumentId) -> Vec<u8> {
        let mut record =
            DocumentRecord::new(id, format!("{}/{}", &self.volume, &id), ActionType::Remove);

        {
            let old_document = self.map.get(&id).unwrap();

            record.set_old_payload(old_document.payload.clone());
            self.log.push(record);
        }
        self.map.remove(&id);

        Vec::new()
    }

    fn put(&mut self, id: DocumentId, new_payload: Vec<u8>) -> Vec<u8> {
        let mut record =
            DocumentRecord::new(id, format!("{}/{}", &self.volume, &id), ActionType::Put);

        {
            let old_document = self.map.get(&id).unwrap();

            record.set_old_payload(old_document.payload.clone());

            assert!(record.get_old_payload().is_some());

            self.log.push(record);
        }

        let mut document = self.map.get_mut(&id).unwrap().clone();
        document.payload = new_payload;


        self.map.remove(&id);
        self.map.insert(id, document.clone());

        encode(&document, SizeLimit::Infinite).unwrap()
    }


    fn findById(&self, id: DocumentId) -> DocumentRecord {
        for s in self.log.iter().rev().skip(self.transaction_offset) {
            if s.get_id() == id {
                return s.clone();
            }
        }

        panic!("Reverting failed")
    }

    pub fn get_snapshot_map(&self) -> Result<Vec<u8>, IoError> {
        let mut fs = try!(File::open(&format!("./{}/snapshot_map", self.volume)));
        let mut buffer = Vec::new();

        try!(fs.read_to_end(&mut buffer));

        Ok(buffer)
    }

    pub fn get_snapshot_log(&self) -> Result<Vec<u8>, IoError> {
        let mut fs = try!(File::open(&format!("./{}/snapshot_log", self.volume)));
        let mut buffer = Vec::new();

        try!(fs.read_to_end(&mut buffer));

        Ok(buffer)
    }
}

impl state_machine::StateMachine for DocumentStateMachine {
    fn apply(&mut self, new_value: &[u8]) -> Vec<u8> {
        let message = decode(&new_value).unwrap();

        let response = match message {
            Message::Get(_) => self.query(new_value),
            Message::Post(document) => self.post(document),
            Message::Remove(id) => self.remove(id),
            Message::Put(id, new_payload) => self.put(id, new_payload),
        };

        self.snapshot();

        println!("{:?}", self.get_documents());

        response
    }

    fn query(&self, query: &[u8]) -> Vec<u8> {
        let message = decode(&query).unwrap();

        let response = match message {
            Message::Get(id) => self.map.get(&id).unwrap().clone().payload,
            _ => {
                let response = encode(&"Wrong usage of .query()", SizeLimit::Infinite);

                response.unwrap()
            }
        };

        response
    }

    fn snapshot(&self) -> (Vec<u8>, Vec<u8>) {

        let mut file = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(&format!("./{}/snapshot_map", self.volume))
            .expect("Unable to create snapshot file");

        let map = encode(&self.map, SizeLimit::Infinite).unwrap();
        file.write_all(&map)
            .expect("Unable to write to the snapshot file");

        let mut file = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(&format!("./{}/snapshot_log", self.volume))
            .expect("Unable to create snapshot file");

        let log = encode(&self.log, SizeLimit::Infinite).unwrap();

        file.write_all(&log)
            .expect("Unable to write to the snapshot file");

        (map, log)
    }

    fn restore_snapshot(&mut self, snap_map: Vec<u8>, snap_log: Vec<u8>) {
        let map: HashMap<DocumentId, Document> = match decode(&snap_map) {
            Ok(m) => m,
            Err(_) => HashMap::new(),
        };

        self.map = map;

        let log: Vec<DocumentRecord> = match decode(&snap_log) {
            Ok(m) => m,
            Err(_) => Vec::new(),
        };

        self.log = log;
    }

    fn revert(&mut self, command: &[u8]) {
        let message = decode(&command).unwrap();

        match message {
            Message::Get(_) => return,
            Message::Post(document) => {
                self.remove(document.id);
            }
            Message::Remove(id) => {
                let record = self.findById(id);
                let document = Document {
                    id: record.get_id(),
                    payload: record.get_old_payload().unwrap(),
                    version: 0,
                };

                self.post(document);
            }
            Message::Put(id, new_payload) => {
                let record = self.findById(id);

                let new_payload = record.get_old_payload().unwrap();
                self.put(id, new_payload);
            }
        }

        self.transaction_offset += 1;

        self.snapshot();
    }

    fn rollback(&mut self) {
        self.transaction_offset = 0;
    }
}
