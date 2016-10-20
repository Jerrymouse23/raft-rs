use document::*;
use std::net::SocketAddr;
use uuid::Uuid;
use raft::Result;
use raft::Client;
use raft::Error as RError;
use raft::state_machine;
use raft::RaftError;
use raft::ServerId;
use raft::persistent_log::doc::DocLog;
use bincode::rustc_serialize::{encode, encode_into, decode, decode_from};
use bincode::SizeLimit;
use std::collections::HashSet;

use raft::auth::null::NullAuth;

#[derive(RustcEncodable,RustcDecodable)]
pub enum Message {
    Get(Uuid),
    Post(Document),
    Remove(Uuid),
    Put(Uuid, Vec<u8>),
}

pub struct Handler;

impl Handler {
    fn to_hashset(addr: SocketAddr) -> HashSet<SocketAddr> {
        let mut hashset: HashSet<SocketAddr> = HashSet::new();
        hashset.insert(addr);
        hashset
    }

    fn new_client(addr: SocketAddr, username: &str, password: &str) -> Client {
        Client::new::<NullAuth>(Self::to_hashset(addr),
                                username.to_string(),
                                password.to_string())
    }

    pub fn get(addr: SocketAddr,
               username: &str,
               plain_password: &str,
               id: Uuid)
               -> Result<Document> {
        let mut client = Self::new_client(addr, username, plain_password);

        let payload = encode(&Message::Get(id), SizeLimit::Infinite).unwrap();

        let response = match client.query(payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::get(parse_addr(&leader_str), username, plain_password, id);
            } 
            Err(err) => panic!(err),
        };

        let document: Document = decode(response.as_slice()).unwrap();

        Ok(document)
    }

    pub fn post(addr: SocketAddr,
                username: &str,
                plain_password: &str,
                document: Document)
                -> Result<Uuid> {
        let mut client = Self::new_client(addr, username, plain_password);

        let payload = encode(&Message::Post(document.clone()), SizeLimit::Infinite).unwrap();

        let response = match client.propose(payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::post(parse_addr(&leader_str), username, plain_password, document);
            } 
            Err(err) => panic!(err),
        };

        let uid: Uuid = decode(response.as_slice()).unwrap();

        Ok(uid)
    }

    pub fn remove(addr: SocketAddr, username: &str, plain_password: &str, id: Uuid) -> Result<()> {
        let mut client = Self::new_client(addr, username, plain_password);

        let payload = encode(&Message::Remove(id), SizeLimit::Infinite).unwrap();

        let response = match client.propose(payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::remove(parse_addr(&leader_str), username, plain_password, id);
            } 
            Err(err) => panic!(err),
        };

        Ok(())
    }

    pub fn put(addr: SocketAddr,
               username: &str,
               plain_password: &str,
               id: Uuid,
               new_payload: Vec<u8>)
               -> Result<()> {

        let mut client = Self::new_client(addr, username, plain_password);

        let payload = encode(&Message::Put(id, new_payload.clone()), SizeLimit::Infinite).unwrap();

        let response = match client.propose(payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::put(parse_addr(&leader_str),
                                    username,
                                    plain_password,
                                    id,
                                    new_payload);
            } 
            Err(err) => panic!(err),
        };

        Ok(())
    }
}
