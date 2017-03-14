use document::*;
use std::net::SocketAddr;
use uuid::Uuid;
use raft::Result;
use raft::Client;
use raft::LogId;
use raft::TransactionId;
use raft::Error as RError;
use raft::RaftError;

use bincode::serde::serialize as encode;
use bincode::serde::deserialize as decode;
use bincode::SizeLimit;
use std::collections::HashSet;
use std::str::from_utf8;

use raft::auth::credentials::SingleCredentials;
use raft::auth::simple::SimpleAuth;

#[derive(Debug,Serialize,Deserialize)]
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

    fn new_client(addr: &SocketAddr, username: &str, password: &str, lid: &LogId) -> Client {
        Client::new::<SimpleAuth<SingleCredentials>>(Self::to_hashset(*addr),
                                username.to_string(),
                                password.to_string(),
                                *lid)
    }

    pub fn get(addr: &SocketAddr,
               username: &str,
               plain_password: &str,
               id: &Uuid,
               lid: &LogId)
               -> Result<Document> {

        let mut client = Self::new_client(addr, username, plain_password, lid);

        let payload = encode(&Message::Get(*id), SizeLimit::Infinite).unwrap();

        let response = match client.query(payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::get(&parse_addr(&leader_str),
                                    &username,
                                    &plain_password,
                                    &id,
                                    &lid);
            } 
            Err(err) => return Err(err),
        };

        let document: Document = decode(response.as_slice()).unwrap();

        Ok(document)
    }

    pub fn post(addr: &SocketAddr,
                username: &str,
                plain_password: &str,
                document: Document,
                session: &TransactionId,
                lid: &LogId)
                -> Result<Uuid> {

        let mut client = Self::new_client(addr, username, plain_password, lid);

        let payload = encode(&Message::Post(document.clone()), SizeLimit::Infinite).unwrap();

        let response = match client.propose(session, payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::post(&parse_addr(&leader_str),
                                     &username,
                                     &plain_password,
                                     document,
                                     &session,
                                     &lid);
            } 
            Err(err) => return Err(err),
        };

        let uid: Uuid = decode(response.as_slice()).unwrap();

        Ok(uid)
    }

    pub fn remove(addr: &SocketAddr,
                  username: &str,
                  plain_password: &str,
                  id: &Uuid,
                  session: &TransactionId,
                  lid: &LogId)
                  -> Result<()> {
        let mut client = Self::new_client(addr, username, plain_password, lid);

        let payload = encode(&Message::Remove(*id), SizeLimit::Infinite).unwrap();

        match client.propose(session, payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::remove(&parse_addr(&leader_str),
                                       &username,
                                       &plain_password,
                                       &id,
                                       &session,
                                       &lid);
            } 
            Err(err) => return Err(err),
        };

        Ok(())
    }

    pub fn put(addr: &SocketAddr,
               username: &str,
               plain_password: &str,
               id: &Uuid,
               new_payload: Vec<u8>,
               session: &TransactionId,
               lid: &LogId)
               -> Result<()> {

        let mut client = Self::new_client(addr, username, plain_password, lid);

        let payload = encode(&Message::Put(*id, new_payload.clone()), SizeLimit::Infinite).unwrap();

        match client.propose(session, payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::put(&parse_addr(&leader_str),
                                    &username,
                                    &plain_password,
                                    &id,
                                    new_payload,
                                    &session,
                                    &lid);
            } 
            Err(err) => return Err(err),
        };

        Ok(())
    }

    pub fn begin_transaction(addr: &SocketAddr,
                             username: &str,
                             password: &str,
                             session: &TransactionId,
                             lid: &LogId)
                             -> Result<String> {
        let mut client = Self::new_client(addr, username, password, lid);

        let res = client.begin_transaction(session);

        Ok(Uuid::from_bytes(res.unwrap().as_slice()).unwrap().hyphenated().to_string())
    }

    pub fn commit_transaction(addr: &SocketAddr,
                              username: &str,
                              password: &str,
                              lid: &LogId,
                              session: &TransactionId)
                              -> Result<String> {
        let mut client = Self::new_client(addr, username, password, lid);

        let res = client.end_transaction(session);

        Ok(from_utf8(res.unwrap().as_slice()).unwrap().to_string())
    }

    pub fn rollback_transaction(addr: &SocketAddr,
                                username: &str,
                                password: &str,
                                lid: &LogId,
                                session: &TransactionId)
                                -> Result<String> {
        let mut client = Self::new_client(addr, username, password, lid);

        let res = client.rollback_transaction(session);

        Ok(from_utf8(res.unwrap().as_slice()).unwrap().to_string())
    }
}
