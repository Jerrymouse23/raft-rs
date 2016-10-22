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

#[cfg(test)]
mod tests {

    extern crate mio;
    extern crate capnp;

    use raft::{Server, ServerId};
    use raft::Log;
    use raft::state_machine::NullStateMachine;
    use raft::auth::Auth;
    use raft::auth::null::NullAuth;
    use raft::persistent_log::MemLog;
    use self::mio::EventLoop;
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::collections::HashMap;
    use handler::Handler;
    use document::Document;
    use uuid::Uuid;
    use raft::Result;
    use std::str::FromStr;
    use self::capnp::message::ReaderOptions;
    use self::capnp::serialize;
    use raft::messages_capnp::connection_preamble;
    use std::io::{Write, Read};

    type TestServer = Server<MemLog, NullStateMachine, NullAuth>;

    static USERNAME: &str = "username";
    static PASSWORD: &str = "password";

    fn new_test_server(peers: HashMap<ServerId, SocketAddr>)
                       -> Result<(TestServer, EventLoop<TestServer>)> {
        Server::new(ServerId::from(0),
                    SocketAddr::from_str("127.0.0.1:0").unwrap(),
                    peers,
                    MemLog::new(),
                    NullStateMachine,
                    "test".to_string(),
                    NullAuth)
    }

    fn read_server_preamble<R>(read: &mut R) -> ServerId
        where R: Read
    {
        let message = serialize::read_message(read, ReaderOptions::new()).unwrap();
        let preamble = message.get_root::<connection_preamble::Reader>().unwrap();

        match preamble.get_id().which().unwrap() {
            connection_preamble::id::Which::Server(peer) => ServerId::from(peer.unwrap().get_id()),
            _ => panic!("unexpected preamble id"),
        }
    }

    #[ignore]
    #[test]
    // FIXME
    fn test_get() {
        let mut peers = HashMap::new();
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        let doc = Document {
            id: Uuid::new_v4(),
            payload: b"Hello world!".to_vec(),
            version: 0,
        };

        let id = Handler::post(server.get_local_addr(), USERNAME, PASSWORD, doc.clone()).unwrap();

        let doc2 = Handler::get(server.get_local_addr(), USERNAME, PASSWORD, id).unwrap();

        assert_eq!(doc, doc2);
    }

    #[ignore]
    #[test]
    // FIXME
    fn test_post() {
        let mut peers = HashMap::new();
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        let doc = Document {
            id: Uuid::new_v4(),
            payload: b"Hello world!".to_vec(),
            version: 0,
        };

        let id = Handler::post(server.get_local_addr(), USERNAME, PASSWORD, doc.clone()).unwrap();
    }

    #[ignore]
    #[test]
    // FIXME
    fn test_put() {
        let mut peers = HashMap::new();
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        let doc = Document {
            id: Uuid::new_v4(),
            payload: b"Hello world!".to_vec(),
            version: 0,
        };

        let id = Handler::post(server.get_local_addr(), USERNAME, PASSWORD, doc.clone()).unwrap();

        let new_payload = b"This is updated! :P".to_vec();

        Handler::put(server.get_local_addr(), USERNAME, PASSWORD, id, new_payload).unwrap();

        let doc2 = Handler::get(server.get_local_addr(), USERNAME, PASSWORD, id).unwrap();

        assert_eq!(doc2.payload, b"This is updated! :P".to_vec());
    }

    #[ignore]
    #[test]
    // FIXME
    fn test_remove() {
        let mut peers = HashMap::new();
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        let doc = Document {
            id: Uuid::new_v4(),
            payload: b"Hello world!".to_vec(),
            version: 0,
        };

        let id = Handler::post(server.get_local_addr(), USERNAME, PASSWORD, doc.clone()).unwrap();

        Handler::remove(server.get_local_addr(), USERNAME, PASSWORD, doc.id.clone()).unwrap();
    }
}
