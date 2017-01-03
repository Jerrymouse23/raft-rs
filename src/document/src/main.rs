#![feature(plugin)]
#![feature(custom_derive)]
#![feature(drop_types_in_const)]

extern crate raft;

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate iron;
extern crate router;
extern crate params;

extern crate docopt;
extern crate rustc_serialize;
extern crate bincode;

extern crate uuid;
extern crate toml;

#[macro_use]
extern crate lazy_static;

pub mod document;
pub mod io_handler;
pub mod http_handler;
pub mod handler;
pub mod config;
pub mod doclog;

use std::net::SocketAddr;
use docopt::Docopt;

use raft::ServerId;
use std::collections::HashMap;

use std::fs::File;
use std::io::Read;

use uuid::Uuid;

use raft::Server;
use raft::LogId;
use raft::state_machine::StateMachine;

use document::*;
use config::*;
use handler::Handler;
use doclog::DocLog;

use raft::auth::null::NullAuth;

use http_handler::*;

static USAGE: &'static str = "
A replicated document database.

Commands:

    get     Return document

    put     Set document

    server  Start server

Usage:
    document get <doc-id> <lid> <node-address> <username> <password>
    document put <doc-id> <lid> <node-address> <filepath> <username> <password>
    document post <lid> <node-address> <filepath> <username> <password> 
    document remove <doc-id> <lid> <node-address> <username> <password>
    document server  <config-path>
    document begintrans <lid> <node-address> <username> <password>
    document endtrans <lid> <node-address> <username> <password>
    document rollback <lid> <node-address> <username> <password>
    document transpost <lid> <node-address> <filepath> <username> <password> <transid>
    document transremove <lid> <node-address> <doc-id> <username> <password> <transid>
    document transput <lid> <node-address> <doc-id> <filepath> <username> <password> <transid>
";

#[derive(Debug,RustcDecodable,Clone)]
struct Args {
    cmd_server: bool,
    cmd_get: bool,
    cmd_post: bool,
    cmd_remove: bool,
    cmd_put: bool,
    cmd_begintrans: bool,
    cmd_endtrans: bool,
    cmd_rollback: bool,
    cmd_transpost: bool,
    cmd_transremove: bool,
    cmd_transput: bool,
    arg_id: Option<u64>,
    arg_doc_id: Option<String>,
    arg_node_id: Vec<u64>,
    arg_node_address: Option<String>,
    arg_filepath: String,
    arg_config_path: Option<String>,
    arg_addr: Option<String>,
    arg_password: Option<String>,
    arg_username: Option<String>,
    arg_transid: Option<String>,
    arg_lid: Option<String>,
}
fn main() {
    env_logger::init().unwrap();

    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());

    if args.cmd_server {

        let config = match Config::init(args.arg_config_path.unwrap()) {
            Ok(config) => config,
            Err(err) => panic!("{}", err),
        };

        let mut node_ids: Vec<u64> = Vec::new();
        let mut node_addresses: Vec<String> = Vec::new();

        for peer in config.peers.clone() {
            node_ids.push(peer.node_id);
            node_addresses.push(peer.node_address);
        }

        let local_addr = parse_addr(&config.server.node_address);

        let node_addr = match parse_addr(&config.server.node_address) {
            SocketAddr::V4(v) => v,
            _ => panic!("The node address given must be IPv4"),
        };

        // http_handler::init(parse_addr(&config.server.binding_addr), node_addr);
        server(ServerId::from(config.server.node_id),
               local_addr,
               node_ids,
               node_addresses,
               config.server.community_string.to_string(),
               parse_addr(&config.server.binding_addr),
               &config);
    } else if args.cmd_get {
        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(_) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        get(parse_addr(&args.arg_node_address.unwrap()),
            id,
            args.arg_username.unwrap(),
            args.arg_password.unwrap(),
            LogId::from(&args.arg_lid.unwrap()).unwrap());
    } else if args.cmd_post {
        post(parse_addr(&args.arg_node_address.unwrap()),
             &args.arg_filepath,
             args.arg_username.unwrap(),
             args.arg_password.unwrap(),
             Uuid::new_v4(),
             LogId::from(&args.arg_lid.unwrap()).unwrap());
    } else if args.cmd_remove {
        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(_) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        remove(parse_addr(&args.arg_node_address.unwrap()),
               id,
               args.arg_username.unwrap(),
               args.arg_password.unwrap(),
               Uuid::new_v4(),
               LogId::from(&args.arg_lid.unwrap()).unwrap());
    } else if args.cmd_put {

        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(_) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        put(parse_addr(&args.arg_node_address.unwrap()),
            id,
            args.arg_filepath,
            args.arg_username.unwrap(),
            args.arg_password.unwrap(),
            Uuid::new_v4(),
            LogId::from(&args.arg_lid.unwrap()).unwrap());
    } else if args.cmd_begintrans {
        let res = Handler::begin_transaction(parse_addr(&args.arg_node_address.unwrap()),
                                             &args.arg_username.unwrap(),
                                             &args.arg_password.unwrap(),
                                             Uuid::new_v4(),
                                             LogId::from(&args.arg_lid.unwrap()).unwrap());

        println!("{}", res.unwrap());
    } else if args.cmd_endtrans {
        let res = Handler::commit_transaction(parse_addr(&args.arg_node_address.unwrap()),
                                              &args.arg_username.unwrap(),
                                              &args.arg_password.unwrap(),
                                              LogId::from(&args.arg_lid.unwrap()).unwrap());
        println!("{}", res.unwrap());
    } else if args.cmd_rollback {
        let res = Handler::rollback_transaction(parse_addr(&args.arg_node_address.unwrap()),
                                                &args.arg_username.unwrap(),
                                                &args.arg_password.unwrap(),
                                                LogId::from(&args.arg_lid.unwrap()).unwrap());

        println!("{}", res.unwrap());
    } else if args.cmd_transpost {
        post(parse_addr(&args.arg_node_address.unwrap()),
             &args.arg_filepath,
             args.arg_username.unwrap(),
             args.arg_password.unwrap(),
             Uuid::parse_str(&args.arg_transid.unwrap()).unwrap(),
             LogId::from(&args.arg_lid.unwrap()).unwrap());

    } else if args.cmd_transremove {
        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(_) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        remove(parse_addr(&args.arg_node_address.unwrap()),
               id,
               args.arg_username.unwrap(),
               args.arg_password.unwrap(),
               Uuid::parse_str(&args.arg_transid.unwrap()).unwrap(),
               LogId::from(&args.arg_lid.unwrap()).unwrap());
    } else if args.cmd_transput {
        let id: Uuid = match Uuid::parse_str(&args.arg_doc_id.clone().unwrap()) {
            Ok(id) => id,
            Err(_) => panic!("{} is not a valid id", args.arg_doc_id.clone().unwrap()),
        };

        put(parse_addr(&args.arg_node_address.unwrap()),
            id,
            args.arg_filepath,
            args.arg_username.unwrap(),
            args.arg_password.unwrap(),
            Uuid::parse_str(&args.arg_transid.unwrap()).unwrap(),
            LogId::from(&args.arg_lid.unwrap()).unwrap());
    }
}

fn server(server_id: ServerId,
          addr: SocketAddr,
          node_id: Vec<u64>,
          node_address: Vec<String>,
          community_string: String,
          binding_addr: SocketAddr,
          config: &Config) {

    // let persistent_log1 = DocLog::new("node1", LogId::from(0));
    // let persistent_log2 = DocLog::new("node2", LogId::from(1));
    //    let persistent_log3 = DocLog::new();
    //   let persistent_log4 = DocLog::new();



    let peers = node_id.iter()
        .zip(node_address.iter())
        .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
        .collect::<HashMap<_, _>>();



    let node_addr = match parse_addr(&node_address[0]) {
        SocketAddr::V4(b) => b,
        _ => panic!("The node_address must be IPv4"),
    };


    let mut logs: Vec<(LogId, DocLog, DocumentStateMachine)> = Vec::new();

    for l in config.logs.iter() {
        let logid = LogId::from(&l.lid).expect(&format!("The logid given was invalid {:?}", l.lid));
        let log = DocLog::new(&l.path, LogId::from(&l.lid).unwrap());
        let mut state_machine = DocumentStateMachine::new(log.clone());
        logs.push((logid, log, state_machine));
        println!("Init {:?}", l.lid);
    }

    // TODO implement snapshot
    // match File::open("./snapshot") {
    // Ok(mut handler) => {
    // let mut buffer: Vec<u8> = Vec::new();
    // handler.read_to_end(&mut buffer).expect("Unable to read the snapshot file to end");
    //
    // state_machine.restore_snapshot(buffer);
    // }
    // Err(_) => {
    // println!("No snapshot found! Start from the beginning");
    // }
    // }
    //

    let (mut server, mut event_loop) =
        Server::new(server_id, addr, peers, community_string, NullAuth, logs).unwrap();

    {
        let states = server.log_manager.get_states();

        init(binding_addr, node_addr, states);
    }

    server.init(&mut event_loop);

    event_loop.run(&mut server);
}

fn get(addr: SocketAddr, doc_id: Uuid, username: String, password: String, lid: LogId) {
    let document = Handler::get(addr, &username, &password, doc_id, lid);
    println!("{:?}", document);
}

fn post(addr: SocketAddr,
        filepath: &str,
        username: String,
        password: String,
        session: Uuid,
        lid: LogId) {

    let mut handler = File::open(&filepath).expect(&format!("Unable to open the file{}", filepath));
    let mut buffer: Vec<u8> = Vec::new();

    handler.read_to_end(&mut buffer).expect(&format!("Unable read the file to end {}", filepath));

    let document = Document {
        id: Uuid::new_v4(),
        payload: buffer,
        version: 1,
    };

    let id = match Handler::post(addr, &username, &password, document, session, lid) {
        Ok(id) => id,
        Err(err) => panic!(err),
    };

    println!("{}", id);
}

fn put(addr: SocketAddr,
       doc_id: Uuid,
       filepath: String,
       username: String,
       password: String,
       session: Uuid,
       lid: LogId) {

    let mut handler = File::open(&filepath).expect(&format!("Unable to open the file{}", filepath));
    let mut buffer: Vec<u8> = Vec::new();

    handler.read_to_end(&mut buffer).expect(&format!("Unable read the file to end {}", filepath));

    match Handler::put(addr, &username, &password, doc_id, buffer, session, lid) {
        Ok(()) => {

        }
        Err(err) => panic!(err),
    }
}

fn remove(addr: SocketAddr,
          doc_id: Uuid,
          username: String,
          password: String,
          session: Uuid,
          lid: LogId) {
    match Handler::remove(addr, &username, &password, doc_id, session, lid) {
        Ok(()) => println!("Ok"),
        Err(err) => panic!(err),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
