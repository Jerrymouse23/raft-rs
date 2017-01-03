use iron::status;
use router::Router;
use iron::prelude::*;
use params::{Params, Value};
use std::fs::read_dir;

use uuid::Uuid;
use std::net::{SocketAddr, ToSocketAddrs, SocketAddrV4, Ipv4Addr};

use std::error::Error;

use rustc_serialize::json;
use rustc_serialize::base64::{self, ToBase64, FromBase64, STANDARD};
use rustc_serialize::hex::{ToHex, FromHex};

use document::*;
use handler::Handler;

use std::thread::spawn;
use std::collections::HashSet;
use std::boxed::Box;

use raft::LogId;
use raft::state::{LeaderState, CandidateState, FollowerState};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(RustcDecodable,RustcEncodable)]
struct http_Response {
    payload: String,
    version: usize,
}

#[derive(Clone,Copy)]
struct Context {
    node_addr: SocketAddrV4,
}

pub fn init(binding_addr: SocketAddr,
            node_addr: SocketAddrV4,
            states: HashMap<LogId,
                            (Arc<RwLock<LeaderState>>,
                             Arc<RwLock<CandidateState>>,
                             Arc<RwLock<FollowerState>>)>) {
    let mut router = Router::new();
    let states = Arc::new(states);
    let context = Context { node_addr: node_addr };

    router.get("/document/:fileId",
               move |request: &mut Request| http_get(request, &context),
               "get_document");
    router.post("/document",
                move |request: &mut Request| http_post(request, &context),
                "post_document");
    router.delete("/document/:fileId",
                  move |request: &mut Request| http_delete(request, &context),
                  "delete_document");
    router.put("/document",
               move |request: &mut Request| http_put(request, &context),
               "put_document");

    router.get("/document",
               move |request: &mut Request| http_get_keys(request, &context),
               "get_document_keys");

    router.post("/transaction/begin",
                move |request: &mut Request| http_begin_transaction(request, &context),
                "begin_transaction");

    router.post("/transaction/commit",
                move |request: &mut Request| http_commit_transaction(request, &context),
                "commit_transaction");

    router.post("/transaction/rollback",
                move |request: &mut Request| http_rollback_transaction(request, &context),
                "rollback_transaction");

    {
        let states = states.clone();
        router.get("/meta/:lid/state/leader",
                   move |request: &mut Request| {
                       http_meta_state_leader(request, &context, states.clone())
                   },
                   "meta_state_leader");
    }
    {
        let states = states.clone();
        router.get("/meta/:lid/state/candidate",
                   move |request: &mut Request| {
                       http_meta_state_candidate(request, &context, states.clone())
                   },
                   "meta_state_candidate");
    }
    {
        router.get("/meta/:lid/state/follower",
                   move |request: &mut Request| {
                       http_meta_state_follower(request, &context, states.clone())
                   },
                   "meta_state_follower");
    }
    fn http_meta_state_leader(req: &mut Request,
                              context: &Context,
                              state: Arc<HashMap<LogId,
                                                 (Arc<RwLock<LeaderState>>,
                                                  Arc<RwLock<CandidateState>>,
                                                  Arc<RwLock<FollowerState>>)>>)
                              -> IronResult<Response> {

        let raw_lid = req.extensions.get::<Router>().unwrap().find("lid").unwrap();
        let lid = LogId::from(raw_lid.to_string()).unwrap();
        let lock = state.get(&lid).unwrap().0.read().unwrap();

        Ok(Response::with((status::Ok, "meta_state_leader")))
    }

    fn http_meta_state_candidate(req: &mut Request,
                                 context: &Context,
                                 state: Arc<HashMap<LogId,
                                                    (Arc<RwLock<LeaderState>>,
                                                     Arc<RwLock<CandidateState>>,
                                                     Arc<RwLock<FollowerState>>)>>)
                                 -> IronResult<Response> {

        let raw_lid = req.extensions.get::<Router>().unwrap().find("lid").unwrap();
        let lid = LogId::from(raw_lid.to_string()).unwrap();
        let lock = state.get(&lid).unwrap().1.read().unwrap();

        Ok(Response::with((status::Ok, "http_meta_state_candidate")))
    }

    fn http_meta_state_follower(req: &mut Request,
                                context: &Context,
                                state: Arc<HashMap<LogId,
                                                   (Arc<RwLock<LeaderState>>,
                                                    Arc<RwLock<CandidateState>>,
                                                    Arc<RwLock<FollowerState>>)>>)
                                -> IronResult<Response> {

        let raw_lid = req.extensions.get::<Router>().unwrap().find("lid").unwrap();
        let lid = LogId::from(raw_lid.to_string()).unwrap();
        let lock = state.get(&lid).unwrap().2.read().unwrap();

        Ok(Response::with((status::Ok, "http_meta_state_follower")))
    }

    fn http_get_keys(req: &mut Request, context: &Context) -> IronResult<Response> {
        let keys = read_dir("data1").unwrap();
        let mut response = "".to_owned();
        for key in keys {
            response.push_str(key.unwrap().path().to_str().unwrap());
            response.push_str(";");
        }
        Ok(Response::with((status::Ok, response)))
    }


    spawn(move || {
        Iron::new(router).http(binding_addr);
    });

    fn test(req: &mut Request) -> IronResult<Response> {
        Ok(Response::with((status::Ok, "I am runnnig :D")))
    }

    fn http_get(req: &mut Request, context: &Context) -> IronResult<Response> {
        let ref fileId = req.extensions
            .get::<Router>()
            .unwrap()
            .find("fileId")
            .unwrap();
        let ref lid = req.extensions.get::<Router>().unwrap().find("logid").unwrap();

        let username = "username";
        let password = "password";

        let document = Handler::get(SocketAddr::V4(context.node_addr),
                                    username,
                                    password,
                                    Uuid::parse_str(*fileId).unwrap(),
                                    LogId::from(lid.to_string()).unwrap())
            .unwrap();

        let http_doc = http_Response {
            version: document.version,
            payload: document.payload.as_slice().to_base64(STANDARD),
        };

        let encoded = json::encode(&http_doc).unwrap();

        Ok(Response::with((status::Ok, encoded)))
    }

    fn http_post(req: &mut Request, context: &Context) -> IronResult<Response> {

        let ref lid = req.extensions.get::<Router>().unwrap().find("logid").unwrap();
        let ref payload = req.extensions.get::<Router>().unwrap().find("payload").unwrap();

        let username = "username";
        let password = "password";

        let bytes = payload.from_base64().expect("Payload is not base64");

        let id = Uuid::new_v4();

        let document = Document {
            id: id,
            payload: bytes,
            version: 1,
        };

        let session = Uuid::new_v4();

        match Handler::post(SocketAddr::V4(context.node_addr),
                            username,
                            password,
                            document,
                            session,
                            LogId::from(lid.to_string()).unwrap()) {
            Ok(id) => Ok(Response::with((status::Ok, format!("{}", id)))),
            Err(err) => {
                Ok(Response::with((status::InternalServerError,
                                   "An error occured when posting new document")))
            }
        }
    }

    fn http_delete(req: &mut Request, context: &Context) -> IronResult<Response> {
        let ref fileId = req.extensions
            .get::<Router>()
            .unwrap()
            .find("fileId")
            .unwrap();

        let ref lid = req.extensions.get::<Router>().unwrap().find("logid").unwrap();

        let username = "username";
        let password = "password";

        let session = Uuid::new_v4();

        let res = match Handler::remove(SocketAddr::V4(context.node_addr),
                                        username,
                                        password,
                                        Uuid::parse_str(*fileId).unwrap(),
                                        session,
                                        LogId::from(lid.to_string()).unwrap()) {
            Ok(()) => Response::with((status::Ok, "Ok")),
            Err(err) => {
                Response::with((status::InternalServerError,
                                "An error occured when removing document"))
            }
        };

        Ok(res)
    }

    fn http_put(req: &mut Request, context: &Context) -> IronResult<Response> {
        let ref id = req.extensions.get::<Router>().unwrap().find("id").unwrap();
        let ref lid = req.extensions.get::<Router>().unwrap().find("logid").unwrap();
        let ref payload = req.extensions.get::<Router>().unwrap().find("payload").unwrap();

        let username = "username";
        let password = "password";

        let bytes = payload.from_base64().expect("Payload is not base64");

        let res = match Handler::put(SocketAddr::V4(context.node_addr),
                                     username,
                                     password,
                                     Uuid::parse_str(&id).unwrap(),
                                     bytes,
                                     Uuid::new_v4(),
                                     LogId::from(lid.to_string()).unwrap()) {
            Ok(()) => Response::with((status::Ok, "Ok")),
            Err(err) => {
                Response::with((status::InternalServerError,
                                "An error occured when updating document"))
            }
        };
        Ok(res)

    }

    fn http_begin_transaction(req: &mut Request, context: &Context) -> IronResult<Response> {
        let username = "username";
        let password = "password";
        let ref lid = req.extensions.get::<Router>().unwrap().find("logid").unwrap();

        match Handler::begin_transaction(SocketAddr::V4(context.node_addr),
                                         username,
                                         password,
                                         Uuid::new_v4(),
                                         LogId::from(lid.to_string()).unwrap()) {
            Ok(session) => Ok(Response::with((status::Ok, session))),
            Err(_) => Ok(Response::with((status::InternalServerError, "Something went wrong :("))),
        }
    }

    fn http_commit_transaction(req: &mut Request, context: &Context) -> IronResult<Response> {
        let username = "username";
        let password = "password";
        let ref lid = req.extensions.get::<Router>().unwrap().find("logid").unwrap();

        match Handler::commit_transaction(SocketAddr::V4(context.node_addr),
                                          username,
                                          password,
                                          LogId::from(lid.to_string()).unwrap()) {
            Ok(res) => Ok(Response::with((status::Ok, res))),
            Err(_) => Ok(Response::with((status::InternalServerError, "Something went wrong :("))),
        }
    }

    fn http_rollback_transaction(req: &mut Request, context: &Context) -> IronResult<Response> {
        let username = "username";
        let password = "password";
        let ref lid = req.extensions.get::<Router>().unwrap().find("logid").unwrap();

        match Handler::rollback_transaction(SocketAddr::V4(context.node_addr),
                                            username,
                                            password,
                                            LogId::from(lid.to_string()).unwrap()) {
            Ok(res) => Ok(Response::with((status::Ok, res))),
            Err(_) => Ok(Response::with((status::InternalServerError, "Something went wrong :("))),
        }
    }
}
