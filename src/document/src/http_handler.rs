use iron::status;
use router::Router;
use iron::prelude::*;
use params::{Params, Value};

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

#[derive(RustcDecodable,RustcEncodable)]
struct http_Response {
    payload: String,
}

#[derive(Clone,Copy)]
struct Context {
    node_addr: SocketAddrV4,
}

pub fn init(binding_addr: SocketAddr, node_addr: SocketAddrV4) {
    let mut router = Router::new();

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

        let username = "username";
        let password = "password";

        let document = Handler::get(SocketAddr::V4(context.node_addr),
                                    username,
                                    password,
                                    Uuid::parse_str(*fileId).unwrap())
            .unwrap();

        let http_doc = http_Response { payload: document.payload.as_slice().to_base64(STANDARD) };

        let encoded = json::encode(&http_doc).unwrap();

        Ok(Response::with((status::Ok, encoded)))
    }

    fn http_post(req: &mut Request, context: &Context) -> IronResult<Response> {

        let map = req.get_ref::<Params>().unwrap();

        let username = "username";
        let password = "password";

        match map.find(&["payload"]) {
            Some(&Value::String(ref p)) => {

                let bytes = p.from_base64().expect("Payload is not base64");

                let document = Document { payload: bytes };
                match Handler::post(SocketAddr::V4(context.node_addr),
                                    username,
                                    password,
                                    document) {
                    Ok(id) => Ok(Response::with((status::Ok, format!("{}", id)))),
                    Err(err) => {
                        Ok(Response::with((status::InternalServerError,
                                           "An error occured when posting new document")))
                    }
                }
            } 
            _ => Ok(Response::with((status::InternalServerError, "No payload defined"))), 
        }
    }

    fn http_delete(req: &mut Request, context: &Context) -> IronResult<Response> {
        let ref fileId = req.extensions
            .get::<Router>()
            .unwrap()
            .find("fileId")
            .unwrap();

        let username = "username";
        let password = "password";

        let res = match Handler::remove(SocketAddr::V4(context.node_addr),
                                        username,
                                        password,
                                        Uuid::parse_str(*fileId).unwrap()) {
            Ok(()) => Response::with((status::Ok, "Ok")),
            Err(err) => {
                Response::with((status::InternalServerError,
                                "An error occured when removing document"))
            }
        };

        Ok(res)
    }
}
