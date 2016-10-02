use iron::status;
use router::Router;
use iron::prelude::*;
use params::{Params, Value};

use uuid::Uuid;
use std::net::{SocketAddr, ToSocketAddrs, SocketAddrV4, Ipv4Addr};

use std::error::Error;

use rustc_serialize::json;

use Document;

use handler;
use handler::Handler;

use std::thread::spawn;

#[derive(RustcDecodable,RustcEncodable)]
struct http_Response {
    payload: String,
}

pub fn init(rest_port: u16) {
    let mut router = Router::new();
    router.get("/document/:fileId", http_get, "get_document");
    router.post("/document/", http_post, "post_document");
    router.delete("/document/:fileId", http_delete, "delete_document");

    spawn(move || {
        Iron::new(router).http(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), rest_port));
    });

    fn http_get(req: &mut Request) -> IronResult<Response> {
        let ref fileId = req.extensions
            .get::<Router>()
            .unwrap()
            .find("fileId")
            .unwrap();

        let document = Handler::get(Uuid::parse_str(*fileId).unwrap()).unwrap();

        let http_doc = http_Response { payload: String::from_utf8(document.payload).unwrap() };

        let encoded = json::encode(&http_doc).unwrap();

        Ok(Response::with((status::Ok, encoded)))
    }

    fn http_post(req: &mut Request) -> IronResult<Response> {

        let map = req.get_ref::<Params>().unwrap();

        match map.find(&["payload"]) {
            Some(&Value::String(ref p)) => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(p.as_bytes());

                let document = Document { payload: bytes };
                match Handler::put(document) {
                    Ok(id) => Ok(Response::with((status::Ok, format!("{}", id)))),
                    Err(err) => {
                        Ok(Response::with((status::InternalServerError, err.description())))
                    }
                }
            } 
            _ => Ok(Response::with((status::InternalServerError, "No payload defined"))), 
        }
    }

    fn http_delete(req: &mut Request) -> IronResult<Response> {
        let ref fileId = req.extensions
            .get::<Router>()
            .unwrap()
            .find("fileId")
            .unwrap();

        let res = match Handler::remove(Uuid::parse_str(*fileId).unwrap()) {
            Ok(_) => Response::with((status::Ok, "Ok")),
            Err(err) => Response::with((status::InternalServerError, err.description())),
        };

        Ok(res)
    }
}
