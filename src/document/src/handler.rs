use uuid::Uuid;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::remove_file;
use std::io::Read;
use std::io::Write;

use bincode::rustc_serialize::{encode, decode, encode_into, decode_from};
use bincode::SizeLimit;
use rustc_serialize::json;
use bincode::rustc_serialize::{EncodingError, DecodingError};

use std::io::Error as IoError;

use document::Document;

pub struct Handler;

impl Handler {
    pub fn get(id: Uuid) -> Result<Document, DecodingError> {
        let mut handler = try!(File::open(format!("data/{}", id)));

        let mut decoded: Document = try!(decode_from(&mut handler, SizeLimit::Infinite));

        Ok(decoded)
    }

    pub fn put(document: Document) -> Result<Uuid, EncodingError> {
        let id = Uuid::new_v4();

        // TODO implement error-handling
        let mut handler = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(format!("data/{}", id))
            .unwrap();

        try!(encode_into(&document, &mut handler, SizeLimit::Infinite));

        Ok(id)
    }

    pub fn remove(id: Uuid) -> Result<String, IoError> {
        try!(remove_file(format!("data/{}", id)));
        Ok("Document deleted".to_string())
    }
}
