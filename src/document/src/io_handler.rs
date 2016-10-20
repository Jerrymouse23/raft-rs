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

pub struct ioHandler;

impl ioHandler {
    /// Decodes document from file by given id
    /// # Arguments
    ///
    /// * `id` - The uuid of the document in order to find the document
    /// * `volume` - The folder where the documents are saved
    pub fn get(id: Uuid, volume: &str) -> Result<Document, DecodingError> {
        let mut handler = try!(File::open(format!("{}/{}", volume, id)));

        let mut decoded: Document = try!(decode_from(&mut handler, SizeLimit::Infinite));

        Ok(decoded)
    }

    /// Encodes a document and writes it into a file
    /// # Arguments
    ///
    /// * `document` - The document which will be encoded
    /// * `volume` - The folder where the documents are saved
    pub fn post(document: Document, volume: &str) -> Result<Uuid, EncodingError> {
        let id = Uuid::new_v4();

        // TODO implement error-handling
        let mut handler = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(format!("{}/{}", volume, id))
            .unwrap();

        try!(encode_into(&document, &mut handler, SizeLimit::Infinite));

        Ok(id)
    }

    /// Deletes a document
    /// # Arguments
    ///
    /// * `id` - The uuid of the document in order to find the document
    /// * `volume` - The folder where the documents are saved
    pub fn remove(id: Uuid, volume: &str) -> Result<String, IoError> {
        try!(remove_file(format!("{}/{}", volume, id)));
        Ok("Document deleted".to_string())
    }
}
