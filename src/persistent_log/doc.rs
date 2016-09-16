use std::{error, fmt, result};
use std::fs::File;
use std::io::prelude::*;
use bincode::SizeLimit;
use bincode::rustc_serialize::{encode_into, encode, decode, decode_from};
use std::fs::OpenOptions;

use persistent_log::Log;
use LogIndex;
use ServerId;
use Term;

#[derive(Clone, Debug)]
pub struct DocLog {
    current_term: Term,
    voted_for: Option<ServerId>,
    entries: Vec<(Term, Vec<u8>)>,
}

/// Non-instantiable error type for MemLog
pub enum Error { }

impl fmt::Display for Error {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        unreachable!()
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        unreachable!()
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        unreachable!()
    }
}

impl DocLog {
    pub fn new() -> Self {
        let mut d = DocLog {
            current_term: Term(0),
            voted_for: None,
            entries: Vec::new(),
        };

        d.set_current_term(Term(0));

        d
    }

    pub fn sync_voted_for(&mut self) -> result::Result<ServerId, Error> {
        let mut voted_for_handler = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("voted_for")
            .unwrap();

        let voted_for_decode: ServerId = (decode_from(&mut voted_for_handler, SizeLimit::Infinite))
            .unwrap();
        self.voted_for = Some(voted_for_decode);

        Ok(voted_for_decode)
    }

    pub fn sync_term(&mut self) -> result::Result<Term, Error> {
        let mut term_handler = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("term")
            .unwrap();

        let term: Term = (decode_from(&mut term_handler, SizeLimit::Infinite)).unwrap();

        self.current_term = term;

        Ok(term)
    }
}

impl Log for DocLog {
    type Error = Error;

    fn current_term(&self) -> result::Result<Term, Error> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, term: Term) -> result::Result<(), Error> {
        let mut term_handler = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("term")
            .unwrap();

        encode_into(&term, &mut term_handler, SizeLimit::Infinite);

        self.voted_for = None;
        Ok(self.current_term = term)
    }

    fn inc_current_term(&mut self) -> result::Result<Term, Error> {
        self.voted_for = None;
        let new_term = self.sync_term().unwrap() + 1;
        self.set_current_term(new_term);
        println!("{}", new_term);
        // self.set_current_term(self.current_term().unwrap() + 1).unwrap();
        self.current_term()
    }

    fn voted_for(&self) -> result::Result<Option<ServerId>, Error> {
        Ok(self.voted_for)
    }

    fn set_voted_for(&mut self, address: ServerId) -> result::Result<(), Error> {
        let mut voted_for_handler = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("voted_for")
            .unwrap();

        encode_into(&address, &mut voted_for_handler, SizeLimit::Infinite);

        Ok(self.voted_for = Some(address))
    }

    fn latest_log_index(&self) -> result::Result<LogIndex, Error> {
        Ok(LogIndex(self.entries.len() as u64))
    }

    fn latest_log_term(&self) -> result::Result<Term, Error> {
        let len = self.entries.len();
        if len == 0 {
            Ok(Term::from(0))
        } else {
            Ok(self.entries[len - 1].0)
        }
    }

    fn entry(&self, index: LogIndex) -> result::Result<(Term, &[u8]), Error> {
        let (term, ref bytes) = self.entries[(index - 1).as_u64() as usize];
        Ok((term, bytes))
    }

    fn append_entries(&mut self,
                      from: LogIndex,
                      entries: &[(Term, &[u8])])
                      -> result::Result<(), Error> {
        assert!(self.latest_log_index().unwrap() + 1 >= from);
        self.entries.truncate((from - 1).as_u64() as usize);
        Ok(self.entries.extend(entries.iter().map(|&(term, command)| (term, command.to_vec()))))
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use LogIndex;
    use ServerId;
    use Term;
    use persistent_log::Log;
    use std::fs::File;
    use bincode::SizeLimit;
    use bincode::rustc_serialize::{encode_into, encode, decode, decode_from};
    use std::io::prelude::*;
    use std::fs::OpenOptions;
    use std::io::SeekFrom;

    #[test]
    fn test_sync_term() {
        let mut f = OpenOptions::new().read(true).write(true).create(true).open("term").unwrap();

        let term = Term(10);

        encode_into(&term, &mut f, SizeLimit::Infinite).unwrap();

        f.seek(SeekFrom::Start(0));

        let decoded_term: Term = decode_from(&mut f, SizeLimit::Infinite).unwrap();

        println!("{}", decoded_term);

        assert_eq!(decoded_term, term);
    }

    #[test]
    fn test_sync_voted_for() {
        let mut f =
            OpenOptions::new().read(true).write(true).create(true).open("voted_for").unwrap();

        let voted_for = ServerId(5);

        encode_into(&voted_for, &mut f, SizeLimit::Infinite).unwrap();

        f.seek(SeekFrom::Start(0));

        let decoded_voted_for: ServerId = decode_from(&mut f, SizeLimit::Infinite).unwrap();

        assert_eq!(decoded_voted_for, voted_for);
    }

    #[test]
    fn test_current_term() {
        let mut store = DocLog::new();
        assert_eq!(Term(0), store.current_term().unwrap());
        store.set_voted_for(ServerId::from(0)).unwrap();
        store.set_current_term(Term(42)).unwrap();
        let sync = store.sync_term().unwrap();
        assert_eq!(Term(42), sync);
        assert_eq!(None, store.voted_for().unwrap());
        assert_eq!(Term(42), store.current_term().unwrap());
        store.inc_current_term().unwrap();
        assert_eq!(Term(43), store.current_term().unwrap());
        store.current_term = Term(44);
        let sync = store.sync_term().unwrap();
        assert_eq!(sync, Term(43));
    }

    #[test]
    fn test_voted_for() {
        let mut store = DocLog::new();

        assert_eq!(None, store.voted_for().unwrap());
        let id = ServerId::from(0);
        store.set_voted_for(id).unwrap();
        assert_eq!(Some(id), store.voted_for().unwrap());
        // sync
        store.set_voted_for(ServerId(5)).unwrap();
        let sync = store.sync_voted_for().unwrap();
        assert_eq!(ServerId(5), sync);
        store.voted_for = Some(ServerId(6));
        let sync = store.sync_voted_for().unwrap();
        assert_eq!(ServerId(5), sync);

    }

    #[test]
    fn test_append_entries() {
        let mut store = DocLog::new();
        assert_eq!(LogIndex::from(0), store.latest_log_index().unwrap());
        assert_eq!(Term::from(0), store.latest_log_term().unwrap());

        // [0.1, 0.2, 0.3, 1.4]
        store.append_entries(LogIndex(1),
                            &[(Term::from(0), &[1]),
                              (Term::from(0), &[2]),
                              (Term::from(0), &[3]),
                              (Term::from(1), &[4])])
            .unwrap();
        assert_eq!(LogIndex::from(4), store.latest_log_index().unwrap());
        assert_eq!(Term::from(1), store.latest_log_term().unwrap());
        assert_eq!((Term::from(0), &*vec![1u8]),
                   store.entry(LogIndex::from(1)).unwrap());
        assert_eq!((Term::from(0), &*vec![2u8]),
                   store.entry(LogIndex::from(2)).unwrap());
        assert_eq!((Term::from(0), &*vec![3u8]),
                   store.entry(LogIndex::from(3)).unwrap());
        assert_eq!((Term::from(1), &*vec![4u8]),
                   store.entry(LogIndex::from(4)).unwrap());

        // [0.1, 0.2, 0.3]
        store.append_entries(LogIndex::from(4), &[]).unwrap();
        assert_eq!(LogIndex(3), store.latest_log_index().unwrap());
        assert_eq!(Term::from(0), store.latest_log_term().unwrap());
        assert_eq!((Term::from(0), &*vec![1u8]),
                   store.entry(LogIndex::from(1)).unwrap());
        assert_eq!((Term::from(0), &*vec![2u8]),
                   store.entry(LogIndex::from(2)).unwrap());
        assert_eq!((Term::from(0), &*vec![3u8]),
                   store.entry(LogIndex::from(3)).unwrap());

        // [0.1, 0.2, 2.3, 3.4]
        store.append_entries(LogIndex::from(3), &[(Term(2), &[3]), (Term(3), &[4])]).unwrap();
        assert_eq!(LogIndex(4), store.latest_log_index().unwrap());
        assert_eq!(Term::from(3), store.latest_log_term().unwrap());
        assert_eq!((Term::from(0), &*vec![1u8]),
                   store.entry(LogIndex::from(1)).unwrap());
        assert_eq!((Term::from(0), &*vec![2u8]),
                   store.entry(LogIndex::from(2)).unwrap());
        assert_eq!((Term::from(2), &*vec![3u8]),
                   store.entry(LogIndex::from(3)).unwrap());
        assert_eq!((Term::from(3), &*vec![4u8]),
                   store.entry(LogIndex::from(4)).unwrap());
    }
}
