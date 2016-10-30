use Error;
use RaftError;

use capnp::message::{Builder, HeapAllocator, Reader, ReaderSegments};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use consensus::Actions;
use messages;
use uuid::Uuid;
use std::rc::Rc;
use ClientId;

#[derive(Clone)]
pub struct Transaction {
    pub isActive: bool,
    pub session: Option<Uuid>,
    pub queue: Vec<(ClientId, Rc<Builder<HeapAllocator>>)>,
    counter: usize,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            isActive: false,
            session: None,
            queue: vec![],
            counter: 0,
        }
    }

    pub fn begin(&mut self, session: Uuid) {
        scoped_debug!("TRANSACTION BEGINS");

        self.session = Some(session);
        self.isActive = true;
    }

    pub fn rollback(&mut self) {
        unimplemented!()
    }

    pub fn end(&mut self) {
        scoped_debug!("TRANSACTION FINISHED! {} messages received", self.counter);

        self.session = None;
        self.counter = 0;
        self.isActive = false;
    }

    pub fn broadcast_begin(&mut self, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION BEGINS");
        let message = messages::transaction_begin(self.session.unwrap().as_bytes());
        actions.peer_messages_broadcast.push(message);
    }

    pub fn broadcast_end(&self, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION ENDS");
        let message = messages::transaction_end();
        actions.peer_messages_broadcast.push(message);
    }

    pub fn broadcast_rollback(&self, actions: &mut Actions) {
        unimplemented!()
    }

    pub fn compare(&self, session: Uuid) -> bool {
        match self.session {
            Some(s) => if s == session { true } else { false },
            None => true,
        }
    }

    pub fn get(&self) -> bool {
        self.isActive
    }

    pub fn get_counter(&self) -> usize {
        self.counter
    }

    pub fn count_up(&mut self) {
        self.counter += 1;
    }
}
