use Error;
use RaftError;

use capnp::message::{Builder, HeapAllocator, Reader, ReaderSegments};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use consensus::Actions;
use messages;
use uuid::Uuid;
use std::rc::Rc;
use LogIndex;
use ClientId;

#[derive(Clone)]
pub struct Transaction {
    pub isActive: bool,
    pub session: Option<Uuid>,
    pub queue: Vec<(ClientId, Rc<Builder<HeapAllocator>>)>,
    counter: usize,
    commit_index: LogIndex,
    last_applied: LogIndex,
    follower_state_min: Option<LogIndex>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            isActive: false,
            session: None,
            queue: vec![],
            counter: 0,
            commit_index: LogIndex::from(0),
            last_applied: LogIndex::from(0),
            follower_state_min: None,
        }
    }

    pub fn begin(&mut self,
                 session: Uuid,
                 commit_index: LogIndex,
                 last_applied: LogIndex,
                 follower_state_min: Option<LogIndex>) {
        scoped_debug!("TRANSACTION BEGINS");

        self.session = Some(session);
        self.isActive = true;
        self.commit_index = commit_index;
        self.last_applied = last_applied;
        self.follower_state_min = follower_state_min;
    }

    pub fn rollback(&mut self) -> (LogIndex, LogIndex, Option<LogIndex>) {
        let commit_index = self.commit_index;
        let last_applied = self.last_applied;
        let follower_state_min = self.follower_state_min;

        self.end();
        (commit_index, last_applied, follower_state_min)
    }

    pub fn end(&mut self) {
        scoped_debug!("TRANSACTION FINISHED! {} messages received", self.counter);

        self.session = None;
        self.counter = 0;
        self.isActive = false;
        self.commit_index = LogIndex::from(0);
        self.last_applied = LogIndex::from(0);
        self.follower_state_min = None;
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
        scoped_debug!("BROADCAST TRANSACTION ROLLBACK");
        let message = messages::transaction_rollback();
        actions.peer_messages_broadcast.push(message);
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
