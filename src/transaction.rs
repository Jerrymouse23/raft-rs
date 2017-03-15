use capnp::message::{Builder, HeapAllocator};
use consensus::Actions;
use messages;
use uuid::Uuid;
use std::rc::Rc;
use LogIndex;
use LogId;
use ClientId;
use TransactionId;

#[derive(Clone)]
pub struct Transaction {
    pub is_active: bool,
    pub session: Option<TransactionId>,
    pub queue: Vec<(ClientId, Rc<Builder<HeapAllocator>>)>,
    counter: usize,
    commit_index: LogIndex,
    last_applied: LogIndex,
    follower_state_min: Option<LogIndex>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            is_active: false,
            session: None,
            queue: vec![],
            counter: 0,
            commit_index: LogIndex::from(0),
            last_applied: LogIndex::from(0),
            follower_state_min: None,
        }
    }

    pub fn begin(&mut self,
                 session: TransactionId,
                 commit_index: LogIndex,
                 last_applied: LogIndex,
                 follower_state_min: Option<LogIndex>) {
        scoped_debug!("TRANSACTION BEGINS");

        self.session = Some(session);
        self.is_active = true;
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
        self.is_active = false;
        self.commit_index = LogIndex::from(0);
        self.last_applied = LogIndex::from(0);
        self.follower_state_min = None;
    }

    pub fn broadcast_begin(&mut self, lid: &LogId, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION BEGINS");
        let message = messages::transaction_begin(lid,
                                                  &self.session
                                                      .expect("Cannot start transaction when no \
                                                               TransactionId has been set"));
        actions.peer_messages_broadcast.push(message);
    }

    pub fn broadcast_end(&self, lid: &LogId, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION ENDS");
        let message = messages::transaction_commit(lid,
                                                   &self.session
                                                       .expect("Cannot end transaction when \
                                                                no TransactionId has been set"));
        actions.peer_messages_broadcast.push(message);
    }

    pub fn broadcast_rollback(&self, lid: &LogId, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION ROLLBACK");
        let message = messages::transaction_rollback(lid,
                                                     &self.session
                                                         .expect("Cannot rollback transaction \
                                                                  when no TransactionId has \
                                                                  been set"));
        actions.peer_messages_broadcast.push(message);
    }

    pub fn compare(&self, session: TransactionId) -> bool {
        self.session.expect("No TransactionId has been set") == session
    }


    pub fn get(&self) -> bool {
        self.is_active
    }

    pub fn get_counter(&self) -> usize {
        self.counter
    }

    pub fn count_up(&mut self) {
        self.counter += 1;
    }
}
