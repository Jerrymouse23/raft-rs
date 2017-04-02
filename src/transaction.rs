use std::fmt;

use capnp::message::{Builder, HeapAllocator};
use consensus::Actions;
use messages;
use std::rc::Rc;
use LogIndex;
use LogId;
use ClientId;
use TransactionId;

#[derive(Debug,Clone)]
pub enum TransactionError {
    NotActive,
    AlreadyActive,
    Other(String),
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TransactionError::NotActive => fmt::Display::fmt("No transaction is active", f),
            TransactionError::AlreadyActive => {
                fmt::Display::fmt("A transaction is already active", f)
            }
            TransactionError::Other(ref error) => fmt::Display::fmt(error, f),
        }
    }
}

#[derive(Clone)]
pub struct TransactionManager {
    /// Whether a transaction is running
    pub is_active: bool,
    /// The ID of the current transaction. If `None`, no transaction is running
    pub session: Option<TransactionId>,
    /// The amount of the messages which has been applied during transaction
    counter: usize,
    /// The commit_index before the transaction
    commit_index: LogIndex,
    /// The last_applied index before the transaction
    last_applied: LogIndex,
    /// The follower_state_min index before the transaction
    follower_state_min: Option<LogIndex>,
}

impl TransactionManager {
    /// Creates new TransactionManager
    pub fn new() -> Self {
        TransactionManager {
            is_active: false,
            session: None,
            counter: 0,
            commit_index: LogIndex::from(0),
            last_applied: LogIndex::from(0),
            follower_state_min: None,
        }
    }

    /// Begins new transaction
    ///
    /// # Arguments
    /// * `session` - The ID of the transaction
    /// * `commit_index` - The commit_index before the transaction
    /// * `last_applied` - The last_applied index before the transaction
    /// * `follower_state_min` - The follower_state_min index before the transaction
    pub fn begin(&mut self,
                 session: TransactionId,
                 commit_index: LogIndex,
                 last_applied: LogIndex,
                 follower_state_min: Option<LogIndex>)
                 -> Result<(), TransactionError> {
        assert!(!self.is_active);

        if !self.is_active {
            scoped_debug!("TRANSACTION BEGINS");

            self.session = Some(session);
            self.is_active = true;
            self.commit_index = commit_index;
            self.last_applied = last_applied;
            self.follower_state_min = follower_state_min;
            Ok(())
        } else {
            Err(TransactionError::AlreadyActive)
        }
    }

    /// Reverts all messages which has been applied during transaction
    pub fn rollback(&mut self) -> Result<(LogIndex, LogIndex, Option<LogIndex>), TransactionError> {
        if self.is_active {
            let commit_index = self.commit_index;
            let last_applied = self.last_applied;
            let follower_state_min = self.follower_state_min;

            try!(self.end());

            Ok((commit_index, last_applied, follower_state_min))
        } else {
            Err(TransactionError::NotActive)
        }
    }

    /// Resets all values of the TransactionManager for the next transaction
    pub fn end(&mut self) -> Result<(), TransactionError> {
        if self.is_active {
            scoped_debug!("TRANSACTION FINISHED! {} messages received", self.counter);

            self.session = None;
            self.counter = 0;
            self.is_active = false;
            self.commit_index = LogIndex::from(0);
            self.last_applied = LogIndex::from(0);
            self.follower_state_min = None;

            Ok(())
        } else {
            Err(TransactionError::NotActive)
        }
    }

    /// Sends to all peers a message that a transaction has been started
    pub fn broadcast_begin(&mut self, lid: LogId, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION BEGINS");
        let message = messages::transaction_begin(lid,
                                                  self.session
                                                      .expect("Cannot start transaction when no \
                                                               TransactionId has been set"));
        actions.peer_messages_broadcast.push(message);
    }

    /// Sends to all peers a message that the currently running transaction should be commited
    pub fn broadcast_end(&self, lid: LogId, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION ENDS");
        let message = messages::transaction_commit(lid,
                                                   self.session
                                                       .expect("Cannot end transaction when \
                                                                no TransactionId has been set"));
        actions.peer_messages_broadcast.push(message);
    }

    /// Sends to all peers a messages that the all messages which has been applied during the
    /// transaction should be reverted
    pub fn broadcast_rollback(&self, lid: LogId, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION ROLLBACK");
        let message = messages::transaction_rollback(lid,
                                                     self.session
                                                         .expect("Cannot rollback transaction \
                                                                  when no TransactionId has \
                                                                  been set"));
        actions.peer_messages_broadcast.push(message);
    }

    /// Compares the current TransactionId with the given one. If `true`, it is the same
    ///
    /// # Arguments
    /// * `session` - The TransactionId which will be compared with
    pub fn compare(&self, session: TransactionId) -> bool {
        self.session.expect("No TransactionId has been set") == session
    }

    /// Returns whether the a transaction is running
    pub fn get(&self) -> bool {
        self.is_active
    }

    /// Returns how many messages has been applied
    pub fn get_counter(&self) -> usize {
        self.counter
    }

    /// Counts up the message counter
    pub fn count_up(&mut self) {
        self.counter += 1;
    }
}
