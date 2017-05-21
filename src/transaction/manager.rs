use consensus::Actions;
use messages;
use LogIndex;
use LogId;
use TransactionId;

use transaction::TransactionError;
use transaction::snapshot::Snapshot;

#[derive(Clone)]
pub struct TransactionManager {
    /// Whether a transaction is running
    pub is_active: bool,
    /// The ID of the current transaction. If `None`, no transaction is running
    pub session: Option<TransactionId>,
    /// The amount of the messages which has been applied during transaction
    counter: usize,
    snapshot: Option<Snapshot>,
}

impl TransactionManager {
    /// Creates new TransactionManager
    pub fn new() -> Self {
        TransactionManager {
            is_active: false,
            session: None,
            counter: 0,
            snapshot: None,
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

            let snapshot = Snapshot {
                commit_index,
                last_applied,
                follower_state_min,
            };

            self.snapshot = Some(snapshot);

            Ok(())
        } else {
            Err(TransactionError::AlreadyActive)
        }
    }

    /// Reverts all messages which has been applied during transaction
    pub fn rollback(&mut self) -> Result<(LogIndex, LogIndex, Option<LogIndex>), TransactionError> {
        let snapshot = self.snapshot.clone();

        if self.is_active {
            let snapshot = snapshot
                .ok_or(TransactionError::Other("Snapshot is None".to_owned()))?;

            let commit_index = snapshot.commit_index;
            let last_applied = snapshot.last_applied;
            let follower_state_min = snapshot.follower_state_min;

            try!(self.commit());

            Ok((commit_index, last_applied, follower_state_min))
        } else {
            Err(TransactionError::NotActive)
        }
    }

    /// Resets all values of the TransactionManager for the next transaction
    pub fn commit(&mut self) -> Result<(), TransactionError> {
        if self.is_active {
            scoped_debug!("TRANSACTION FINISHED! {} messages received", self.counter);

            self.session = None;
            self.counter = 0;
            self.is_active = false;
            self.snapshot = None;

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
