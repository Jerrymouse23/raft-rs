use consensus::Actions;
use messages;
use LogIndex;
use LogId;
use TransactionId;

use transaction::Transaction;
use transaction::TransactionError;
use transaction::snapshot::Snapshot;

use std::collections::VecDeque;

pub struct TransactionManager {
    transactions: VecDeque<Transaction>,
}

impl TransactionManager {
    /// Creates new TransactionManager
    pub fn new() -> Self {
        TransactionManager { transactions: VecDeque::new() }
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
        scoped_debug!("TRANSACTION BEGINS");

        let snapshot = Snapshot {
            commit_index,
            last_applied,
            follower_state_min,
        };

        let transaction = Transaction::new(session, snapshot);

        self.transactions.push_back(transaction);

        Ok(())
    }

    /// Reverts all messages which has been applied during transaction
    pub fn rollback(&mut self) -> Result<(LogIndex, LogIndex, Option<LogIndex>), TransactionError> {
        if self.is_active() {
            let snapshot = self.transactions
                .back()
                .ok_or(TransactionError::NotActive)?
                .get_snapshot();

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
        if self.is_active() {
            scoped_debug!("TRANSACTION FINISHED! {} messages received",
                          self.get_counter());

            self.transactions.pop_back();

            Ok(())
        } else {
            Err(TransactionError::NotActive)
        }
    }

    /// Sends to all peers a message that a transaction has been started
    pub fn broadcast_begin(&mut self, lid: LogId, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION BEGINS");
        let message = messages::transaction_begin(lid,
                                                  self.get_current_session()
                                                      .expect("Cannot start transaction when no \
                                                               TransactionId has been set"));
        actions.peer_messages_broadcast.push(message);
    }

    /// Sends to all peers a message that the currently running transaction should be commited
    pub fn broadcast_end(&self, lid: LogId, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION ENDS");
        let message = messages::transaction_commit(lid,
                                                   self.get_current_session()
                                                       .expect("Cannot end transaction when \
                                                                no TransactionId has been set"));
        actions.peer_messages_broadcast.push(message);
    }

    /// Sends to all peers a messages that the all messages which has been applied during the
    /// transaction should be reverted
    pub fn broadcast_rollback(&self, lid: LogId, actions: &mut Actions) {
        scoped_debug!("BROADCAST TRANSACTION ROLLBACK");
        let message = messages::transaction_rollback(lid,
                                                     self.get_current_session()
                                                         .expect("Cannot rollback transaction \
                                                                  when no TransactionId has \
                                                                  been set"));
        actions.peer_messages_broadcast.push(message);
    }

    pub fn get_current_session(&self) -> Option<TransactionId> {
        self.transactions.back().map(|x| x.get_session())
    }

    /// Compares the current TransactionId with the given one. If `true`, it is the same
    ///
    /// # Arguments
    /// * `session` - The TransactionId which will be compared with
    pub fn compare(&self, session: TransactionId) -> bool {
        match self.transactions.back() {
            Some(t) => t.compare_session(session),
            None => true,
        }
    }

    /// Returns how many messages has been applied
    pub fn get_counter(&self) -> usize {
        match self.transactions.back() {
            Some(t) => t.get_counter(),
            None => 0,
        }
    }

    /// Counts up the message counter
    pub fn count_up(&mut self) -> Result<(), TransactionError> {
        self.transactions
            .back_mut()
            .ok_or(TransactionError::NotActive)?
            .count_up();

        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.transactions.back().is_some()
    }
}
