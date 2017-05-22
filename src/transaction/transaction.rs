use transaction::snapshot::Snapshot;
use TransactionId;

pub struct Transaction {
    snapshot: Snapshot,
    /// The ID of the current transaction. If `None`, no transaction is running
    session: TransactionId,
    /// The amount of the messages which has been applied during transaction
    counter: usize,
}

impl Transaction {
    pub fn new(session: TransactionId, snapshot: Snapshot) -> Self {
        Transaction {
            snapshot,
            session,
            counter: 0,
        }
    }

    pub fn get_counter(&self) -> usize {
        self.counter
    }

    pub fn count_up(&mut self) {
        self.counter += 1;
    }

    pub fn compare_session(&self, session: TransactionId) -> bool {
        self.session == session
    }

    pub fn get_session(&self) -> TransactionId {
        self.session
    }

    pub fn get_snapshot(&self) -> Snapshot {
        self.snapshot.clone()
    }
}
