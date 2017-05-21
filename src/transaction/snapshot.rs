use Term;
use LogIndex;

#[derive(Clone)]
pub struct Snapshot {
    /// The commit_index before the transaction
    pub commit_index: LogIndex,
    /// The last_applied index before the transaction
    pub last_applied: LogIndex,
    /// The follower_state_min index before the transaction
    pub follower_state_min: Option<LogIndex>,
}
