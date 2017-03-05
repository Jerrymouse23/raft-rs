use state_machine::StateMachine;

/// A state machine with no states.
#[derive(Debug,Clone)]
pub struct NullStateMachine;

impl StateMachine for NullStateMachine {
    fn apply(&mut self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }

    fn query(&self, _query: &[u8]) -> Vec<u8> {
        Vec::new()
    }

    fn snapshot(&self) -> (Vec<u8>,Vec<u8>) {
        (Vec::new(),Vec::new())
    }

    fn restore_snapshot(&mut self, _snap_map: Vec<u8>,_snap_log: Vec<u8>) {
        ()
    }

    fn revert(&mut self, _command: &[u8]) -> () {
        unimplemented!()
    }

    fn rollback(&mut self) {}
}
