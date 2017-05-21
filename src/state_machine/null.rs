use state_machine::StateMachine;
use state_machine::StateMachineError;

/// A state machine with no states.
#[derive(Debug,Clone)]
pub struct NullStateMachine;

impl StateMachine for NullStateMachine {
    fn apply(&mut self, _command: &[u8]) -> Result<Vec<u8>, StateMachineError> {
        Ok(Vec::new())
    }

    fn query(&self, _query: &[u8]) -> Result<Vec<u8>, StateMachineError> {
        Ok(Vec::new())
    }

    fn snapshot(&self) -> Result<Vec<u8>, StateMachineError> {
        Ok(Vec::new())
    }

    fn restore_snapshot(&mut self, _snap_map: Vec<u8>) -> Result<(), StateMachineError> {
        Ok(())
    }

    fn revert(&mut self, _command: &[u8]) -> Result<(), StateMachineError> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), StateMachineError> {
        Ok(())
    }
}
