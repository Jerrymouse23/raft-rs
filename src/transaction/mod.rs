use std::fmt;

pub mod manager;
mod snapshot;

pub use transaction::manager::TransactionManager;

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
