//! Utility functions for working with Cap'n Proto Raft messages.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::rc::Rc;
use std::collections::HashMap;

use capnp::message::{Builder, HeapAllocator};

use {ClientId, Term, LogIndex, ServerId, LogId, TransactionId};
use messages_capnp::{client_request, client_response, connection_preamble, message};
use transaction;

// ConnectionPreamble

pub fn server_connection_preamble(id: ServerId,
                                  addr: &SocketAddr,
                                  community_string: &str,
                                  peers: &HashMap<ServerId, SocketAddr>)
                                  -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut server = message
            .init_root::<connection_preamble::Builder>()
            .init_id()
            .init_server();
        server.set_addr(&format!("{}", addr));
        server.set_id(id.as_u64());
        server.set_community(community_string);

        let mut entry_list = server.init_peers(peers.len() as u32);
        for (n, entry) in peers.iter().enumerate() {
            let mut slot = entry_list.borrow().get(n as u32);
            slot.set_id(entry.0.as_u64());
            slot.set_addr(&format!("{}", entry.1));
        }
    }
    Rc::new(message)
}

pub fn client_connection_preamble(id: ClientId,
                                  username: &str,
                                  password: &str)
                                  -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut client = message
            .init_root::<connection_preamble::Builder>()
            .init_id()
            .init_client();
        client.set_username(username);
        client.set_password(password);
        client.set_id(id.as_bytes());
    }
    Rc::new(message)
}

pub fn server_add(id: ServerId,
                  community_string: &str,
                  addr: &SocketAddr)
                  -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut message = message
            .init_root::<connection_preamble::Builder>()
            .init_id()
            .init_server_add();
        message.set_id(id.as_u64());
        message.set_community(community_string);
        message.set_addr(&format!("{}", addr));
    }
    Rc::new(message)
}

// AppendEntries

pub fn append_entries_request(term: Term,
                              // prev_log_index: LogIndex,
                              // prev_log_term: Term,
                              entries: &[&[u8]],
                              leader_commit: LogIndex,
                              backup_pre_req_sigs: &[(ServerId, &[u8])],
                              backup_req_sigs: &[(ServerId, &[u8])],
							  lid: &LogId)
                              -> Rc<Builder<HeapAllocator>> {
    let bytes = &lid.as_bytes();

    assert!(bytes.len() > 0);

    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<message::Builder>();
        request.set_log_id(bytes);
        let mut request = request.init_append_entries_request();
        request.set_term(term.as_u64());
        // request.set_prev_log_index(prev_log_index.as_u64());
        // request.set_prev_log_term(prev_log_term.as_u64());
        request.set_leader_commit(leader_commit.as_u64());

        let mut entry_list = request.init_entries(entries.len() as u32);
        for (n, entry) in entries.iter().enumerate() {
            // let mut slot = entry_list.borrow().get(n as u32);
            // slot.set_term(entry.0.into());
            // slot.set_data(entry.1);
            // slot.set_data(entry);
            entry_list.set(n as u32, entry);
        }

        let mut backup_pre_req_sigs_list = request.init_backup_pre_req_sigs(backup_pre_req_sigs.len() as u32);
        for (n, backup_pre_req_sig) in backup_pre_req_sigs.iter().enumerate() {
            let mut slot = backup_pre_req_sigs_list.borrow().get(n as u32);
            slot.set_server_id(backup_pre_req_sig.0.into());
            slot.set_data(backup_pre_req_sig.1);
            // backup_pre_req_sigs_list.set(n as u32, backup_pre_req_sig);
        }

        let mut backup_req_sigs_list = request.init_backup_req_sigs(backup_req_sigs.len() as u32);
        for (n, backup_req_sig) in backup_req_sigs.iter().enumerate() {
            let mut slot = backup_req_sigs_list.borrow().get(n as u32);
            slot.set_server_id(backup_req_sig.0.into());
            slot.set_data(backup_req_sig.1);
            // backup_req_sigs_list.set(n as u32, backup_req_sig);
        }
    }
    Rc::new(message)
}

pub fn append_entries_response_success(term: Term,
                                       node_num: ServerId,
                                       log_index: LogIndex,
                                       append_sig: (ServerId, &[u8]),
                                       lid: &LogId)
                                       -> Rc<Builder<HeapAllocator>> {
    let bytes = &lid.as_bytes();

    assert!(bytes.len() > 0);

    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<message::Builder>();
        response.set_log_id(bytes);
        let mut response = response.init_append_entries_response();
        response.set_term(term.as_u64());
        response.set_node_num(node_num.into());
        // response.set_success(log_index.as_u64());
        response.set_log_index(log_index.as_u64());
        let mut sig = response.init_append_sig();
        sig.set_server_id(append_sig.0.into());
        sig.set_data(append_sig.1);
    }
    Rc::new(message)
}

/*
pub fn append_entries_response_stale_term(term: Term, lid: &LogId) -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<message::Builder>();
        response.set_log_id(&lid.as_bytes());
        let mut response = response.init_append_entries_response();
        response.set_term(term.as_u64());
        response.set_stale_term(());
    }
    Rc::new(message)
}

pub fn append_entries_response_inconsistent_prev_entry(term: Term,
                                                       index: LogIndex,
                                                       lid: &LogId)
                                                       -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<message::Builder>();
        response.set_log_id(&lid.as_bytes());
        let mut response = response.init_append_entries_response();
        response.set_term(term.as_u64());
        response.set_inconsistent_prev_entry(index.into());
    }
    Rc::new(message)
}

pub fn append_entries_response_internal_error(term: Term,
                                              error: &str,
                                              lid: &LogId)
                                              -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<message::Builder>();
        response.set_log_id(&lid.as_bytes());
        let mut response = response.init_append_entries_response();
        response.set_term(term.as_u64());
        response.set_internal_error(error);
    }
    Rc::new(message)
}
*/

// PreAppendEntries

pub fn pre_append_entries_request(term: Term,
                                  log_index: LogIndex,
                                  entries_hash: &[u8],
							      lid: &LogId)
                                  -> Rc<Builder<HeapAllocator>> {
    let bytes = &lid.as_bytes();

    assert!(bytes.len() > 0);

    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<message::Builder>();
        request.set_log_id(bytes);
        let mut request = request.init_pre_append_entries_request();
        request.set_term(term.as_u64());
        request.set_log_index(log_index.as_u64());
        request.set_entries_hash(entries_hash);

        // let mut entries_hash_list = request.init_entries_hash(entries_hash.len() as u32);
        // for (n, entry_hash) in entries_hash.iter().enumerate() {
        //     entries_hash_list.set(n as u32, entry_hash);
        // }
    }
    Rc::new(message)
}

pub fn pre_append_entries_response_success(term: Term,
                                           node_num: ServerId,
                                           log_index: LogIndex,
                                           entries_sig: (ServerId, &[u8]),
                                           lid: &LogId)
                                           -> Rc<Builder<HeapAllocator>> {
    let bytes = &lid.as_bytes();

    assert!(bytes.len() > 0);

    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<message::Builder>();
        response.set_log_id(bytes);
        let mut response = response.init_pre_append_entries_response();
        response.set_term(term.as_u64());
        response.set_node_num(node_num.into());
        // response.set_success(log_index.as_u64());
        response.set_log_index(log_index.as_u64());
        let mut sig = response.init_entries_sig();
        sig.set_server_id(entries_sig.0.into());
        sig.set_data(entries_sig.1);
    }
    Rc::new(message)
}

// RequestVote

pub fn request_vote_request(term: Term,
                            last_log_index: LogIndex,
                            last_log_term: Term,
                            lid: &LogId)
                            -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<message::Builder>();
        request.set_log_id(&lid.as_bytes());
        let mut request = request.init_request_vote_request();
        request.set_term(term.as_u64());
        request.set_last_log_index(last_log_index.as_u64());
        request.set_last_log_term(last_log_term.as_u64());
    }
    Rc::new(message)
}

pub fn request_vote_response_granted(term: Term, lid: &LogId) -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<message::Builder>();
        response.set_log_id(&lid.as_bytes());
        let mut response = response.init_request_vote_response();
        response.set_term(term.as_u64());
        response.set_granted(());
    }
    Rc::new(message)
}

pub fn request_vote_response_stale_term(term: Term, lid: &LogId) -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<message::Builder>();
        response.set_log_id(&lid.as_bytes());
        let mut response = response.init_request_vote_response();
        response.set_term(term.as_u64());
        response.set_stale_term(());
    }
    Rc::new(message)
}

pub fn request_vote_response_already_voted(term: Term, lid: &LogId) -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<message::Builder>();
        response.set_log_id(&lid.as_bytes());
        let mut response = response.init_request_vote_response();
        response.set_term(term.as_u64());
        response.set_already_voted(());
    }
    Rc::new(message)
}

pub fn request_vote_response_inconsistent_log(term: Term,
                                              lid: &LogId)
                                              -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<message::Builder>();
        response.set_log_id(&lid.as_bytes());
        let mut response = response.init_request_vote_response();
        response.set_term(term.as_u64());
        response.set_inconsistent_log(());
    }
    Rc::new(message)
}

pub fn request_vote_response_internal_error(term: Term,
                                            error: &str,
                                            lid: &LogId)
                                            -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<message::Builder>();
        response.set_log_id(&lid.as_bytes());
        let mut response = response.init_request_vote_response();
        response.set_term(term.as_u64());
        response.set_internal_error(error);
    }
    Rc::new(message)
}

// Ping

pub fn ping_request(session: TransactionId, lid: &LogId) -> Builder<HeapAllocator> {
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<client_request::Builder>();
        request.set_log_id(&lid.as_bytes());
        let mut request = request.init_ping();
        request.set_session(&session.as_bytes());
    }
    message
}

// Query

pub fn query_request(entry: &[u8], lid: &LogId) -> Builder<HeapAllocator> {
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<client_request::Builder>();
        request.set_log_id(&lid.as_bytes());
        let mut request = request.init_query();
        request.set_query(entry);
    }
    message
}


// Proposal

pub fn proposal_request(session: TransactionId,
                        entry: &[u8],
                        lid: LogId)
                        -> Builder<HeapAllocator> {
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<client_request::Builder>();
        request.set_log_id(&lid.as_bytes());
        let mut request = request.init_proposal();
        request.set_entry(entry);
        request.set_session(&session.as_bytes());
    }
    message
}

// Query / Proposal Response

pub fn command_response_success(data: &[u8], lid: LogId) -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<client_response::Builder>();
        response.set_log_id(&lid.as_bytes());
        response.init_proposal().set_success(data);
    }
    Rc::new(message)
}

pub fn command_response_unknown_leader(lid: LogId) -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<client_response::Builder>();
        response.set_log_id(&lid.as_bytes());
        response.init_proposal().set_unknown_leader(());
    }
    Rc::new(message)
}

pub fn command_response_not_leader(leader_hint: &SocketAddr,
                                   lid: LogId)
                                   -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<client_response::Builder>();
        response.set_log_id(&lid.as_bytes());
        response
            .init_proposal()
            .set_not_leader(&format!("{}", leader_hint));
    }
    Rc::new(message)
}

// Transaction

pub fn transaction_begin(lid: LogId, session: TransactionId) -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<message::Builder>();
        request.set_log_id(&lid.as_bytes());
        request
            .init_transaction_begin()
            .set_session(&session.as_bytes());
    }
    Rc::new(message)
}

pub fn transaction_commit(lid: LogId, session: TransactionId) -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<message::Builder>();
        request.set_log_id(&lid.as_bytes());
        request
            .init_transaction_commit()
            .set_session(&session.as_bytes());
    }
    Rc::new(message)
}

pub fn client_transaction_begin(lid: LogId, session: TransactionId) -> Builder<HeapAllocator> {
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<client_request::Builder>();
        request.set_log_id(&lid.as_bytes());
        request
            .init_transaction_begin()
            .set_session(&session.as_bytes());
    }
    message
}
pub fn client_transaction_commit(lid: LogId, session: TransactionId) -> Builder<HeapAllocator> {
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<client_request::Builder>();
        request.set_log_id(&lid.as_bytes());
        request
            .init_transaction_commit()
            .set_session(&session.as_bytes());
    }
    message
}

pub fn client_transaction_rollback(lid: LogId, session: TransactionId) -> Builder<HeapAllocator> {
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<client_request::Builder>();
        request.set_log_id(&lid.as_bytes());
        request
            .init_transaction_rollback()
            .set_session(&session.as_bytes());
    }
    message
}

pub fn command_transaction_success(data: &[u8], lid: LogId) -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<client_response::Builder>();
        response.set_log_id(&lid.as_bytes());
        response.init_proposal().set_success(data);
    }
    Rc::new(message)
}

pub fn command_transaction_failure(error: transaction::TransactionError,
                                   lid: LogId)
                                   -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<client_response::Builder>();
        response.set_log_id(&lid.as_bytes());
        response
            .init_transaction()
            .set_failure(format!("{}", error).as_bytes());
    }
    Rc::new(message)
}

pub fn transaction_rollback(lid: LogId, session: TransactionId) -> Rc<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<message::Builder>();
        request.set_log_id(&lid.as_bytes());
        request
            .init_transaction_rollback()
            .set_session(&session.as_bytes());
    }
    Rc::new(message)
}
