use ServerId;
use ClientId;
use LogId;
use consensus::Consensus;
use consensus::Actions;
use std::net::SocketAddr;
use std::collections::HashMap;
use persistent_log::Log;
use state_machine::StateMachine;

use capnp::message::{Builder, HeapAllocator, Reader, ReaderSegments};
use messages_capnp::{append_entries_request, append_entries_response, client_request,
                     proposal_request, query_request, message, request_vote_request,
                     request_vote_response};

use mio::Timeout as TimeoutHandle;

pub struct LogManager<L, M> {
    pub consensus: HashMap<LogId, Consensus<L, M>>,
    requests_in_queue: HashMap<LogId, Vec<(ClientId, Builder<HeapAllocator>)>>,
}

impl<L, M> LogManager<L, M>
    where L: Log,
          M: StateMachine
{
    pub fn new(id: ServerId,
               logs: Vec<(LogId, L)>,
               peers: HashMap<ServerId, SocketAddr>,
               state_machine: M)
               -> Self {
        let logs: HashMap<LogId, Consensus<L, M>> = HashMap::new();

        for (index, store) in logs {
            let consenus = Consensus::new(id, peers.clone(), store, state_machine);
            logs.insert(index, &consenus);
        }

        LogManager {
            consensus: logs,
            requests_in_queue: HashMap::new(),
        }
    }

    pub fn get(&self, index: LogId) -> Option<&Consensus<L, M>> {
        self.consensus.get(&index)
    }

    pub fn active_transaction(&self, logid: &LogId) -> bool {
        self.consensus.get(logid).unwrap().transaction.isActive
    }

    pub fn get_requests_in_queue(&self,
                                 logid: &LogId)
                                 -> Option<&Vec<(ClientId, Builder<HeapAllocator>)>> {
        self.requests_in_queue.get(logid)
    }

    pub fn init(&self) -> Vec<(LogId, Actions)> {
        let mut actions = Vec::new();
        for (id, mut consensus) in self.consensus {
            let ac = consensus.init();
            actions.push((id, ac));
        }

        actions
    }

    pub fn apply_client_message<S>(&mut self,
                                   from: ClientId,
                                   message: &Reader<S>,
                                   actions: &mut Actions)
        where S: ReaderSegments
    {
        let reader = message.get_root::<client_request::Reader>().unwrap();
        let log_id = reader.get_log_id().unwrap();

        // TODO implement error handling
        let mut cons = self.consensus.get(&log_id).unwrap();

        cons.apply_client_message(from, reader, actions, log_id);
    }

    pub fn apply_peer_message<S>(&mut self,
                                 from: ServerId,
                                 message: &Reader<S>,
                                 actions: &mut Actions)
        where S: ReaderSegments
    {
        let reader = message.get_root::<message::Reader>().unwrap();
        let log_id = reader.get_log_id();

        // TODO implement error handling
        let mut cons = self.consensus.get(&log_id).unwrap();

        cons.apply_peer_message(from, message, actions, &log_id);
    }

    pub fn peer_connection_reset(&mut self,
                                 peer: ServerId,
                                 addr: SocketAddr,
                                 actions: &mut Actions) {
        for (lid, mut cons) in self.consensus {
            cons.peer_connection_reset(peer, addr, actions);
        }
    }

    pub fn apply_timeout(&mut self, timeout: ConsensusTimeout, actions: &mut Actions) {}
}
