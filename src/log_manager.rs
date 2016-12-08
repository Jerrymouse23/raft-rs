use ServerId;
use ClientId;
use LogId;
use consensus::{Consensus, Actions, ConsensusTimeout};
use std::net::SocketAddr;
use std::collections::HashMap;
use persistent_log::Log;
use state_machine::StateMachine;
use std::rc::Rc;
use std::cell::RefCell;

use capnp::message::{Builder, HeapAllocator, Allocator, Reader, ReaderSegments, ReaderOptions};
use messages_capnp::{append_entries_request, append_entries_response, client_request,
                     proposal_request, query_request, message, request_vote_request,
                     request_vote_response};

use mio::Timeout as TimeoutHandle;

pub struct LogManager<L, M>
    where L: Log,
          M: StateMachine
{
    pub consensus: HashMap<LogId, Consensus<L, M>>,
}

impl<L, M> LogManager<L, M>
    where L: Log,
          M: StateMachine
{
    pub fn new(id: ServerId,
               store_logs: Vec<(LogId, L)>,
               peers: HashMap<ServerId, SocketAddr>,
               state_machine: M)
               -> Self {
        let mut logs: HashMap<LogId, Consensus<L, M>> = HashMap::new();
        let state_machine = Rc::new(RefCell::new(state_machine));

        for (lid, store) in store_logs {
            let consensus: Consensus<L, M> =
                Consensus::new(id, lid, peers.clone(), store, state_machine.clone());
            logs.insert(lid, consensus);
        }

        LogManager { consensus: logs }
    }

    pub fn get(&self, index: LogId) -> Option<&Consensus<L, M>> {
        self.consensus.get(&index)
    }

    pub fn active_transaction(&self, logid: &LogId) -> bool {
        self.consensus.get(logid).unwrap().transaction.isActive
    }

    pub fn init(&self) -> Vec<(LogId, Actions)> {
        let mut actions = Vec::new();
        for (id, ref mut consensus) in self.consensus.iter() {
            let ac = consensus.init();
            actions.push((*id, ac));
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
        let log_id = LogId(reader.get_log_id());

        // TODO implement error handling
        let mut cons = self.consensus.get_mut(&log_id).unwrap();

        cons.apply_client_message(from, &reader, actions, &log_id);
    }

    pub fn apply_peer_message<S>(&mut self,
                                 from: ServerId,
                                 message: &Reader<S>,
                                 actions: &mut Actions)
        where S: ReaderSegments
    {
        let reader = message.get_root::<message::Reader>().unwrap();
        let log_id = LogId(reader.get_log_id());

        // TODO implement error handling
        let mut cons = self.consensus.get_mut(&log_id).unwrap();

        cons.apply_peer_message(from, &reader, actions, &log_id);
    }

    pub fn peer_connection_reset(&mut self,
                                 peer: ServerId,
                                 addr: SocketAddr,
                                 actions: &mut Actions) {
        for (lid, mut cons) in self.consensus.iter_mut() {
            cons.peer_connection_reset(peer, addr, actions);
        }
    }

    pub fn apply_timeout(&mut self,
                         lid: &LogId,
                         consensus: ConsensusTimeout,
                         actions: &mut Actions) {
        self.consensus.get_mut(lid).unwrap().apply_timeout(consensus, actions);
    }

    // pub fn process_requests_in_queue(&mut self, mut actions: Actions) -> Actions {
    // let values = self.consensus.values_mut().clone();
    // for ref mut cons in values {
    // if !cons.transaction.isActive {
    // for (client, builder) in cons.requests_in_queue.pop() {
    // self.apply_client_message(client, &Self::into_reader(&builder), &mut actions)
    // }
    // }
    // }
    // actions
    // }
}
