use ServerId;
use ClientId;
use LogId;
use StateInformation;
use consensus::{Consensus, Actions, ConsensusTimeout};
use std::net::SocketAddr;
use std::collections::HashMap;
use persistent_log::Log;
use state_machine::StateMachine;
use uuid::Uuid;

use std::sync::{Arc, RwLock};

use state::{LeaderState, CandidateState, FollowerState};

use capnp::message::{Reader, ReaderSegments, Builder, HeapAllocator};
use messages_capnp::{client_request, message};
pub struct LogManager<L, M>
    where L: Log,
          M: StateMachine
{
    peers: Arc<RwLock<HashMap<ServerId, SocketAddr>>>,
    pub consensus: HashMap<LogId, Consensus<L, M>>,
}

impl<L, M> LogManager<L, M>
    where L: Log,
          M: StateMachine
{
    pub fn new(id: ServerId,
               store_logs: Vec<(LogId, L, M)>,
               peers: HashMap<ServerId, SocketAddr>)
               -> Self {
        let mut logs: HashMap<LogId, Consensus<L, M>> = HashMap::new();

        for (lid, log, state_machine) in store_logs {
            let consensus: Consensus<L, M> =
                Consensus::new(id, lid, peers.clone(), log, state_machine);
            logs.insert(lid, consensus);
        }

        LogManager {
            consensus: logs,
            peers: Arc::new(RwLock::new(peers)),
        }
    }

    pub fn get(&self, index: LogId) -> Option<&Consensus<L, M>> {
        self.consensus.get(&index)
    }

    pub fn get_mut(&mut self, index: LogId) -> Option<&mut Consensus<L, M>> {
        self.consensus.get_mut(&index)
    }

    pub fn active_transaction(&self, logid: &LogId) -> bool {
        self.consensus[logid].transaction.is_active
    }

    pub fn init(&self) -> Actions {
        let mut actions = Actions::new();
        for id in self.consensus.keys() {
            scoped_info!("Consensus Election timeout initialised: {:?}", id);

            actions.timeouts.push(ConsensusTimeout::Election(*id));
        }

        actions
    }

    pub fn apply_client_message<S>(&mut self,
                                   from: ClientId,
                                   message: &Reader<S>,
                                   actions: &mut Actions)
        where S: ReaderSegments
    {
        // TODO: implement error handling
        let reader = message.get_root::<client_request::Reader>().unwrap();
        let log_id = LogId(Uuid::from_bytes(reader.get_log_id().unwrap()).unwrap());

        scoped_trace!("Received client message on log {:?}", log_id);

        // TODO implement error handling
        let mut cons = self.consensus.get_mut(&log_id).unwrap();

        cons.apply_client_message(from, &reader, actions);
    }

    pub fn apply_peer_message<S>(&mut self,
                                 from: ServerId,
                                 message: &Reader<S>,
                                 actions: &mut Actions)
        where S: ReaderSegments
    {
        let reader = message.get_root::<message::Reader>().unwrap();

        let logid_in_bytes = reader.get_log_id()
            .expect("LogId property was not set in the message");
        let id = match Uuid::from_bytes(logid_in_bytes) {
            Ok(id) => id,
            Err(_) => {
                scoped_warn!("Received a message with an invalid LogId; Discarding it");
                return;
            }
        };

        let log_id = LogId(id);

        // TODO implement error handling
        let mut cons = self.consensus
            .get_mut(&log_id)
            .expect(&format!("There is not consensus instance with this {:?}", log_id));

        cons.apply_peer_message(from, &reader, actions);
    }

    pub fn peer_connection_reset(&mut self,
                                 peer: ServerId,
                                 addr: SocketAddr,
                                 actions: &mut Actions) {
        for mut cons in self.consensus.values_mut() {
            cons.peer_connection_reset(peer, addr, actions);
        }
    }

    pub fn apply_timeout(&mut self,
                         lid: &LogId,
                         consensus: ConsensusTimeout,
                         actions: &mut Actions) {
        self.consensus.get_mut(lid).unwrap().apply_timeout(consensus, actions);
    }

    pub fn handle_queue(&mut self,
                        requests_in_queue: &mut HashMap<LogId,
                                                        Vec<(ClientId, Builder<HeapAllocator>)>>,
                        actions: &mut Actions) {

        for (&lid, ref mut messages) in requests_in_queue.iter_mut() {
            // TODO implement deref for LogId
            self.consensus
                .get_mut(&LogId::from(&format!("{}", lid)).unwrap())
                .expect(&format!("Cannot find {:?} in the logs", lid))
                .handle_queue(messages, actions)
                .unwrap();
        }
    }

    pub fn get_states(&self) -> HashMap<LogId, StateInformation> {
        let mut result: HashMap<LogId, StateInformation> = HashMap::new();
        for (&lid, cons) in &self.consensus {
            let leader_state = cons.leader_state.clone();
            let candidate_state = cons.candidate_state.clone();
            let follower_state = cons.follower_state.clone();

            result.insert(lid, (leader_state, candidate_state, follower_state));
        }

        result
    }

    pub fn get_state_machines(&self) -> HashMap<LogId, Arc<RwLock<M>>> {
        let mut result = HashMap::new();

        for (&lid, cons) in &self.consensus {
            let state_machine = cons.state_machine.clone();

            result.insert(lid, state_machine);
        }

        result
    }

    pub fn add_peer(&mut self, peer_id: ServerId, peer_addr: SocketAddr) {
        let mut lock = self.peers.write().unwrap();
        assert!(lock.insert(peer_id, peer_addr).is_none());

        for mut cons in self.consensus.values_mut() {
            cons.add_peer(peer_id, peer_addr);
        }
    }

    pub fn check_peer_exists(&self, peer_id: ServerId) -> bool {
        let (_, cons) = self.consensus.iter().next().unwrap();

        cons.peers().contains_key(&peer_id)
    }

    pub fn get_peers(&self) -> Arc<RwLock<HashMap<ServerId, SocketAddr>>> {
        self.peers.clone()
    }
}
