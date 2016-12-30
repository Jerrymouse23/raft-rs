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
use uuid::Uuid;

use capnp::message::{Reader, ReaderSegments};
use messages_capnp::{client_request, message};

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
               store_logs: Vec<(LogId, L, M)>,
               peers: HashMap<ServerId, SocketAddr>)
               -> Self {
        let mut logs: HashMap<LogId, Consensus<L, M>> = HashMap::new();

        for (lid, log, state_machine) in store_logs {
            let consensus: Consensus<L, M> =
                Consensus::new(id, lid, peers.clone(), log, state_machine);
            logs.insert(lid, consensus);
        }

        LogManager { consensus: logs }
    }

    pub fn get(&self, index: LogId) -> Option<&Consensus<L, M>> {
        self.consensus.get(&index)
    }

    pub fn get_mut(&mut self, index: LogId) -> Option<&mut Consensus<L, M>> {
        self.consensus.get_mut(&index)
    }

    pub fn active_transaction(&self, logid: &LogId) -> bool {
        self.consensus.get(logid).unwrap().transaction.is_active
    }

    pub fn init(&self) -> Actions {
        let mut actions = Actions::new();
        for (id, _) in self.consensus.iter() {
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
        for (_, mut cons) in self.consensus.iter_mut() {
            cons.peer_connection_reset(peer, addr, actions);
        }
    }

    pub fn apply_timeout(&mut self,
                         lid: &LogId,
                         consensus: ConsensusTimeout,
                         actions: &mut Actions) {
        self.consensus.get_mut(lid).unwrap().apply_timeout(consensus, actions);
    }

    pub fn handle_queue(&mut self, actions: &mut Actions) {
        for (_, mut con) in self.consensus.iter_mut() {
            con.handle_queue(actions);
        }
    }
}
