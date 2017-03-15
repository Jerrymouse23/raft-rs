//! `Server` is a Rust type which is responsible for coordinating with other remote `Server`
//! instances, responding to commands from the `Client`, and applying commands to a local
//! `StateMachine` consensus. A `Server` may be a `Leader`, `Follower`, or `Candidate` at any given
//! time as described by the Raft Consensus Algorithm.

use std::{fmt, io};
use std::str::FromStr;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::rc::Rc;

use mio::tcp::TcpListener;
use mio::util::Slab;
use mio::{EventLoop, EventSet, Handler, PollOpt, Token};
use mio::Timeout as TimeoutHandle;
use capnp::message::{Builder, Allocator, HeapAllocator, Reader, ReaderOptions};
use capnp::serialize::{self, OwnedSegments};

use ClientId;
use Result;
use Error;
use RaftError;
use ServerId;
use LogId;
use messages;
use messages_capnp::connection_preamble;
use consensus::{Actions, ConsensusTimeout};
use state_machine::StateMachine;
use persistent_log::Log;
use connection::{Connection, ConnectionKind};
use std::io::Cursor;

use auth::Auth;
use log_manager::LogManager;

const LISTENER: Token = Token(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]

pub enum ServerTimeout {
    Consensus(LogId, ConsensusTimeout),
    Reconnect(Token),
}

/// The `Server` is responsible for receiving events from peer `Server` instance or clients,
/// as well as managing election and heartbeat timeouts. When an event is received, it is applied
/// to the local `Consensus`. The `Consensus` may optionally return a set of events to be
/// dispatched to either remote peers or clients.
///
/// ## Logging
///
/// Server instances log events according to frequency and importance. It is recommended to use at
/// least info level logging when running in production. The warn level is used for unexpected,
/// but recoverable events. The info level is used for infrequent events such as connection resets
/// and election results. The debug level is used for frequent events such as client proposals and
/// heartbeats. The trace level is used for very high frequency debugging output.
pub struct Server<L, M, A>
    where L: Log,
          M: StateMachine,
          A: Auth
{
    /// Id of this server.
    id: ServerId,

    addr: SocketAddr,

    // TODO implement getter for states
    /// Raft state machine consensus.
    pub log_manager: LogManager<L, M>,

    /// Connection listener.
    listener: TcpListener,

    /// Collection of connections indexed by token.
    connections: Slab<Connection>,

    /// Index of peer id to connection token.
    peer_tokens: HashMap<ServerId, Token>,

    /// Index of client id to connection token.
    client_tokens: HashMap<ClientId, Token>,

    /// Currently registered reconnection timeouts.
    reconnection_timeouts: HashMap<Token, TimeoutHandle>,

    /// String to allow connecting to different peers
    community_string: String,

    /// Instance of the authentification module
    auth: A,

    requests_in_queue: HashMap<LogId, Vec<(ClientId, Builder<HeapAllocator>)>>,
}

/// The implementation of the Server.
impl<L, M, A> Server<L, M, A>
    where L: Log,
          M: StateMachine,
          A: Auth
{
    /// Creates a new instance of the server.
    /// *Gotcha:* `peers` must not contain the local `id`.
    pub fn new(id: ServerId,
               addr: SocketAddr,
               peers: &HashMap<ServerId, SocketAddr>,
               community_string: String,
               auth: A,
               logs: Vec<(LogId, L, M)>)
               -> Result<(Server<L, M, A>, EventLoop<Server<L, M, A>>)> {
        if peers.contains_key(&id) {
            return Err(Error::Raft(RaftError::InvalidPeerSet));
        }

        let mut requests_in_queue = HashMap::new();

        for &(lid, _, _) in logs.iter() {
            requests_in_queue.insert(lid, Vec::new());
        }

        let log_manager = LogManager::new(id, logs, peers.clone());

        let mut event_loop = try!(EventLoop::<Server<L, M, A>>::new());
        let listener = try!(TcpListener::bind(&addr));
        try!(event_loop.register(&listener, LISTENER, EventSet::all(), PollOpt::level()));

        let mut server = Server {
            id: id,
            addr: addr,
            log_manager: log_manager,
            listener: listener,
            connections: Slab::new_starting_at(Token(1), 129),
            peer_tokens: HashMap::new(),
            client_tokens: HashMap::new(),
            reconnection_timeouts: HashMap::new(),
            community_string: community_string.clone(),
            auth: auth,
            requests_in_queue: requests_in_queue,
        };

        for (peer_id, peer_addr) in peers {
            server.add_peer_static(&mut event_loop, *peer_id, *peer_addr);
        }

        Ok((server, event_loop))
    }

    pub fn add_peer_static(&mut self,
                           event_loop: &mut EventLoop<Server<L, M, A>>,
                           peer_id: ServerId,
                           peer_addr: SocketAddr)
                           -> Result<()> {
        let token: Token = try!(self.connections
            .insert(try!(Connection::peer(peer_id, peer_addr)))
            .map_err(|_| Error::Raft(RaftError::ConnectionLimitReached)));
        scoped_assert!(self.peer_tokens.insert(peer_id, token).is_none());

        try!(self.connections[token].register(event_loop, token));

        let peers = self.log_manager.get_peers();
        let message = messages::server_connection_preamble(self.id,
                                                           &self.addr,
                                                           self.community_string.clone().as_str(),
                                                           &peers.read().unwrap());

        self.send_message(event_loop, token, message);

        Ok(())
    }

    pub fn peering_request(&mut self,
                           event_loop: &mut EventLoop<Server<L, M, A>>,
                           peer_id: ServerId,
                           peer_addr: SocketAddr)
                           -> Result<()> {

        scoped_debug!("Start peering with {:?}", peer_addr);

        let token: Token = try!(self.connections
            .insert(try!(Connection::peer(peer_id, peer_addr)))
            .map_err(|_| Error::Raft(RaftError::ConnectionLimitReached)));

        scoped_assert!(self.peer_tokens.insert(peer_id, token).is_none());

        self.log_manager.add_peer(peer_id, peer_addr);

        try!(self.connections[token].register(event_loop, token));

        let message = messages::server_add(self.id, &self.addr);

        self.send_message(event_loop, token, message);

        Ok(())
    }

    /// Runs a new Raft server in the current thread.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the new node.
    /// * `addr` - The address of the new node.
    /// * `peers` - The ID and address of all peers in the Raft cluster.
    /// * `store` - The persistent log store.
    /// * `state_machine` - The client state machine to which client commands will be applied.
    pub fn run(id: ServerId,
               addr: SocketAddr,
               peers: &HashMap<ServerId, SocketAddr>,
               community_string: String,
               auth: A,
               logs: Vec<(LogId, L, M)>)
               -> Self {
        let (mut server, mut event_loop) =
            Server::new(id, addr, peers, community_string, auth, logs).unwrap();

        server.init(&mut event_loop);

        event_loop.run(&mut server);

        server
    }

    /// Spawns a new Raft server in a background thread.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the new node.
    /// * `addr` - The address of the new node.
    /// * `peers` - The ID and address of all peers in the Raft cluster.
    /// * `store` - The persistent log store.
    /// * `state_machine` - The client state machine to which client commands will be applied.
    /// * `communinity_string` - To allow peers to connect to each other
    // TODO reimplement
    //    pub fn spawn(id: ServerId,
    // addr: SocketAddr,
    // peers: HashMap<ServerId, SocketAddr>,
    // store: L,
    // state_machine: M,
    // community_string: String,
    // auth: A)
    // -> Result<JoinHandle<Result<()>>> {
    // thread::Builder::new()
    // .name(format!("raft::Server({})", id))
    // .spawn(move || {
    // Server::run(id,
    // addr,
    // peers,
    // store,
    // state_machine,
    // community_string,
    // auth)
    // })
    // .map_err(From::from)
    // }
    /// Sends the message to the connection associated with the provided token.
    /// If sending the message fails, the connection is reset.
    fn send_message(&mut self,
                    event_loop: &mut EventLoop<Server<L, M, A>>,
                    token: Token,
                    message: Rc<Builder<HeapAllocator>>) {
        match self.connections[token].send_message(message) {
            Ok(false) => (),
            Ok(true) => {
                self.connections[token]
                    .reregister(event_loop, token)
                    .unwrap_or_else(|_| self.reset_connection(event_loop, token));
            }
            Err(error) => {
                scoped_warn!("{:?}: error while sending message: {:?}", self, error);
                self.reset_connection(event_loop, token);
            }
        }
    }

    fn execute_actions(&mut self, event_loop: &mut EventLoop<Server<L, M, A>>, actions: Actions) {
        scoped_trace!("executing actions: {:?}", actions);
        let Actions { peer_messages,
                      client_messages,
                      timeouts,
                      clear_timeouts,
                      clear_peer_messages,
                      peer_messages_broadcast,
                      transaction_queue } = actions;

        if clear_peer_messages {
            for &token in self.peer_tokens.values() {
                self.connections[token].clear_messages();
            }
        }
        for (peer, message) in peer_messages {
            let token = self.peer_tokens[&peer];
            self.send_message(event_loop, token, message);
        }
        for (client, message) in client_messages {
            if let Some(&token) = self.client_tokens.get(&client) {
                self.send_message(event_loop, token, message);
            }
        }

        if transaction_queue.len() > 0 {
            scoped_debug!("Messages appended to queue {}", transaction_queue.len());
            for (lid, cid, message) in transaction_queue {
                let mut messages = self.requests_in_queue.get_mut(&lid).unwrap();
                messages.push((cid, message));
            }
        }

        for message in peer_messages_broadcast {
            let tokens = self.peer_tokens.clone().into_iter().collect::<Vec<_>>();

            for (_, t) in tokens {
                self.send_message(event_loop, t, message.clone());
            }
        }

        for lid in clear_timeouts {
            let mut consensus = self.log_manager
                .get_mut(lid)
                .expect(&format!("Log {:?} is not registered in the log_manager", lid));

            for (timeout, &handle) in consensus.consensus_timeouts.iter() {
                scoped_assert!(event_loop.clear_timeout(handle),
                               "unable to clear timeout; {:?}",
                               timeout);
            }

            consensus.consensus_timeouts.clear();
        }

        for timeout in timeouts {
            let duration = timeout.duration_ms();

            let lid = match timeout {
                ConsensusTimeout::Election(lid) => lid,
                ConsensusTimeout::Heartbeat(_, lid) => lid,
            };

            // Registering a timeout may only fail if the maximum number of timeouts
            // is already registered, which is by default 65,536. We use a
            // maximum of one timeout per peer, so this unwrap should be safe.
            let handle = event_loop.timeout_ms(ServerTimeout::Consensus(lid, timeout), duration)
                .unwrap();

            let mut consensus = self.log_manager
                .get_mut(lid)
                .expect(&format!("Log {:?} is not registered in the log_manager", lid));

            consensus.consensus_timeouts
                .insert(timeout, handle)
                .map(|handle| {
                    scoped_assert!(event_loop.clear_timeout(handle),
                                   "unable to clear timeout: {:?}",
                                   timeout)
                });
        }
    }

    /// Resets the connection corresponding to the provided token.
    ///
    /// If the connection is to a peer, the server will attempt to reconnect after a waiting
    /// period.
    ///
    /// If the connection is to a client or unknown it will be closed.
    fn reset_connection(&mut self, event_loop: &mut EventLoop<Server<L, M, A>>, token: Token) {
        let kind = *self.connections[token].kind();
        match kind {
            ConnectionKind::Peer(..) => {
                // Crash if reseting the connection fails.
                let (timeout, handle) = self.connections[token]
                    .reset_peer(event_loop, token)
                    .unwrap();

                scoped_assert!(self.reconnection_timeouts.insert(token, handle).is_none(),
                               "timeout already registered: {:?}",
                               timeout);
            }
            ConnectionKind::Client(ref id) => {
                self.connections.remove(token).expect("unable to find client connection");
                scoped_assert!(self.client_tokens.remove(id).is_some(),
                               "client {:?} not connected",
                               id);
            }
            ConnectionKind::Unknown => {
                self.connections.remove(token).expect("unable to find unknown connection");
            }
        }
    }

    fn handle_queue(&mut self, actions: &mut Actions) {
        self.log_manager.handle_queue(&mut self.requests_in_queue, actions);
    }

    /// Reads messages from the connection until no more are available.
    ///
    /// If the connection returns an error on any operation, or any message fails to be
    /// deserialized, an error result is returned.
    fn readable(&mut self,
                event_loop: &mut EventLoop<Server<L, M, A>>,
                token: Token)
                -> Result<()> {

        let mut actions = Actions::new();
        self.handle_queue(&mut actions);
        self.execute_actions(event_loop, actions);

        scoped_trace!("{:?}: readable event", self.connections[token]);
        // Read messages from the connection until there are no more.
        while let Some(message) = try!(self.connections[token].readable()) {
            match *self.connections[token].kind() {
                ConnectionKind::Peer(id) => {
                    let mut actions = Actions::new();
                    self.log_manager.apply_peer_message(id, &message, &mut actions);
                    self.execute_actions(event_loop, actions);
                }
                ConnectionKind::Client(id) => {
                    let mut actions = Actions::new();
                    self.log_manager.apply_client_message(id, &message, &mut actions);
                    self.execute_actions(event_loop, actions);
                }
                ConnectionKind::Unknown => {
                    let preamble = try!(message.get_root::<connection_preamble::Reader>());
                    match try!(preamble.get_id().which()) {
                        connection_preamble::id::Which::ServerAdd(peer) => {
                            let peer = try!(peer);
                            let peer_id = ServerId(peer.get_id());

                            // Not the source address of this connection, but the
                            // address the peer tells us it's listening on.
                            let peer_addr = SocketAddr::from_str(try!(peer.get_addr())).unwrap();
                            scoped_debug!("received new connection from {:?} ({})",
                                          peer_id,
                                          peer_addr);

                            if !self.log_manager.check_peer_exists(peer_id) {
                                self.log_manager.add_peer(peer_id, peer_addr);
                                self.add_peer_static(event_loop, peer_id, peer_addr);
                            } else {
                                // Was already connected
                                scoped_debug!("Dynamic peer wants to reconnect {:?}", peer_addr);
                            }

                        }
                        connection_preamble::id::Which::Server(peer) => {
                            let peer = try!(peer);
                            let peer_id = ServerId(peer.get_id());

                            // Not the source address of this connection, but the
                            // address the peer tells us it's listening on.
                            let peer_addr = SocketAddr::from_str(try!(peer.get_addr())).unwrap();
                            scoped_debug!("received new connection from {:?} ({})",
                                          peer_id,
                                          peer_addr);

                            if peer.get_community().unwrap() != self.community_string {
                                scoped_debug!("peer with addr {:?} tried to connect with an \
                                              invalid community_string",
                                              peer_addr);
                            } else {

                                self.connections[token].set_kind(ConnectionKind::Peer(peer_id));
                                // Use the advertised address, not the remote's source
                                // address, for future retries in this connection.
                                self.connections[token].set_addr(peer_addr);

                                // Connect also to the other peers

                                if let Ok(peers) = peer.get_peers() {
                                    let num_peers: u32 = peers.len();

                                    // Filter peers which are already connected to
                                    let peers_vec: Vec<(ServerId, SocketAddr)> = peers.iter()
                                        .map(|entry| {
                                            (ServerId::from(entry.get_id()),
                                             entry.get_addr()
                                                 .unwrap()
                                                 .parse()
                                                 .expect("Received invalid peer addr"))
                                        })
                                        .filter(|&(id, _)| {
                                            !self.peer_tokens.contains_key(&id) && id != self.id
                                        })
                                        .collect();

                                    for &(id, addr) in peers_vec.iter() {
                                        self.peering_request(event_loop, id, addr);
                                    }
                                }

                                let prev_token = Some(match self.peer_tokens
                                    .insert(peer_id, token) {
                                    Some(x) => x,
                                    None => {
                                        self.log_manager.add_peer(peer_id, peer_addr);
                                        try!(self.connections[token].register(event_loop, token));

                                        token
                                    }
                                });

                                // Close the existing connection, if any.
                                // Currently, prev_token is never `None`; see above.
                                // With config changes, this will have to be handled.
                                match prev_token {
                                    Some(tok) => {
                                        self.connections
                                            .remove(tok)
                                            .expect("peer connection not found");

                                        // Clear any timeouts associated with the existing connection.
                                        self.reconnection_timeouts
                                            .remove(&tok)
                                            .map(|handle| {
                                                scoped_assert!(event_loop.clear_timeout(handle))
                                            });
                                    }
                                    _ => unreachable!(),
                                }
                                // Notify consensus that the connection reset.
                                let mut actions = Actions::new();
                                self.log_manager
                                    .peer_connection_reset(peer_id, peer_addr, &mut actions);
                                self.execute_actions(event_loop, actions);
                            }
                        }
                        connection_preamble::id::Which::Client(Ok(client)) => {
                            scoped_debug!("received new connection from a client");

                            let client_id = try!(ClientId::from_bytes(client.get_id().unwrap()));
                            let client_username = client.get_username().unwrap();
                            let client_password = client.get_password().unwrap();

                            if !self.auth.find(client_username, client_password) {
                                scoped_debug!("Wrong username or password");
                            } else {

                                scoped_debug!("Username and password are okay");
                                self.connections[token].set_kind(ConnectionKind::Client(client_id));
                                let prev_token = self.client_tokens
                                    .insert(client_id, token);
                                scoped_assert!(prev_token.is_none(),
                                               "{:?}: two clients connected with the same id: \
                                                {:?}",
                                               self,
                                               client_id);
                            }
                        }
                        _ => {
                            return Err(Error::Raft(RaftError::UnknownConnectionType));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Accepts a new TCP connection, adds it to the connection slab, and registers it with the
    /// event loop.
    fn accept_connection(&mut self, event_loop: &mut EventLoop<Server<L, M, A>>) -> Result<()> {
        scoped_trace!("accept_connection");
        self.listener
            .accept()
            .map_err(From::from)
            .and_then(|stream_opt| {
                stream_opt.ok_or_else(|| {
                    Error::Io(io::Error::new(io::ErrorKind::WouldBlock,
                                             "listener.accept() returned None"))
                })
            })
            .and_then(|(stream, _)| Connection::unknown(stream))
            .and_then(|conn| {
                self.connections
                    .insert(conn)
                    .map_err(|_| Error::Raft(RaftError::ConnectionLimitReached))
            })
            .and_then(|token|
                // Until this point if any failures occur the connection is simply dropped. From
                // this point down, the connection is stored in the slab, so dropping it would
                // result in a leaked TCP stream and slab entry. Instead of dropping the
                // connection, it will be reset if an error occurs.
                self.connections[token]
                    .register(event_loop, token)
                    .or_else(|_| {
                        self.reset_connection(event_loop, token);
                        Err(Error::Raft(RaftError::ConnectionRegisterFailed))
                    })
                    .map(|_| scoped_debug!("new connection accepted from {}",
                                           self.connections[token].addr())))
    }

    pub fn get_local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }


    pub fn init(&mut self, event_loop: &mut EventLoop<Server<L, M, A>>) {
        let action = self.log_manager.init();

        self.execute_actions(event_loop, action);
    }

    pub fn into_reader<C>(message: &Builder<C>) -> Reader<OwnedSegments>
        where C: Allocator
    {
        let mut buf = Cursor::new(Vec::new());

        serialize::write_message(&mut buf, message).unwrap();
        buf.set_position(0);
        serialize::read_message(&mut buf, ReaderOptions::new()).unwrap()
    }
}

impl<L, M, A> Handler for Server<L, M, A>
    where L: Log,
          M: StateMachine,
          A: Auth
{
    type Message = ();
    type Timeout = ServerTimeout;

    fn ready(&mut self,
             event_loop: &mut EventLoop<Server<L, M, A>>,
             token: Token,
             events: EventSet) {
        push_log_scope!("{:?}", self);
        scoped_trace!("ready; token: {:?}; events: {:?}", token, events);

        if events.is_error() {
            scoped_assert!(token != LISTENER, "unexpected error event from LISTENER");
            scoped_warn!("{:?}: error event", self.connections[token]);
            self.reset_connection(event_loop, token);
            return;
        }

        if events.is_hup() {
            scoped_assert!(token != LISTENER, "unexpected hup event from LISTENER");
            scoped_trace!("{:?}: hup event", self.connections[token]);
            self.reset_connection(event_loop, token);
            return;
        }

        if events.is_writable() {
            scoped_assert!(token != LISTENER, "unexpected writeable event for LISTENER");
            if let Err(error) = self.connections[token].writable() {
                scoped_warn!("{:?}: failed write: {}", self.connections[token], error);
                self.reset_connection(event_loop, token);
                return;
            }
            if !events.is_readable() {
                self.connections[token]
                    .reregister(event_loop, token)
                    .unwrap_or_else(|_| self.reset_connection(event_loop, token));
            }
        }

        if events.is_readable() {
            if token == LISTENER {
                self.accept_connection(event_loop)
                    .unwrap_or_else(|error| scoped_warn!("unable to accept connection: {}", error));
            } else {
                self.readable(event_loop, token)
                    // Only reregister the connection with the event loop if no error occurs and
                    // the connection is *not* reset.
                    .and_then(|_| self.connections[token].reregister(event_loop, token))
                    .unwrap_or_else(|error| {
                        scoped_warn!("{:?}: failed read: {}",
                                     self.connections[token], error);
                        self.reset_connection(event_loop, token);
                    });
            }
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Server<L, M, A>>, timeout: ServerTimeout) {
        push_log_scope!("{:?}", self);
        scoped_trace!("timeout: {:?}", &timeout);
        match timeout {
            ServerTimeout::Consensus(lid, ctimeout) => {
                //
                scoped_assert!(self.log_manager
                                   .get_mut(lid)
                                   .expect(&format!("Log {:?} is not registered in the \
                                                     log_manager",
                                                    lid))
                                   .consensus_timeouts
                                   .remove(&ctimeout)
                                   .is_some(),
                               "missing timeout: {:?}",
                               timeout);
                let mut actions = Actions::new();
                self.log_manager.apply_timeout(&lid, ctimeout, &mut actions);
                self.execute_actions(event_loop, actions);
            }

            ServerTimeout::Reconnect(token) => {
                scoped_assert!(self.reconnection_timeouts.remove(&token).is_some(),
                               "{:?} missing timeout: {:?}",
                               self.connections[token],
                               timeout);
                let local_addr = self.listener.local_addr();
                scoped_assert!(local_addr.is_ok(), "could not obtain listener address");
                let id = match *self.connections[token].kind() {
                    ConnectionKind::Peer(id) => id,
                    _ => unreachable!(),
                };
                let addr = self.connections[token].addr().clone();
                let peers = self.log_manager.get_peers();
                self.connections[token]
                    .reconnect_peer(self.id,
                                    &local_addr.unwrap(),
                                    self.community_string.clone(),
                                    &peers.read().unwrap())
                    .and_then(|_| self.connections[token].register(event_loop, token))
                    .map(|_| {
                        let mut actions = Actions::new();
                        self.log_manager.peer_connection_reset(id, addr, &mut actions);
                        self.execute_actions(event_loop, actions);
                    })
                    .unwrap_or_else(|error| {
                        scoped_warn!("unable to reconnect connection {:?}: {}",
                                     self.connections[token],
                                     error);
                        self.reset_connection(event_loop, token);
                    });
            }
        }
    }
}

impl<L, M, A> fmt::Debug for Server<L, M, A>
    where L: Log,
          M: StateMachine,
          A: Auth
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Server({})", self.id)
    }
}

#[cfg(test)]
mod tests {

    extern crate env_logger;

    use std::collections::HashMap;
    use std::io::{self, Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::str::FromStr;

    use capnp::message::ReaderOptions;
    use capnp::serialize;
    use mio::EventLoop;

    use ClientId;
    use Result;
    use ServerId;
    use LogId;
    use messages;
    use messages_capnp::connection_preamble;
    use consensus::Actions;
    use state_machine::NullStateMachine;
    use persistent_log::MemLog;
    use super::*;
    use auth::Auth;
    use auth::null::NullAuth;
    use auth::credentials::SingleCredentials;
    use uuid::Uuid;

    type TestServer = Server<MemLog, NullStateMachine, NullAuth<SingleCredentials>>;
    lazy_static!{
        static ref lid: LogId = LogId(Uuid::new_v4());
    }

    fn new_test_server(peers: HashMap<ServerId, SocketAddr>)
                       -> Result<(TestServer, EventLoop<TestServer>)> {
        let mut logs: Vec<(LogId, MemLog, NullStateMachine)> = Vec::new();
        logs.push((*lid, MemLog::new(), NullStateMachine));
        Server::new(ServerId::from(0),
                    SocketAddr::from_str("127.0.0.1:0").unwrap(),
                    &peers,
                    "test".to_string(),
                    NullAuth::new(SingleCredentials::new("test".to_string(), "test".to_string())),
                    logs)
    }

    /// Attempts to grab a local, unbound socket address for testing.
    fn get_unbound_address() -> SocketAddr {
        TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
    }

    /// Verifies that the proved stream has been sent a va*lid connection
    /// preamble.
    fn read_server_preamble<R>(read: &mut R) -> ServerId
        where R: Read
    {
        let message = serialize::read_message(read, ReaderOptions::new()).unwrap();
        let preamble = message.get_root::<connection_preamble::Reader>().unwrap();

        match preamble.get_id().which().unwrap() {
            connection_preamble::id::Which::Server(peer) => ServerId::from(peer.unwrap().get_id()),
            _ => {
                panic!("unexpected preamble id");
            }
        }
    }

    /// Returns true if the server has an open connection with the peer.
    fn peer_connected(server: &TestServer, peer: ServerId) -> bool {
        let token = server.peer_tokens[&peer];
        server.reconnection_timeouts.get(&token).is_none()
    }

    /// Returns true if the server has an open connection with the client.
    fn client_connected(server: &TestServer, client: ClientId) -> bool {
        server.client_tokens.contains_key(&client)
    }

    /// Returns true if the provided TCP connection has been shutdown.
    ///
    /// TODO: figure out a more robust way to implement this, the current check
    /// will block the thread indefinitely if the stream is not shutdown.
    fn stream_shutdown(stream: &mut TcpStream) -> bool {
        let mut buf = [0u8; 128];
        // OS X returns a read of 0 length for closed sockets.
        // Linux returns an errcode 104: Connection reset by peer.
        match stream.read(&mut buf) {
            Ok(0) => true,
            Err(ref error) if error.kind() == io::ErrorKind::ConnectionReset => true,
            Err(ref error) => panic!("unexpected error: {}", error),
            _ => false,
        }
    }

    /// Tests that a Server will reject an inva*lid peer configuration set.
    #[test]
    fn test_illegal_peer_set() {
        setup_test!("test_illegal_peer_set");
        let peer_id = ServerId::from(0);
        let mut peers = HashMap::new();
        peers.insert(peer_id, SocketAddr::from_str("127.0.0.1:0").unwrap());
        assert!(new_test_server(peers).is_err());
    }

    /// Tests that a Server connects to peer at startup, and reconnects when the
    /// connection is dropped.
    #[test]
    fn test_peer_connect() {
        setup_test!("test_peer_connect");
        let peer_id = ServerId::from(1);

        let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let mut peers = HashMap::new();
        peers.insert(peer_id, peer_listener.local_addr().unwrap());
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        // Accept the server's connection.
        let (mut stream, _) = peer_listener.accept().unwrap();

        // Check that the server sends a va*lid preamble.
        assert_eq!(ServerId::from(0), read_server_preamble(&mut stream));
        assert!(peer_connected(&server, peer_id));

        // Drop the connection.
        drop(stream);
        event_loop.run_once(&mut server, None).unwrap();
        assert!(!peer_connected(&server, peer_id));

        // Check that the server reconnects after a timeout.
        event_loop.run_once(&mut server, None).unwrap();
        assert!(peer_connected(&server, peer_id));
        let (mut stream, _) = peer_listener.accept().unwrap();

        // Check that the server sends a va*lid preamble after the connection is
        // established.
        assert_eq!(ServerId::from(0), read_server_preamble(&mut stream));
        assert!(peer_connected(&server, peer_id));
    }

    /// Tests that a Server will replace a peer's TCP connection if the peer
    /// connects through another TCP connection.
    #[test]
    fn test_peer_accept() {
        setup_test!("test_peer_accept");
        let peer_id = ServerId::from(1);

        let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let mut peers = HashMap::new();
        peers.insert(peer_id, peer_listener.local_addr().unwrap());
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        // Accept the server's connection.
        let (mut in_stream, _) = peer_listener.accept().unwrap();

        // Check that the server sends a va*lid preamble.
        assert_eq!(ServerId::from(0), read_server_preamble(&mut in_stream));
        assert!(peer_connected(&server, peer_id));

        let server_addr = server.listener.local_addr().unwrap();

        // Open a replacement connection to the server.
        let mut out_stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server, None).unwrap();

        // This is what the new peer tells the server is listening address is.
        let fake_peer_addr = SocketAddr::from_str("192.168.0.1:12345").unwrap();
        // Send server the preamble message to the server.
        serialize::write_message(&mut out_stream,
                                 &*messages::server_connection_preamble(peer_id,
                                                                        &fake_peer_addr,
                                                                        "test",
                                                                        &HashMap::new()))
            .unwrap();
        out_stream.flush().unwrap();
        event_loop.run_once(&mut server, None).unwrap();

        // Make sure that reconnecting updated the peer address
        // known to `Consensus` with the one given in the preamble.
        assert_eq!(server.log_manager.get(*lid).unwrap().peers()[&peer_id],
                   fake_peer_addr);
        // Check that the server has closed the old connection.
        assert!(stream_shutdown(&mut in_stream));
        // Check that there's a connection which has the fake address
        // stored for reconnection purposes.
        assert!(server.connections.iter().any(|conn| conn.addr().port() == 12345))
    }

    /// Tests that the server will accept a client connection, then disposes of
    /// it when the client disconnects.
    #[test]
    fn test_client_accept() {
        setup_test!("test_client_accept");

        let (mut server, mut event_loop) = new_test_server(HashMap::new()).unwrap();

        // Connect to the server.
        let server_addr = server.listener.local_addr().unwrap();
        let mut stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server, None).unwrap();

        let client_id = ClientId::new();

        // Send the client preamble message to the server.
        serialize::write_message(&mut stream,
                                 &*messages::client_connection_preamble(client_id,
                                                                        "username",
                                                                        "password"))
            .unwrap();
        stream.flush().unwrap();
        event_loop.run_once(&mut server, None).unwrap();

        // Check that the server holds on to the client connection.
        assert!(client_connected(&server, client_id));

        // Check that the server disposes of the client connection when the TCP
        // stream is dropped.
        drop(stream);
        event_loop.run_once(&mut server, None).unwrap();
        assert!(!client_connected(&server, client_id));
    }

    /// Tests that the server will throw away connections that do not properly
    /// send a preamble.
    #[test]
    fn test_invalid_accept() {
        setup_test!("test_invalid_accept");

        let (mut server, mut event_loop) = new_test_server(HashMap::new()).unwrap();

        // Connect to the server.
        let server_addr = server.listener.local_addr().unwrap();
        let mut stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server, None).unwrap();

        // Send an inva*lid preamble.
        stream.write(b"foo bar baz").unwrap();
        stream.flush().unwrap();
        event_loop.run_once(&mut server, None).unwrap();

        // Check that the server disposes of the connection.
        assert!(stream_shutdown(&mut stream));
    }

    /// Tests that the server will reset a peer connection when an inva*lid
    /// message is received.
    #[test]
    fn test_invalid_peer_message() {
        setup_test!("test_invalid_peer_message");

        let peer_id = ServerId::from(1);

        let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let mut peers = HashMap::new();
        peers.insert(peer_id, peer_listener.local_addr().unwrap());
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        // Accept the server's connection.
        let (mut stream_a, _) = peer_listener.accept().unwrap();

        // Read the server's preamble.
        assert_eq!(ServerId::from(0), read_server_preamble(&mut stream_a));

        // Send an inva*lid message.
        stream_a.write(b"foo bar baz").unwrap();
        stream_a.flush().unwrap();
        event_loop.run_once(&mut server, None).unwrap();

        // Check that the server resets the connection.
        assert!(!peer_connected(&server, peer_id));

        // Check that the server reconnects after a timeout.
        event_loop.run_once(&mut server, None).unwrap();
        assert!(peer_connected(&server, peer_id));
    }

    /// Tests that the server will reset a client connection when an inva*lid
    /// message is received.
    #[test]
    fn test_invalid_client_message() {
        setup_test!("test_invalid_client_message");

        let (mut server, mut event_loop) = new_test_server(HashMap::new()).unwrap();

        // Connect to the server.
        let server_addr = server.listener.local_addr().unwrap();
        let mut stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server, None).unwrap();

        let client_id = ClientId::new();

        // Send the client preamble message to the server.
        serialize::write_message(&mut stream,
                                 &*messages::client_connection_preamble(client_id,
                                                                        "username",
                                                                        "password"))
            .unwrap();
        stream.flush().unwrap();
        event_loop.run_once(&mut server, None).unwrap();

        // Check that the server holds on to the client connection.
        assert!(client_connected(&server, client_id));

        // Send an invalid client message to the server.
        stream.write(b"foo bar baz").unwrap();
        stream.flush().unwrap();
        event_loop.run_once(&mut server, None).unwrap();

        // Check that the server disposes of the client connection.
        assert!(!client_connected(&server, client_id));
    }

    /// Tests that a Server will attempt to connect to peers on startup, and
    /// immediately reset the connection if unreachable.
    #[test]
    fn test_unreachable_peer() {
        setup_test!("test_unreachable_peer_reconnect");
        let peer_id = ServerId::from(1);
        let mut peers = HashMap::new();
        peers.insert(peer_id, get_unbound_address());

        // Creates the Server, which registers the peer connection, and
        // immediately resets it.
        let (mut server, _) = new_test_server(peers).unwrap();
        assert!(!peer_connected(&mut server, peer_id));
    }

    /// Tests that the server will send a message to a peer connection.
    #[test]
    fn test_connection_send() {
        setup_test!("test_connection_send");
        let peer_id = ServerId::from(1);

        let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let mut peers = HashMap::new();
        let peer_addr = peer_listener.local_addr().unwrap();
        peers.insert(peer_id, peer_addr);
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        // Accept the server's connection.
        let (mut in_stream, _) = peer_listener.accept().unwrap();

        // Accept the preamble.
        assert_eq!(ServerId::from(0), read_server_preamble(&mut in_stream));

        // Send a test message (the type is not important).
        let mut actions = Actions::new();
        actions.peer_messages
            .push((peer_id,
                   messages::server_connection_preamble(peer_id,
                                                        &peer_addr,
                                                        "test",
                                                        &HashMap::new())));
        server.execute_actions(&mut event_loop, actions);

        assert_eq!(peer_id, read_server_preamble(&mut in_stream));
    }
}
