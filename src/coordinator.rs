//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Instant, Duration};

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision
}

/// Coordinator
/// Struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
    num_requests: u64,
    num_commits: u64, 
    num_aborts: u64,
    participant_map: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
    client_map: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>
}

///
/// Coordinator
/// Implementation of coordinator functionality
/// Required:
/// 1. new -- Constructor
/// 2. protocol -- Implementation of coordinator side of protocol
/// 3. report_status -- Report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- What to do when a participant joins
/// 5. client_join -- What to do when a client joins
///
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     log_path: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///
    pub fn new(
        log_path: String,
        r: &Arc<AtomicBool>) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            num_requests: 0,
            num_commits: 0,
            num_aborts: 0,
            participant_map: HashMap::new(),
            client_map: HashMap::new(),
        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self, name: String,
         participant_tx: Sender<ProtocolMessage>,
         participant_rx: Receiver<ProtocolMessage>
        ) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.participant_map.insert(String::from(name), (participant_tx, participant_rx));
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(&mut self, name: String,
        client_tx: Sender<ProtocolMessage>,
        client_rx: Receiver<ProtocolMessage>
    ) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.client_map.insert(String::from(name), (client_tx, client_rx));
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        let successful_ops: u64 = self.num_commits;
        let failed_ops: u64 = self.num_aborts;
        let unknown_ops: u64 = self.num_requests - self.num_commits - self.num_aborts;

        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", successful_ops, failed_ops, unknown_ops);
    }


    fn get_request(&mut self) -> Option<ProtocolMessage> {
        while self.running.load(Ordering::SeqCst) {
            for (name, (client_tx, client_rx)) in self.client_map.iter() {
                if let Ok(pm) = client_rx.try_recv() {
                    if pm.mtype == MessageType::ClientRequest {
                        return Some(pm)
                    }
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
        None
    }

    fn check_commits(&mut self, request: &ProtocolMessage) -> bool {
        let start_time = Instant::now();

        for (name, (participant_tx, participant_rx)) in self.participant_map.iter() {
            let mut voted = false;

            while Instant::now() < start_time + Duration::from_secs(1) {
                if let Ok(vote) = participant_rx.try_recv() {
                    if vote.txid != request.txid {
                        continue;
                    }
                    if vote.mtype != MessageType::ParticipantVoteCommit {
                        return false;
                    }
                    voted = true; 
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }

            if !voted {
                return false;
            }
            
        }
        true
    }

    fn broadcast(&mut self, messageType: MessageType, request: &ProtocolMessage) {
        for (name, (participant_tx, participant_rx)) in self.participant_map.iter() {
            participant_tx.send(
                ProtocolMessage::generate(messageType, request.txid.clone(), String::from("coordinator"), request.opid.clone())
            ).expect("Failed to send message");
        }
    }


    fn broadcast_exit(&mut self) {
        let exit_message = ProtocolMessage::generate(MessageType::CoordinatorExit, String::new(), String::from("coordinator"), 0);
        for (name, (client_tx, client_rx)) in self.client_map.iter() {
            client_tx.send(exit_message.clone())
            .expect("failed to send exit to client");
        }
        for (name, (participant_tx, participant_rx)) in self.participant_map.iter() {
            participant_tx.send(exit_message.clone())
            .expect("failed to send exit to participant");
        }
    }


    ///
    /// protocol() 
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self, expected_requests: u64) {
        for i in 0..expected_requests {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            
            let wrapped_result = self.get_request(); 
            if wrapped_result.is_none() {
                break;
            }
            let request = wrapped_result.unwrap();
            self.num_requests += 1; 
            self.state = CoordinatorState::ReceivedRequest;
            self.broadcast(MessageType::CoordinatorPropose, &request);
            self.state = CoordinatorState::ProposalSent;

            let result;
            let message; 

            if self.check_commits(&request) {
                self.num_commits += 1;
                self.state = CoordinatorState::ReceivedVotesCommit;
                message = MessageType::CoordinatorCommit;
                result = MessageType::ClientResultCommit;
            } else {
                self.num_aborts += 1;
                self.state = CoordinatorState::ReceivedVotesAbort;
                message = MessageType::CoordinatorAbort;
                result = MessageType::ClientResultAbort;
            }

            self.log.append(message, request.txid.clone(), String::from("coordinator"), request.opid.clone());

            self.broadcast(message, &request);
            if let Some((client_tx, client_rx)) = self.client_map.get(&request.senderid) {
                client_tx.send(ProtocolMessage::generate(result, request.txid.clone(), String::from("coordinator"), request.opid.clone()))
                .expect("Failed to send result to client");
            }
            self.state = CoordinatorState::Quiescent;
        }
        self.broadcast_exit();
        self.report_status();
    }
}
