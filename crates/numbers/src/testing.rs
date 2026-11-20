//! Test support: a fake DNS server on 127.0.0.1 (cargo feature `test-support`).
//!
//! [`FakeDns`] answers DNS queries over UDP from a table the test sets, and counts what it was
//! asked. Point a [`DnsLocator`](crate::bootstrap::DnsLocator) at it with
//! [`DnsLocator::with_nameserver`](crate::bootstrap::DnsLocator::with_nameserver). It never
//! forwards a query anywhere.

use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use simple_dns::rdata::{A, RData, SRV};
use simple_dns::{CLASS, Name, Packet, PacketFlag, QTYPE, RCODE, ResourceRecord, TYPE};
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

/// The TTL of every record the fake server returns, in seconds.
const TTL: u32 = 300;
/// The largest query the fake server reads.
const MAX_QUERY: usize = 1500;

/// One query the fake server received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    /// The name asked for, in lower case, without the trailing dot.
    pub name: String,
    /// The record type asked for, such as `"SRV"` or `"A"`.
    pub qtype: String,
}

/// What the fake server answers for one name.
#[derive(Clone, Debug)]
enum Entry {
    /// SRV records: (port, target).
    Srv(Vec<(u16, String)>),
    /// One A record, 127.0.0.1.
    A,
    /// No error, no records, whatever the type asked.
    Empty,
}

#[derive(Debug, Default)]
struct State {
    zone: HashMap<String, Entry>,
    silent: bool,
    queries: Vec<Query>,
}

/// A DNS server on 127.0.0.1 that answers from a table the test sets.
///
/// It binds UDP on a free port and serves until it is dropped. A name it does not know gets
/// NXDOMAIN. Names are compared in lower case and without a trailing dot. Every record it returns
/// has a TTL of 300 seconds. It serves UDP only: the TCP query a resolver adds after 900 ms of UDP
/// silence finds nothing listening and is refused. It needs a tokio runtime with IO enabled.
pub struct FakeDns {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
    task: JoinHandle<()>,
}

impl FakeDns {
    /// Starts a server with an empty table, which answers NXDOMAIN to everything.
    pub async fn start() -> io::Result<Self> {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = socket.local_addr()?;
        let state = Arc::new(Mutex::new(State::default()));
        let task = tokio::spawn(serve(socket, Arc::clone(&state)));
        Ok(Self { addr, state, task })
    }

    /// The address the server listens on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Answers SRV queries for `name` with one record per (target, port), in that order. Priority
    /// rises by 10 from one record to the next; weight is 1. A target of `"."` or `""` is the root
    /// name ("the service is not available"). Queries of other types for `name` get an empty
    /// answer.
    pub fn set_srv(&self, name: &str, targets: &[(&str, u16)]) {
        let records = targets
            .iter()
            .map(|(target, port)| (*port, (*target).to_owned()))
            .collect();
        self.lock().zone.insert(key(name), Entry::Srv(records));
    }

    /// Answers A queries for `name` with 127.0.0.1. Queries of other types get an empty answer.
    pub fn set_a(&self, name: &str) {
        self.lock().zone.insert(key(name), Entry::A);
    }

    /// Answers every query for `name` with no error and no records.
    pub fn set_empty(&self, name: &str) {
        self.lock().zone.insert(key(name), Entry::Empty);
    }

    /// Forgets `name`, so that it gets NXDOMAIN again.
    pub fn remove(&self, name: &str) {
        self.lock().zone.remove(&key(name));
    }

    /// While `silent` is true, the server counts queries but answers none.
    pub fn set_silent(&self, silent: bool) {
        self.lock().silent = silent;
    }

    /// The number of queries received so far, answered or not.
    pub fn query_count(&self) -> usize {
        self.lock().queries.len()
    }

    /// The number of queries received so far for `name` (any spelling) of type `qtype`.
    pub fn query_count_for(&self, name: &str, qtype: &str) -> usize {
        let name = key(name);
        self.lock()
            .queries
            .iter()
            .filter(|q| q.name == name && q.qtype == qtype)
            .count()
    }

    /// Every query received so far, in order.
    pub fn queries(&self) -> Vec<Query> {
        self.lock().queries.clone()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for FakeDns {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl std::fmt::Debug for FakeDns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeDns")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

/// A table key: lower case, without the trailing dot.
fn key(name: &str) -> String {
    name.strip_suffix('.').unwrap_or(name).to_ascii_lowercase()
}

/// Reads queries until the task is aborted, and answers each from the table.
async fn serve(socket: UdpSocket, state: Arc<Mutex<State>>) {
    let mut buf = vec![0u8; MAX_QUERY];
    loop {
        let Ok((len, peer)) = socket.recv_from(&mut buf).await else {
            continue;
        };
        let reply = {
            let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
            handle(&buf[..len], &mut state)
        };
        if let Some(reply) = reply {
            let _ = socket.send_to(&reply, peer).await;
        }
    }
}

/// Records one query and builds its reply, or returns `None` when there is none to send.
fn handle(bytes: &[u8], state: &mut State) -> Option<Vec<u8>> {
    let query = Packet::parse(bytes).ok()?;
    let question = query.questions.first()?;
    let name = key(&question.qname.to_string());
    let qtype = match question.qtype {
        QTYPE::TYPE(t) => format!("{t:?}"),
        other => format!("{other:?}"),
    };
    state.queries.push(Query {
        name: name.clone(),
        qtype,
    });
    if state.silent {
        return None;
    }

    let mut reply = Packet::new_reply(query.id());
    reply.set_flags(PacketFlag::AUTHORITATIVE_ANSWER | PacketFlag::RECURSION_DESIRED);
    reply.questions.push(question.clone());
    let owner = Name::new_unchecked(&name);
    match state.zone.get(&name) {
        None => *reply.rcode_mut() = RCODE::NameError,
        Some(Entry::Empty) => {}
        Some(Entry::A) => {
            if question.qtype == QTYPE::TYPE(TYPE::A) {
                reply.answers.push(ResourceRecord::new(
                    owner,
                    CLASS::IN,
                    TTL,
                    RData::A(A {
                        address: u32::from(Ipv4Addr::LOCALHOST),
                    }),
                ));
            }
        }
        Some(Entry::Srv(records)) => {
            if question.qtype == QTYPE::TYPE(TYPE::SRV) {
                for (priority, (port, target)) in (0u16..).step_by(10).zip(records) {
                    let target = target.strip_suffix('.').unwrap_or(target);
                    let target = if target.is_empty() {
                        Name::new_with_labels(&[])
                    } else {
                        Name::new_unchecked(target)
                    };
                    reply.answers.push(ResourceRecord::new(
                        owner.clone(),
                        CLASS::IN,
                        TTL,
                        RData::SRV(SRV {
                            priority,
                            weight: 1,
                            port: *port,
                            target,
                        }),
                    ));
                }
            }
        }
    }
    reply.build_bytes_vec_compressed().ok()
}
