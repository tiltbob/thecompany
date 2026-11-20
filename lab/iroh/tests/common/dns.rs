//! A minimal authoritative DNS server on 127.0.0.1 (UDP, random port) for T26. Built on
//! simple-dns 0.13 (already in the lock through n0-dns-resolver). Logs every query it gets.
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use iroh_lab::Res;
use simple_dns::rdata::{A, RData, SRV};
use simple_dns::{CLASS, Name, Packet, PacketFlag, QTYPE, RCODE, ResourceRecord, TYPE};
use tokio::net::UdpSocket;

const TTL: u32 = 300;

/// What the server answers for one owner name (lower case, no trailing dot).
#[derive(Debug, Clone)]
pub enum Owner {
    /// SRV targets: (priority, weight, port, target without trailing dot).
    Srv(Vec<(u16, u16, u16, String)>),
    A(Ipv4Addr),
    /// NOERROR with no answer, whatever the type (NODATA).
    Empty,
}

#[derive(Debug)]
pub struct FakeDns {
    pub addr: SocketAddr,
    /// (qname, qtype) of every query received, in order.
    pub queries: Arc<Mutex<Vec<(String, String)>>>,
    /// While true, queries are logged but never answered.
    pub silent: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeDns {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeDns {
    pub async fn spawn(zone: HashMap<String, Owner>) -> Res<Self> {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        let addr = socket.local_addr()?;
        let queries: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
        let silent = Arc::new(AtomicBool::new(false));
        let (q, s) = (queries.clone(), silent.clone());
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 1500];
            while let Ok((len, peer)) = socket.recv_from(&mut buf).await {
                let Ok(query) = Packet::parse(&buf[..len]) else {
                    continue;
                };
                let Some(question) = query.questions.first() else {
                    continue;
                };
                let qname = question.qname.to_string().to_ascii_lowercase();
                let qtype = format!("{:?}", question.qtype);
                if let Ok(mut v) = q.lock() {
                    v.push((qname.clone(), qtype));
                }
                if s.load(Ordering::SeqCst) {
                    continue;
                }
                if let Some(bytes) = answer(&query, &qname, question.qtype, &zone) {
                    let _ = socket.send_to(&bytes, peer).await;
                }
            }
        });
        Ok(Self {
            addr,
            queries,
            silent,
            task,
        })
    }

    pub fn count(&self) -> usize {
        self.queries.lock().map(|v| v.len()).unwrap_or(0)
    }

    pub fn log(&self) -> Vec<(String, String)> {
        self.queries.lock().map(|v| v.clone()).unwrap_or_default()
    }
}

fn answer(
    query: &Packet,
    qname: &str,
    qtype: QTYPE,
    zone: &HashMap<String, Owner>,
) -> Option<Vec<u8>> {
    let mut reply = Packet::new_reply(query.id());
    reply.set_flags(PacketFlag::AUTHORITATIVE_ANSWER | PacketFlag::RECURSION_DESIRED);
    for q in &query.questions {
        reply.questions.push(q.clone());
    }
    let owner = Name::new_unchecked(qname).into_owned();
    match zone.get(qname) {
        None => *reply.rcode_mut() = RCODE::NameError,
        Some(Owner::Empty) => {}
        Some(Owner::A(ip)) => {
            if qtype == QTYPE::TYPE(TYPE::A) {
                reply.answers.push(ResourceRecord::new(
                    owner,
                    CLASS::IN,
                    TTL,
                    RData::A(A {
                        address: u32::from(*ip),
                    }),
                ));
            }
        }
        Some(Owner::Srv(targets)) => {
            if qtype == QTYPE::TYPE(TYPE::SRV) {
                for (priority, weight, port, target) in targets {
                    reply.answers.push(ResourceRecord::new(
                        owner.clone(),
                        CLASS::IN,
                        TTL,
                        RData::SRV(SRV {
                            priority: *priority,
                            weight: *weight,
                            port: *port,
                            target: Name::new_unchecked(target).into_owned(),
                        }),
                    ));
                }
            }
        }
    }
    reply.build_bytes_vec().ok()
}
