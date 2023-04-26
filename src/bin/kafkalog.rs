use anyhow::Context;
use rustengan::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::io::{StdoutLock, Write};
use std::sync::mpsc::Sender;
use std::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<[usize; 2]>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

struct KafkaLogNode {
    id: usize,
    commited: RwLock<Log>,
    uncommited: RwLock<Log>,
}

struct Log {
    msgs: HashMap<String, BTreeMap<usize, usize>>,
    offsets: HashMap<String, usize>,
}

impl Log {
    fn new() -> Self {
        Self {
            msgs: Default::default(),
            offsets: Default::default(),
        }
    }
}

impl Node<(), Payload> for KafkaLogNode {
    fn from_init(
        _state: (),
        _init: rustengan::Init,
        _tx: Sender<Event<Payload>>,
    ) -> anyhow::Result<Self> {
        Ok(KafkaLogNode {
            id: 1,
            commited: RwLock::new(Log::new()),
            uncommited: RwLock::new(Log::new()),
        })
    }

    fn step(&mut self, input: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("err");
        };
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Send { key, msg } => {
                let mut ucm = self.uncommited.write().unwrap();
                if !ucm.msgs.contains_key(&key) {
                    ucm.msgs.insert(key.clone(), Default::default());
                }
                if !ucm.offsets.contains_key(&key) {
                    ucm.offsets.insert(key.clone(), 0);
                }
                let mut offset = ucm.offsets[&key];
                ucm.msgs.entry(key.clone()).or_default().insert(offset, msg);
                reply.body.payload = Payload::SendOk { offset: offset };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to init")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
                offset += 1;
                //ucm.offsets[key] = offset;
                ucm.offsets.insert(key.clone(), offset);
            }
            Payload::Poll { offsets } => {
                let mut msgs: HashMap<String, Vec<[usize; 2]>> = Default::default();
                let ucm = self.uncommited.read().unwrap();
                offsets.into_iter().for_each(|(key, req_of)| {
                    if let Some(keyMsgs) = ucm.msgs.get(&key) {
                        let offsets: Vec<usize> = keyMsgs
                            .keys()
                            .filter(|&&keyV| keyV >= req_of)
                            .copied()
                            .collect();
                        offsets.into_iter().for_each(|ofK| {
                            if let Some(msg) = keyMsgs.get(&ofK) {
                                let mgV = msgs.entry(key.clone()).or_insert(Default::default());
                                mgV.push([ofK, *msg]);
                            }
                        });
                    }
                });
                reply.body.payload = Payload::PollOk { msgs };
                reply.send(output).context("")?;
                self.id += 1;
            }
            Payload::CommitOffsets { offsets } => {
                let ucm = self.uncommited.read().unwrap();
                let mut cm = self.commited.write().unwrap();
                offsets.into_iter().for_each(|(key, reqOf)| {
                    if let Some(ucmMsgs) = ucm.msgs.get(&key) {
                        ucmMsgs.iter().for_each(|(&offset, ucmmsg)| {
                            if !offset > reqOf {
                                cm.msgs.entry(key.clone()).or_insert(Default::default());
                            }
                            //cm.msgs[&key][&offset] = *ucmmsg;
                            cm.msgs
                                .entry(key.clone())
                                .or_default()
                                .insert(offset, *ucmmsg);
                        });
                        //cm.offsets[&key] = reqOf;
                        cm.offsets.insert(key.clone(), reqOf);
                    }
                });
                reply.body.payload = Payload::CommitOffsetsOk;
                reply.send(output).context("")?;
                self.id += 1;
            }
            Payload::ListCommittedOffsets { keys } => {
                let mut offsets: HashMap<String, usize> = Default::default();
                let cm = self.commited.read().unwrap();
                keys.into_iter().for_each(|key| {
                    if let Some(offset) = cm.offsets.get(&key) {
                        offsets.insert(key, *offset);
                    }
                });
                reply.body.payload = Payload::ListCommittedOffsetsOk { offsets };
                reply.send(output).context("")?;
                self.id += 1;
            }
            Payload::PollOk { .. }
            | Payload::SendOk { .. }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KafkaLogNode, _, _>(())
}
