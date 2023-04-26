use rustengan::*;
use std::collections::HashMap;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: i64 },
    AddOk,
    Read,
    ReadOk { value: i64 },
    Replicate { value: HashMap<String, i64> },
}

enum InjectedPayload {
    Replicate,
}

struct CounterNode {
    node: String,
    id: usize,
    counter: HashMap<String, i64>,
    node_ids: Vec<String>,
}

impl Node<(), Payload, InjectedPayload> for CounterNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(500));
            if let Err(_) = tx.send(Event::Injected(InjectedPayload::Replicate)) {
                break;
            }
        });
        Ok(Self {
            node: init.node_id,
            id: 1,
            counter: HashMap::new(),
            node_ids: init.node_ids,
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Injected(payload) => match payload {
                InjectedPayload::Replicate => {
                    for n in &self.node_ids {
                        Message {
                            src: self.node.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: self.counter.clone(),
                            },
                        }
                        .send(output)
                        .context("")?;
                        self.id += 1;
                    }
                }
            },
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Replicate { value } => {
                        self.counter.extend(value);
                    }
                    Payload::Read => {
                        let result = self.counter.iter().map(|(_, v)| *v).sum();
                        reply.body.payload = Payload::ReadOk { value: result };
                        reply.send(output).context("read ok")?;
                        self.id += 1;
                    }
                    Payload::Add { delta } => {
                        *self.counter.entry(self.node.clone()).or_insert(0) += delta;
                        reply.body.payload = Payload::AddOk;
                        reply.send(output).context("add ok")?;
                        self.id += 1;
                    }
                    Payload::ReadOk { .. } | Payload::AddOk => {}
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, CounterNode, _, _>(())
}
