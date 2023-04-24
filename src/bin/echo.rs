use anyhow::Context;
use rustengan::{main_loop, Body, Message, Node};
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}

struct EchoNode {
    id: usize,
}

impl Node<Payload> for EchoNode {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init { .. } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        payload: Payload::InitOk,
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("")?;
                output.write_all(b"\n").context("write all")?;
                self.id += 1;
            }
            Payload::InitOk => {
                anyhow::bail!("init ok recv");
            }
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        payload: Payload::EchoOk { echo },
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("")?;
                output.write_all(b"\n").context("write all")?;
                self.id += 1;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop(EchoNode { id: 0 })
}
