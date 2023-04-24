use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::io;
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Body {
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

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
impl EchoNode {
    pub fn step(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
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

                /*reply
                .serialize(output)
                .context("serialize response to echo")?;*/
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
                /*reply
                .serialize(output)
                .context("serialize response to echo")?;*/
                self.id += 1;
            }
            Payload::EchoOk { .. } => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
    // let mut output = serde_json::Serializer::new(stdout);
    let mut state = EchoNode { id: 0 };
    for input in inputs {
        let input = input.context("input could not be deserializer")?;
        state
            .step(input, &mut stdout)
            .context("state step failed")?;
    }
    Ok(())
}
