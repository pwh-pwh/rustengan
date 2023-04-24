use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io;
use std::io::StdoutLock;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub trait Node<Payload> {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()>;
}

pub fn main_loop<S, Payload>(mut state: S) -> anyhow::Result<()>
where
    Payload: DeserializeOwned,
    S: Node<Payload>,
{
    let stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>();
    // let mut output = serde_json::Serializer::new(stdout);
    for input in inputs {
        let input = input.context("input could not be deserializer")?;
        state
            .step(input, &mut stdout)
            .context("state step failed")?;
    }
    Ok(())
}
