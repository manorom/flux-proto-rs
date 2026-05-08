// Utilities implementing the Request/Response logic on top of the messages from
// `crate::transport`.
use crate::transport::{Frame, MessageHeader, RawMessage};

pub trait IntoPayload {
    fn into_payload(self) -> Option<Frame>;
}

impl IntoPayload for () {
    fn into_payload(self) -> Option<Frame> {
        None
    }
}

impl IntoPayload for Vec<u8> {
    fn into_payload(self) -> Option<Frame> {
        Some(self)
    }
}

impl IntoPayload for Option<Vec<u8>> {
    fn into_payload(self) -> Option<Frame> {
        self
    }
}

pub trait IntoTopic {
    fn into_topic(self) -> Frame;
}

impl IntoTopic for &str {
    fn into_topic(self) -> Frame {
        let mut topic = Vec::from(self.as_bytes());
        topic.push(b'\0');
        topic
    }
}

pub(crate) fn request_message(
    nodeid: u32,
    matchtag: Option<u32>,
    topic: impl IntoTopic,
    payload: impl IntoPayload,
    upstream_flag: bool,
) -> RawMessage {
    let topic = topic.into_topic();
    let payload = payload.into_payload();
    let header = MessageHeader::new_request(nodeid, matchtag, payload.is_some(), upstream_flag);
    let mut frames = Vec::new();
    // route delimiter
    frames.push(Vec::new());
    frames.push(topic);
    if let Some(payload) = payload {
        frames.push(payload)
    }
    (header, frames)
}

#[derive(Debug)]
pub struct Response {
    errnum: u32,
    topic: Vec<u8>,
    payload: Option<Vec<u8>>,
}

impl Response {
    pub fn new(errnum: u32, topic: Vec<u8>, payload: Option<Vec<u8>>) -> Self {
        Response {
            errnum,
            topic,
            payload,
        }
    }
    pub fn errno(&self) -> u32 {
        self.errnum
    }
    pub fn topic(&self) -> &[u8] {
        self.topic.as_slice()
    }
    pub fn payload_raw(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }
}
