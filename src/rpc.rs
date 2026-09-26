// Utilities implementing the Request/Response logic on top of the messages from
// `crate::transport`.
use crate::error::Error;
use crate::match_tag::MatchTagPool;
use crate::transport::Frame;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) enum ResponseChannel {
    Response(tokio::sync::oneshot::Sender<Result<Response, Error>>),
    StreamingResponse(tokio::sync::mpsc::UnboundedSender<Result<Response, Error>>),
}

struct ResponseRouterInner {
    matchtag_pool: MatchTagPool,
    routes: HashMap<u32, ResponseChannel>,
}

pub(crate) struct ResponseRouter(Mutex<ResponseRouterInner>);

impl ResponseRouter {
    pub fn new(pool_size: u32) -> Self {
        ResponseRouter(Mutex::new(ResponseRouterInner {
            matchtag_pool: MatchTagPool::new(pool_size),
            routes: HashMap::new(),
        }))
    }
    pub fn new_route(&self, resp_route: ResponseChannel) -> u32 {
        let mut inner = self.0.lock().unwrap();
        let new_tag = inner.matchtag_pool.alloc_tag();
        inner.routes.insert(new_tag, resp_route);
        new_tag
    }
    pub fn get_route(&self, tag: u32, keep_streaming: bool) -> Option<ResponseChannel> {
        let mut inner = self.0.lock().unwrap();
        let channel = inner.routes.remove(&tag)?;
        if let ResponseChannel::StreamingResponse(sender) = &channel
            && keep_streaming
        {
            inner
                .routes
                .insert(tag, ResponseChannel::StreamingResponse(sender.clone()));
            Some(ResponseChannel::StreamingResponse(sender.clone()))
        } else {
            inner.matchtag_pool.free_tag(tag);
            Some(channel)
        }
    }
    pub fn remove_route(&self, tag: u32) {
        let mut inner = self.0.lock().unwrap();
        let _ = inner.routes.remove(&tag);
    }
}

impl ResponseChannel {
    fn signal_error(self, error: Error) {
        match self {
            ResponseChannel::Response(sender) => {
                let _ = sender.send(Err(error));
            }
            ResponseChannel::StreamingResponse(sender) => {
                let _ = sender.send(Err(error));
            }
        }
    }
    pub(crate) fn response(self, errnum: u32, topic: Vec<u8>, payload: Vec<u8>) {
        match self {
            Self::Response(sender) => {
                let _ = sender.send(Ok(Response::new(errnum, topic, Some(payload))));
            }
            Self::StreamingResponse(sender) => {
                let _ = sender.send(Ok(Response::new(errnum, topic, Some(payload))));
            }
        }
    }
}

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
