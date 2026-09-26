use crate::error::Error;
use crate::rpc::{Response, ResponseChannel, ResponseRouter};
use crate::transport::{MessageHeader, RawMessage, IntoTopic, IntoPayload, usock_transport};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task;

struct SendRequest(RawMessage, oneshot::Sender<Result<(), Error>>);

type SendQueueTx = mpsc::UnboundedSender<SendRequest>;
type SendQueueRx = mpsc::UnboundedReceiver<SendRequest>;

mod reactor_impl {
    use super::SendQueueRx;
    use crate::{
        reactor::SendRequest,
        rpc::ResponseRouter,
        transport::{TransportReceive, TransportSend, UsockTransportReceive, UsockTransportSend},
    };
    use std::sync::Arc;

    pub(crate) async fn send_task(
        mut send_queue_rx: SendQueueRx,
        mut transport_tx: UsockTransportSend,
    ) {
        while let Some(send) = send_queue_rx.recv().await {
            let SendRequest(raw_msg, result_channel) = send;
            let (header, frames) = raw_msg;
            match transport_tx.send_message(&header, &frames).await {
                Ok(()) => {
                    let _ = result_channel.send(Ok(()));
                }
                Err(e) => {
                    let _ = result_channel.send(Err(e));
                }
            }
        }
    }
    pub(crate) async fn recv_task(
        response_router: Arc<ResponseRouter>,
        mut transport_rx: UsockTransportReceive,
    ) {
        while let Ok(recv) = transport_rx.receive_message().await {
            let (header, mut frames) = recv;
            let Some((errnum, matchtag)) = header.is_response() else {
                continue;
            };

            let Some(response_channel) = response_router.get_route(matchtag, false) else {
                continue;
            };

            let payload = frames.pop().unwrap();
            let topic = frames.pop().unwrap();

            response_channel.response(errnum, topic, payload);
        }
    }
}

pub struct Reactor {
    send_queue_tx: SendQueueTx,
    response_router: Arc<ResponseRouter>, // Arc<Arc<Arc<...
    sender_task: tokio::task::JoinHandle<()>,
    receiver_task: tokio::task::JoinHandle<()>,
}

impl Reactor {
    pub async fn with_local(flux_uri: &str) -> Result<Arc<Self>, Error> {
        let (send_queue_tx, send_queue_rx) = mpsc::unbounded_channel::<SendRequest>();
        let response_router = Arc::new(ResponseRouter::new(1));
        let (transport_tx, transport_rx) = usock_transport(flux_uri).await?;

        let receiver_task = task::spawn({
            let response_router = response_router.clone();
            async move {
                reactor_impl::recv_task(response_router, transport_rx).await;
            }
        });
        let sender_task = task::spawn(async move {
            reactor_impl::send_task(send_queue_rx, transport_tx).await;
        });

        Ok(Arc::new(Self {
            send_queue_tx,
            response_router,
            sender_task,
            receiver_task,
        }))
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        self.sender_task.abort();
        self.receiver_task.abort();
    }
}

#[derive(Clone)]
pub struct FluxHandle(Arc<Reactor>);

impl FluxHandle {
    pub async fn connect_local(flux_uri: &str) -> Result<Self, Error> {
        Ok(FluxHandle(Reactor::with_local(flux_uri).await?))
    }

    pub async fn request(
        &self,
        nodeid: u32,
        topic: impl IntoTopic,
        payload: impl IntoPayload,
        route_upstream: bool,
    ) -> Result<(), Error> {
        let (result_tx, result_rx) = oneshot::channel();

        let topic = topic.into_topic();
        let payload = payload.into_payload();

        let header = MessageHeader::new_request(nodeid, None, payload.is_some(), route_upstream);
        let mut additional_frames = Vec::with_capacity(3);
        additional_frames.push(Vec::new());
        additional_frames.push(topic);
        if let Some(payload) = payload {
            additional_frames.push(payload);
        }

        self.0
            .send_queue_tx
            .send(SendRequest((header, additional_frames), result_tx))
            .map_err(|_| Error::ReactorShutdown)?;

        // One might question the wisdom of waiting here...
        result_rx.await.map_err(|_e| Error::ReactorShutdown)??;

        Ok(())
    }

    pub async fn request_with_response(
        &self,
        nodeid: u32,
        topic: impl IntoTopic,
        payload: impl IntoPayload,
        route_upstream: bool,
    ) -> Result<Response, Error> {
        let (result_tx, result_rx) = oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();

        let matchtag = self
            .0
            .response_router
            .new_route(ResponseChannel::Response(response_tx));

        let topic = topic.into_topic();
        let payload = payload.into_payload();

        let header =
            MessageHeader::new_request(nodeid, Some(matchtag), payload.is_some(), route_upstream);
        let mut additional_frames = Vec::with_capacity(3);
        additional_frames.push(Vec::new());
        additional_frames.push(topic);
        if let Some(payload) = payload {
            additional_frames.push(payload);
        }

        self.0
            .send_queue_tx
            .send(SendRequest((header, additional_frames), result_tx))
            .map_err(|_| Error::ReactorShutdown)?;

        if let Err(e) = result_rx.await.map_err(|_| Error::ReactorShutdown) {
            self.0.response_router.remove_route(matchtag);
            return Err(e);
        }

        response_rx
            .await
            .map_err(|_| Error::ReactorShutdown)
            .flatten()
    }
}
