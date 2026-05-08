use crate::error::Error;
use crate::rpc::{self, IntoPayload, IntoTopic, Response};
use crate::transport::{
    TransportReceive, TransportSend, UsockTransportReceive, UsockTransportSend, usock_transport,
};

pub struct SimpleClient {
    send_half: UsockTransportSend,
    receive_half: UsockTransportReceive,
}

impl SimpleClient {
    pub async fn connect_local(url: &str) -> Result<Self, Error> {
        let (send_half, receive_half) = usock_transport(url).await?;
        Ok(Self {
            send_half,
            receive_half,
        })
    }
    pub async fn request(
        &mut self,
        nodeid: u32,
        topic: impl IntoTopic,
        payload: impl IntoPayload,
        route_upstream: bool,
    ) -> Result<(), Error> {
        let (header, frames) = rpc::request_message(nodeid, None, topic, payload, route_upstream);

        self.send_half.send_message(&header, &frames).await
    }

    async fn wait_for_response(&mut self, my_matchtag: u32) -> Result<Response, Error> {
        loop {
            let next_msg = self.receive_half.receive_message().await?;
            let (header, mut frames) = next_msg;
            if let Some((errnum, matchtag)) = header.is_response()
                && my_matchtag == matchtag
            {
                let payload = if header.has_payload() {
                    Some(frames.pop().unwrap())
                } else {
                    None
                };

                let topic = frames.pop().unwrap();

                return Ok(Response::new(errnum, topic, payload));
            }
        }
    }

    pub async fn request_with_response(
        &mut self,
        nodeid: u32,
        topic: impl IntoTopic,
        payload: impl IntoPayload,
        route_upstream: bool,
    ) -> Result<Response, Error> {
        // we always wait for our response, so we can always use the same matchtag
        let (header, frames) =
            rpc::request_message(nodeid, Some(1), topic, payload, route_upstream);

        self.send_half.send_message(&header, &frames).await?;

        self.wait_for_response(1).await
    }
}
