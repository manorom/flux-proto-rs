use std::env;

use flux_proto::Reactor;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let flux_uri = env::var("FLUX_URI").unwrap_or("local:///run/flux/local".to_owned());
    let reactor = Reactor::run_connect_local(&flux_uri).await.unwrap();
    println!("Connected using reactor");
    let handle = reactor.handle();

    let payload: Vec<u8> = b"{\"name\":\"rank\"}\0".into();
    let response = handle
        .request_with_response(0xFFFFFFFF, "attr.get", payload, false)
        .await
        .unwrap();
    println!("{:?}", response.payload_raw());
}
