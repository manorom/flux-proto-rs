use std::env;
use std::error::Error;

use flux_proto::Reactor;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    // To connect to our local broker: Use their Unix Domain Socket
    let flux_uri = env::var("FLUX_URI").unwrap_or("local:///run/flux/local".to_owned());

    // Creating the Flux reactor, managing the connection to the local broker
    let reactor = Reactor::run_connect_local(&flux_uri).await?;
    
    // A handle is our interface to interacting with the local Flux communcation
    // similar to `flux_t`
    let handle = reactor.handle();

    // Prepare our payload
    let payload: Vec<u8> = b"{\"name\":\"rank\"}\0".into();
    
    // Use the handle to send the request and wait for a response
    let response = handle
        .request_with_response(0xFFFFFFFF, "attr.get", payload, false)
        .await?;

    println!("{:?}", response.payload_raw());
    Ok(())
}
