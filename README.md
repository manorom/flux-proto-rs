# Flux-Proto

An native Rust implementation of [FLux Resource Manager's](https://github.com/flux-framework/flux-core) message and RPC protocol, using `async/await` and the Tokio runtime. 

This crate aims to provide a Rust API to interact with a (local) Flux broker without using `libflux`.
Re-implementing the Flux message and RPC protocol in pure Rust, without relying on `libflux` and it's libev-based IO runtime, enables the use of `async/await` code with Tokio and it's rich ecosystem 


## Current Status

Connecting to a local broker via aUnix Domain Socket and basic request/reply is implemented and working.

Planned features:

* Streaming RPC Responses
* Serde integration (or another JSON serialization library)


## Usage

See the [examples/get_rank.rs](examples/get_rank.rs) example to learn how to
connect to the local Flux broker, send a request and await a reply.