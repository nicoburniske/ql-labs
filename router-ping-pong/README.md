# router-ping-pong

Two peers exchange typed ping/pong RPCs over an encrypted QLv2 session through the public router. Each has its own TCP connection and authenticates its QID to the router.

From the workspace root, with the router's public `bundle.bin`:

```sh
cargo run -p router-ping-pong --release
```

Optional arguments: `[router:port] [path/to/bundle.bin]`.

The example generates temporary identities and binds their public bundles to simulate trust saved after pairing. It then performs the post-pair IK handshake and verifies three ping/pong exchanges in each direction using `ql-runtime` and `ql-rpc`. Pairing UI and identity persistence are omitted. All peer traffic goes through the router; only the initial trusted bundles are shared locally.

After ping/pong, it streams 1 MiB and 8 MiB in each direction using the download RPC, verifies every byte against a deterministic pattern, and reports payload throughput. Data is generated and checked in memory with 4 KiB sender chunks, so disk performance is excluded. Timings include the download request, metadata, payload, verification, and stream EOF, but exclude pairing and router setup. Both peers run on this machine and all transferred bytes traverse the deployed router.

The benchmark configures each peer with a 256 KiB stream send buffer and initial/maximum receive window. Other QLv2 settings remain at their defaults. It measures this configuration on the current network path, not the router's maximum capacity.

The process exits after six verified responses and four byte transfers, or fails after 180 seconds. `main.rs` runs the peers, `rpc.rs` defines the request/response and download, and `platform.rs` adapts the runtime to Tokio.
