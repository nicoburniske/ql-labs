# ql-router

`ql-router` is a minimal authenticated TCP relay for QL peers. It proves which QIDs belong to each live connection, looks up the recipient, and forwards complete QL records unchanged. QL payloads remain end-to-end encrypted.

```text
peer A                         router                         peer B
  |--- TCP: IK + Confirm ------->|<----- TCP: IK + Confirm -----|
  |--- Attach + proof ---------->|<---- Attach + proof ---------|
  |                              |                              |
  |--- authenticated QL record ->| validate sender and route    |
  |                              |--- authenticated QL record ->|
```

The transport IK handshake authenticates the router against its known peer bundle and derives independent keys for each direction. After the handshake, each length-delimited frame contains a kind, payload, and AES-GCM authentication tag. The frame length, kind, and complete payload are authenticated using an implicit per-direction nonce counter. The encrypted QL body remains opaque to the router and is also authenticated end to end by QL.

After connecting, a client attaches a peer bundle and answers a challenge from that identity. The router only accepts records whose sender QID was authenticated on the same connection. A connection may own up to 64 routes.

TCP carries setup, route attachment, and QL records using length-delimited frames. Each record is limited to 8 KiB. Per-recipient queues are bounded and apply backpressure when a recipient is slower than its senders. Records for missing or disconnected recipients are dropped; QL remains responsible for end-to-end session recovery.
