# ql-router

`ql-router` is a minimal authenticated relay for QL peers. It proves which QIDs belong to each live connection, looks up the recipient, and forwards complete QL records unchanged. QL payloads remain end-to-end encrypted.

```text
peer A                         router                         peer B
  |--- TCP: IK + Confirm ------->|<----- TCP: IK + Confirm -----|
  |--- TCP: Attach + proof ----->|<---- TCP: Attach + proof ----|
  |                              |                              |
  |=== UDP: [auth | QL record] =>| bind address, validate route |
  |                              |-- TCP fallback ------------->|
  |                              |                              |
  |                              |<== UDP: [auth | QL record] ==|
  |<== UDP: [auth | QL record] ==| validate sender and route    |
```

TCP carries setup, QID attachment, control messages, and session records that do not fit in one UDP payload. The router also uses TCP for egress until it has learned the recipient connection's UDP address.

UDP packets carry a transport session ID, packet number, QL record, and authentication tag. The first authenticated UDP session record binds its source address to the live TCP transport session. The address cannot change during the connection, and packets from another source are dropped.

The router authenticates the routing metadata, verifies that the sender QID is attached, then wraps the unchanged record for the recipient transport. The default UDP payload limit is 1,200 bytes, leaving 1,167 bytes for the complete QL session record after the router header and authentication tag. Larger QL records use TCP.

The router provides no delivery, retransmission, ordering, congestion control, fragmentation, or replay filtering. UDP sends are attempted once; QL peers handle end-to-end reliability and duplicate records.
