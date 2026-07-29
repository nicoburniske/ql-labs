
prime <-> foundation-server

prime <-BLE-> desktop/link-app <-TCP/UDP-> ql-router <-TCP/UDP-> foundation-server

prime pairing

1. pair to desktop app
2. desktop app requests any permissions that it needs for prime apps
3. bootstrapping the identities of ql-router/foundation-server


pair to ql-router
- open a TCP connection
- send an IK handshake payload to the router's known identity (TCP) 
- ok -> both peers get session keys

1200 KB UDP DATAGRAM


Cake phone app <-> Passport

passport is already connected to ther router + link app

- cake phone app can send rpc stuff DIRECTLY to prime cake app
- the cake phone app, how does it know the QID of arbitrary peer

cake prime app can ask the keyos system for "who are the peers who want to talk to me".

keyos ql-server <-> cake passport app

- cake wallet passport app scans phone app
- execute the handshake
- IDENTITY IS STILL STORED ON SYSTEM LEVEL
- cake could store the QID 

all communication is encapsulated into one stream


### NEXT STEPS
- refine the router protocol. POW intitially?
- get a router online legit
- start porting all online servers to QL via router
  - collaborate with maxime
  - SERVERS
    - security server
    - envoy server
    - ngu server
- 
