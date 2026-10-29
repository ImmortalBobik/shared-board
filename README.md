# Shared Board — 3-node real-time whiteboard

```
docker compose up -d --build
```
Board: `http://<host>:8001` (or 8002 / 8003). Full state: `GET /state` on any port.

Live demo: http://35.232.57.100:8001 · http://35.232.57.100:8002 · http://35.232.57.100:8003

Stack: Node.js 22 + [`ws`](https://github.com/websockets/ws) (no other dependencies, no database, no sync service).
Three identical containers, each one a full peer. The board page is served by every node.

## Environment setup (how the demo server was built)

Google Cloud Compute Engine, `e2-micro` (2 shared vCPU, 1 GB RAM), Ubuntu, zone `us-central1-a`.
Any Linux box with Docker works the same way (Hetzner included).

1. **Firewall:** open TCP 8001–8003 (in Cloud Shell):
   ```bash
   gcloud compute firewall-rules create allow-board --allow=tcp:8001-8003 --source-ranges=0.0.0.0/0
   ```
2. **Docker** (on the VM):
   ```bash
   curl -fsSL https://get.docker.com | sudo sh
   sudo usermod -aG docker $USER   # then log out and back in
   ```
3. **Swap** (1 GB RAM is tight while the image builds):
   ```bash
   sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
   ```
4. **Run:**
   ```bash
   git clone <this repo> board && cd board
   docker compose up -d --build
   curl localhost:8001/health
   ```
5. **Failover check:** `docker stop node2`, wait, `docker start node2`, then compare `/state` on all three ports.
6. **Reset the board:** `docker compose down && sudo rm -rf data && docker compose up -d`.

Each node keeps its op log in `./data/nodeX` (a bind mount), so a stopped node restarts with its history and only
pulls what it missed from its peers.

Files: `server.js` (node), `public/index.html` (board UI), `docker-compose.yml` (3 nodes), `test/local-cluster.js`
(50-bot load + failover rehearsal).

## Protocol (WebSocket on 8001/8002/8003, path `/`)

Client → node, JSON per message (an array of them also works):

| action | message |
|---|---|
| create shape | `{"type":"create","id":"r1","x":10,"y":20,"w":100,"h":60,"color":"#f00"}` |
| move shape | `{"type":"move","id":"r1","x":200,"y":80}` (or `dx`/`dy`) |
| draw line | `{"type":"line","id":"l1","points":[[0,0],[50,40]],"color":"#000"}` |
| ping | `{"type":"ping"}` → `{"type":"pong"}` |
| cursor (ephemeral) | `{"type":"cursor","cid":"tab1","name":"Ivan","color":"#3D6A99","x":10,"y":20}` — relayed to every client on every node, never logged or in `/state`; `{"cid":…,"gone":true}` when a tab disconnects |

The node is lenient about naming: `action`/`op` instead of `type`, `createShape`/`move_shape`/`drawLine`,
`shapeId`, and nested `data`/`payload`/`shape` objects are all accepted. Extra fields are stored and echoed back.
If `id` is missing on create/line, the node assigns one.

Node → client: on connect `{"type":"state","shapes":[...],"lines":[...]}`, then every action from anyone is echoed
as it was sent, plus `_t` (timestamp), `_o` (origin) and `_s` (sequence number). A node holds that op once its
`/vector[_o] >= _s`; the board uses this for its "Synced to all 3 nodes" indicator.

`GET /state` → `{"shapes":[{id,...fields}],"lines":[{id,...fields}]}`, sorted by id with sorted keys, so the
three nodes can be compared byte for byte. Also `/health` and `/vector` for debugging.

## Architecture (what to say in the 5-minute defense)

**Conflicts.** The board is a CRDT. Every op gets a hybrid timestamp (wall clock in µs, always moving forward,
bumped past anything seen from peers) and the id of the node that accepted it. Each shape field (`x`, `y`, `w`, …)
is a last-writer-wins register: the higher `(timestamp, nodeId)` wins. A line is an immutable record keyed by its id.
Merging is commutative, associative and idempotent, so any node that has seen the same set of ops ends up with the
same state, whatever order the ops arrived in. When two users drag the same shape at the same moment, every node
picks the same winner, the later timestamp with node id as the tie-break.

**Replication.** Each op is identified by `(origin, seq)`. Every node keeps an op log per origin and a version
vector (`origin → highest contiguous seq`). Each node opens a connection to every peer and sends its vector; the
peer replies with everything missing, then streams every new op it applies (from any origin, so ops are also
relayed). Ops are batched per event-loop tick, so latency stays in milliseconds. Duplicates are dropped by
`seq <= have`, and a gap forces a resync. `origin` includes the process start time, so a restarted node never
reuses sequence numbers.

**Node failure.** On SIGTERM the node closes client sockets with code 1012, waits for in-flight messages,
flushes them to peers and exits. Browsers detect the close (or a missed heartbeat after 4 s) and reconnect to the
next port, re-sending anything not yet echoed back. Since the ids are the same, a re-send is idempotent. The
other two nodes keep working. When the node comes back it loads its own op log from disk (`./data/nodeX`), sends
its vector to the peers and receives only what it missed. Once its vector matches theirs, its `/state` is
identical.

**Why no leader/consensus:** writes never wait for other nodes, so latency is one hop and the cluster stays
available while a node is down. Convergence comes from the CRDT, not from coordination.

## Local rehearsal (no Docker needed)

```
npm install
node test/local-cluster.js 50 40
```
This starts 3 nodes and 50 bots, stops node2 mid-run and restarts it, then compares `/state` and counts losses.
Against a running deployment: `HOST=35.232.57.100 node test/local-cluster.js 50 60 --external`.
