// Board node: one of N peers. State is a CRDT (per-field last-writer-wins
// registers for shapes, keyed immutable records for lines), so every node that
// has seen the same set of ops has the same /state regardless of arrival order.
// Ops are identified by (origin, seq) and replicated by pull-based catch-up
// plus live streaming between all peers; each node relays everything it applies.

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket, Sender } = require('ws');

const NODE_ID = process.env.NODE_ID || 'node1';
const PORT = Number(process.env.PORT || 8000);
const PEERS = (process.env.PEERS || '').split(',').map(s => s.trim()).filter(Boolean);
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
// A fresh origin per process start, so sequence numbers never get reused after a restart.
const ORIGIN = `${NODE_ID}.${Date.now().toString(36)}`;

// ---------- state ----------
const shapes = new Map(); // id -> { fields: Map(name -> {v, t, o}) }
const lines = new Map();  // id -> { data, t, o }
const log = new Map();    // origin -> [serialized op], index = seq - 1
let hlc = 0;              // hybrid logical clock (microseconds)

const have = o => (log.has(o) ? log.get(o).length : 0);
const later = (t1, o1, t2, o2) => (t1 !== t2 ? t1 > t2 : o1 > o2);
const nextTs = () => (hlc = Math.max(Date.now() * 1000, hlc + 1));

function vector() {
  const v = {};
  for (const [o, arr] of log) v[o] = arr.length;
  return v;
}

// Apply one op. Returns 'dup', 'gap' or 'ok'.
function apply(op, str, live = true) {
  const n = have(op.o);
  if (op.s <= n) return 'dup';
  if (op.s !== n + 1) return 'gap';
  if (!log.has(op.o)) log.set(op.o, []);
  log.get(op.o).push(str);
  if (op.t > hlc) hlc = op.t;

  if (op.k === 'l') {
    const cur = lines.get(op.id);
    if (!cur || later(op.t, op.o, cur.t, cur.o)) lines.set(op.id, { data: op.f, t: op.t, o: op.o });
  } else {
    let sh = shapes.get(op.id);
    if (!sh) shapes.set(op.id, (sh = { fields: new Map() }));
    for (const k of Object.keys(op.f)) {
      const cur = sh.fields.get(k);
      if (!cur || later(op.t, op.o, cur.t, cur.o)) sh.fields.set(k, { v: op.f[k], t: op.t, o: op.o });
    }
  }

  if (live) {
    persist(str);
    broadcastClients(op);
    publishPeers(str);
  }
  return 'ok';
}

// ---------- persistence (append-only op log) ----------
fs.mkdirSync(DATA_DIR, { recursive: true });
const LOG_FILE = path.join(DATA_DIR, `ops-${NODE_ID}.log`);
function loadLog() {
  if (!fs.existsSync(LOG_FILE)) return 0;
  let count = 0;
  for (const line of fs.readFileSync(LOG_FILE, 'utf8').split('\n')) {
    if (!line) continue;
    try {
      if (apply(JSON.parse(line), line, false) === 'ok') count++;
    } catch { /* torn last line after a crash */ }
  }
  return count;
}
const loaded = loadLog();
const logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
function persist(str) { logStream.write(str + '\n'); }

// ---------- canonical /state ----------
function canon(v) {
  if (Array.isArray(v)) return v.map(canon);
  if (v && typeof v === 'object') {
    const out = {};
    for (const k of Object.keys(v).sort()) out[k] = canon(v[k]);
    return out;
  }
  return v;
}
function shapeObj(id, sh, withMeta) {
  const o = { id };
  for (const k of [...sh.fields.keys()].sort()) o[k] = canon(sh.fields.get(k).v);
  if (withMeta) {
    const x = sh.fields.get('x');
    if (x) { o._t = x.t; o._o = x.o; }
  }
  return o;
}
function lineObj(id, ln, withMeta) {
  const o = { id, ...canon(ln.data) };
  if (withMeta) { o._t = ln.t; o._o = ln.o; }
  return o;
}
function snapshot(withMeta) {
  const ids = m => [...m.keys()].sort();
  return {
    shapes: ids(shapes).map(id => shapeObj(id, shapes.get(id), withMeta)),
    lines: ids(lines).map(id => lineObj(id, lines.get(id), withMeta)),
  };
}

// ---------- clients ----------
const clients = new Set();
const TYPE_CREATE = new Set(['create', 'createshape', 'addshape', 'shapecreate', 'add', 'rect', 'rectangle', 'newshape']);
const TYPE_MOVE = new Set(['move', 'moveshape', 'shapemove', 'drag', 'update', 'updateshape']);
const TYPE_LINE = new Set(['line', 'drawline', 'linedraw', 'stroke', 'draw', 'path', 'createline', 'addline']);
const META_KEYS = ['type', 'action', 'op', 'kind', 'data', 'payload', 'shape', 'line', 'id', 'shapeId', 'shape_id', 'lineId', 'line_id'];

// Frame each message once and write the same bytes to every client socket.
// Sockets are corked for the rest of the tick so many frames share one syscall.
const corked = new Set();
function uncorkAll() {
  for (const s of corked) s.uncork();
  corked.clear();
}
function broadcastClients(op) {
  const msg = { type: op.ty, id: op.id, ...op.f, _t: op.t, _o: op.o, _s: op.s };
  const frame = Buffer.concat(Sender.frame(Buffer.from(JSON.stringify(msg)), {
    fin: true, rsv1: false, opcode: 1, mask: false, readOnly: false, // opcode 1 = text frame
  }));
  for (const ws of clients) {
    if (ws.readyState !== WebSocket.OPEN) continue;
    const sock = ws._socket;
    if (sock.writableLength > 2 * 1024 * 1024) { ws.terminate(); continue; } // too slow: it reconnects
    if (!corked.has(sock)) {
      if (!corked.size) setImmediate(uncorkAll);
      sock.cork(); corked.add(sock);
    }
    sock.write(frame);
  }
}

function handleClientMessage(ws, raw) {
  let msg;
  try { msg = JSON.parse(raw); } catch { return; }
  if (Array.isArray(msg)) { for (const m of msg) handleOne(ws, m); return; }
  handleOne(ws, msg);
}

function handleOne(ws, msg) {
  if (!msg || typeof msg !== 'object') return;
  const tyRaw = String(msg.type ?? msg.action ?? msg.op ?? msg.kind ?? '');
  const ty = tyRaw.toLowerCase().replace(/[^a-z]/g, '');
  if (ty === 'ping') { ws.send(JSON.stringify({ type: 'pong', t: msg.t })); return; }
  if (ty === 'cursor') { handleCursor(ws, msg); return; }
  if (ty === 'getstate' || ty === 'state' || ty === 'sync') {
    ws.send(JSON.stringify({ type: 'state', ...snapshot(true) }));
    return;
  }

  let k;
  if (TYPE_CREATE.has(ty)) k = 'c';
  else if (TYPE_MOVE.has(ty)) k = 'm';
  else if (TYPE_LINE.has(ty)) k = 'l';
  else return;

  const nest = [msg.data, msg.payload, msg.shape, msg.line].filter(x => x && typeof x === 'object' && !Array.isArray(x));
  const p = Object.assign({}, msg, ...nest);
  let id = p.id ?? p.shapeId ?? p.shape_id ?? p.lineId ?? p.line_id;
  for (const key of META_KEYS) delete p[key];

  const s = have(ORIGIN) + 1;
  if (id === undefined || id === null || id === '') {
    if (k === 'm') return; // nothing to move
    id = `${ORIGIN}:${s}`;
  }
  id = String(id);

  // Relative moves become absolute at the receiving node.
  if (k === 'm' && p.x === undefined && p.y === undefined && (p.dx !== undefined || p.dy !== undefined)) {
    const sh = shapes.get(id);
    const cx = sh?.fields.get('x')?.v ?? 0, cy = sh?.fields.get('y')?.v ?? 0;
    p.x = Number(cx) + Number(p.dx || 0);
    p.y = Number(cy) + Number(p.dy || 0);
    delete p.dx; delete p.dy;
  }

  const op = { o: ORIGIN, s, t: nextTs(), k, ty: tyRaw, id, f: p };
  apply(op, JSON.stringify(op));
}

// ---------- presence ----------
// Cursors are ephemeral: never logged, never in /state. A node fans them out to its own
// clients and to the peers subscribed to it; peers hand them to their clients only.
function sendEphemeral(m, except) {
  const str = JSON.stringify(m);
  for (const ws of clients) {
    if (ws !== except && ws.readyState === WebSocket.OPEN && ws.bufferedAmount < 1024 * 1024) ws.send(str);
  }
}
function relayEphemeral(m, except) {
  sendEphemeral(m, except);
  const str = JSON.stringify({ type: 'eph', m });
  for (const sub of subscribers) if (sub.ws.readyState === WebSocket.OPEN) sub.ws.send(str);
}
function handleCursor(ws, msg) {
  const cid = String(msg.cid || '').slice(0, 40);
  if (!cid) return;
  ws._cid = cid;
  relayEphemeral({
    type: 'cursor', cid, name: String(msg.name || '').slice(0, 40), color: String(msg.color || '').slice(0, 16),
    x: Number(msg.x) || 0, y: Number(msg.y) || 0, hidden: !!msg.hidden,
  }, ws);
}

// ---------- peers ----------
// Subscribers are peers pulling from us; we push every applied op to them in batches.
const subscribers = new Set();
let flushScheduled = false;

function publishPeers(str) {
  if (!subscribers.size) return;
  for (const sub of subscribers) sub.q.push(str);
  if (!flushScheduled) { flushScheduled = true; setImmediate(flushPeers); }
}
function flushPeers() {
  flushScheduled = false;
  for (const sub of subscribers) sendBatches(sub);
}
function sendBatches(sub) {
  const q = sub.q;
  if (!q.length) return;
  sub.q = [];
  if (sub.ws.readyState !== WebSocket.OPEN) return;
  if (sub.ws.bufferedAmount > 64 * 1024 * 1024) { sub.ws.terminate(); return; } // it will resync
  for (let i = 0; i < q.length; i += 2000) sub.ws.send('[' + q.slice(i, i + 2000).join(',') + ']');
}

function handlePeerServer(ws) {
  let sub = null;
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type !== 'sync' || sub) return;
    // Catch-up and subscribe atomically (single thread: nothing runs in between).
    const v = msg.vec || {};
    sub = { ws, q: [] };
    for (const [o, arr] of log) {
      const from = v[o] || 0;
      for (let i = from; i < arr.length; i++) sub.q.push(arr[i]);
    }
    subscribers.add(sub);
    sendBatches(sub);
  });
  ws.on('close', () => { if (sub) subscribers.delete(sub); });
  ws.on('error', () => {});
}

const peerConns = new Map(); // peer -> ws
function connectPeer(peer) {
  if (shuttingDown) return;
  const ws = new WebSocket(`ws://${peer}/peer?from=${encodeURIComponent(NODE_ID)}`);
  peerConns.set(peer, ws);
  ws.on('open', () => {
    ws.send(JSON.stringify({ type: 'sync', from: NODE_ID, vec: vector() }));
    console.log(`[${NODE_ID}] synced with ${peer}`);
  });
  ws.on('message', raw => {
    let ops;
    try { ops = JSON.parse(raw); } catch { return; }
    if (!Array.isArray(ops)) { if (ops && ops.type === 'eph' && ops.m) sendEphemeral(ops.m); return; }
    for (const op of ops) {
      if (apply(op, JSON.stringify(op)) === 'gap') {
        console.log(`[${NODE_ID}] gap from ${peer} for ${op.o}, resyncing`);
        ws.terminate();
        return;
      }
    }
  });
  ws.on('close', () => { peerConns.delete(peer); setTimeout(() => connectPeer(peer), 300); });
  ws.on('error', () => {});
}

// ---------- http ----------
const INDEX = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'GET' && url === '/state') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(snapshot(false)));
  } else if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ node: NODE_ID, origin: ORIGIN, clients: clients.size, shapes: shapes.size, lines: lines.size, peers: [...peerConns.keys()] }));
  } else if (url === '/vector') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(vector()));
  } else if (url === '/' || url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(INDEX);
  } else {
    res.writeHead(404); res.end();
  }
});

const wssClients = new WebSocketServer({ noServer: true, perMessageDeflate: false });
const wssPeers = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 512 * 1024 * 1024 });

server.on('upgrade', (req, socket, head) => {
  if (shuttingDown) { socket.destroy(); return; }
  const isPeer = req.url.startsWith('/peer');
  const wss = isPeer ? wssPeers : wssClients;
  wss.handleUpgrade(req, socket, head, ws => {
    if (isPeer) return handlePeerServer(ws);
    ws._socket.setNoDelay(true);
    clients.add(ws);
    ws.send(JSON.stringify({ type: 'state', node: NODE_ID, ...snapshot(true) }));
    ws.on('message', raw => handleClientMessage(ws, raw));
    ws.on('close', () => {
      clients.delete(ws);
      // On shutdown the client is failing over and will announce itself on another node.
      if (ws._cid && !shuttingDown) relayEphemeral({ type: 'cursor', cid: ws._cid, gone: true });
    });
    ws.on('error', () => {});
  });
});

server.listen(PORT, () => {
  console.log(`[${NODE_ID}] listening on ${PORT}, origin ${ORIGIN}, loaded ${loaded} ops, peers: ${PEERS.join(', ') || 'none'}`);
  for (const p of PEERS) connectPeer(p);
});

// ---------- graceful shutdown ----------
let shuttingDown = false;
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[${NODE_ID}] shutting down, flushing to peers`);
  flushPeers();
  // 1012 = service restart: clients reconnect to another node right away.
  for (const ws of clients) ws.close(1012, 'node stopping');
  logStream.end();
  // Give in-flight client messages time to land, then make sure peers got everything.
  const deadline = Date.now() + 3000;
  const wait = () => {
    flushPeers();
    const pending = [...subscribers].some(s => s.ws.bufferedAmount > 0);
    if (pending && Date.now() < deadline) return setTimeout(wait, 20);
    process.exit(0);
  };
  setTimeout(wait, 300);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
