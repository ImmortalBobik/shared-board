// Local rehearsal of the final check, without Docker:
// starts 3 nodes, runs N bots that draw/create/move (often the same shapes),
// stops one node mid-test and brings it back, then compares /state of all nodes.
//   node test/local-cluster.js [bots=50] [seconds=40] [--external]
// With --external it skips spawning and tests whatever runs on HOST (default localhost).

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { WebSocket } = require('ws');

const args = process.argv.slice(2).filter(a => !a.startsWith('--'));
const EXTERNAL = process.argv.includes('--external');
const BOTS = Number(args[0] || 50);
const SECONDS = Number(args[1] || 40);
const HOST = process.env.HOST || 'localhost';
const PORTS = [8001, 8002, 8003];
const SHARED = 15; // shared shapes everyone fights over

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'board-'));
const procs = {};
function startNode(i) {
  const id = `node${i + 1}`;
  const peers = PORTS.filter((_, j) => j !== i).map(p => `localhost:${p}`).join(',');
  const p = spawn(process.execPath, [path.join(__dirname, '..', 'server.js')], {
    env: { ...process.env, NODE_ID: id, PORT: String(PORTS[i]), PEERS: peers, DATA_DIR: path.join(dataDir, id) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  p.stdout.on('data', d => process.stdout.write(d));
  p.stderr.on('data', d => process.stderr.write(d));
  procs[i] = p;
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

const sentIds = { shapes: new Set(), lines: new Set() };
let sentOps = 0, recvd = 0, reconnects = 0;
const lat = [];

function bot(n) {
  let ws = null, idx = n % 3, seq = 0, stopped = false;
  const connect = () => {
    if (stopped) return;
    const sock = new WebSocket(`ws://${HOST}:${PORTS[idx % 3]}/`);
    ws = sock;
    sock.on('message', raw => {
      const m = JSON.parse(raw);
      if (m.sentAt && m.bot !== n) { lat.push(Date.now() - m.sentAt); recvd++; }
    });
    sock.on('close', () => { if (ws === sock && !stopped) { reconnects++; idx++; setTimeout(connect, 100); } });
    sock.on('error', () => {});
  };
  connect();
  const timer = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const r = Math.random(), base = { bot: n, sentAt: Date.now() };
    let op;
    if (r < 0.25) {
      const id = `b${n}-${seq++}`;
      op = { type: 'create', id, x: rnd(800), y: rnd(600), w: 40, h: 30, color: '#2457d6', ...base };
      sentIds.shapes.add(id);
    } else if (r < 0.55) {
      const id = `l${n}-${seq++}`;
      op = { type: 'line', id, points: [[rnd(800), rnd(600)], [rnd(800), rnd(600)]], color: '#333', ...base };
      sentIds.lines.add(id);
    } else {
      op = { type: 'move', id: `shared-${rnd(SHARED)}`, x: rnd(800), y: rnd(600), ...base };
    }
    ws.send(JSON.stringify(op)); sentOps++;
  }, 100);
  return () => { stopped = true; clearInterval(timer); ws && ws.close(); };
}
const rnd = n => Math.floor(Math.random() * n);

async function getState(port) {
  const r = await fetch(`http://${HOST}:${port}/state`);
  return r.text();
}

(async () => {
  if (!EXTERNAL) { PORTS.forEach((_, i) => startNode(i)); await sleep(800); }

  // seed shared shapes
  const seed = new WebSocket(`ws://${HOST}:8001/`);
  await new Promise(r => seed.on('open', r));
  for (let i = 0; i < SHARED; i++) {
    seed.send(JSON.stringify({ type: 'create', id: `shared-${i}`, x: 50 * i, y: 50, w: 40, h: 30, color: '#c27a00' }));
    sentIds.shapes.add(`shared-${i}`);
  }
  seed.close();

  const stops = [];
  for (let i = 0; i < BOTS; i++) stops.push(bot(i));
  console.log(`running ${BOTS} bots for ${SECONDS}s`);

  const killAt = SECONDS * 0.35, downFor = Math.min(30, SECONDS * 0.3);
  if (!EXTERNAL) {
    setTimeout(() => { console.log('>>> stopping node2'); procs[1].kill('SIGTERM'); }, killAt * 1000);
    setTimeout(() => { console.log('>>> restarting node2'); startNode(1); }, (killAt + downFor) * 1000);
  }
  await sleep(SECONDS * 1000);
  stops.forEach(s => s());
  await sleep(2000);

  const states = await Promise.all(PORTS.map(getState));
  const same = states.every(s => s === states[0]);
  const st = JSON.parse(states[0]);
  const haveS = new Set(st.shapes.map(s => s.id)), haveL = new Set(st.lines.map(l => l.id));
  const lostS = [...sentIds.shapes].filter(id => !haveS.has(id)).length;
  const lostL = [...sentIds.lines].filter(id => !haveL.has(id)).length;
  lat.sort((a, b) => a - b);
  const avg = lat.reduce((a, b) => a + b, 0) / (lat.length || 1);
  console.log('\n===== RESULT =====');
  console.log(`identical /state on all nodes: ${same ? 'YES' : 'NO'}  (sizes ${states.map(s => s.length).join(', ')})`);
  console.log(`ops sent: ${sentOps}, shapes ${sentIds.shapes.size} (lost ${lostS}), lines ${sentIds.lines.size} (lost ${lostL})`);
  console.log(`bot reconnects: ${reconnects}`);
  console.log(`latency to other bots: avg ${avg.toFixed(1)}ms, p50 ${lat[lat.length >> 1]}ms, p99 ${lat[Math.floor(lat.length * 0.99)]}ms over ${recvd} deliveries`);
  if (!EXTERNAL) Object.values(procs).forEach(p => p.kill('SIGTERM'));
  setTimeout(() => process.exit(same && lostS + lostL === 0 ? 0 : 1), 500);
})();
