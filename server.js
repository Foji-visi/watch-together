const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
app.use(express.static('public'));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// room -> { url, playing, time, ts, clients:Set }
const rooms = new Map();

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'join') {
      ws.room = String(m.room).slice(0, 40);
      if (!rooms.has(ws.room)) rooms.set(ws.room, { url: '', playing: false, time: 0, ts: Date.now(), clients: new Set() });
      const r = rooms.get(ws.room);
      r.clients.add(ws);
      const now = Date.now();
      ws.send(JSON.stringify({
        type: 'state', url: r.url, playing: r.playing,
        time: r.playing ? r.time + (now - r.ts) / 1000 : r.time,
        peers: r.clients.size
      }));
      broadcast(ws, { type: 'peers', peers: r.clients.size }, true);
      return;
    }

    const r = rooms.get(ws.room);
    if (!r) return;
    if (m.type === 'load') { r.url = m.url; r.time = 0; r.playing = false; }
    if (m.type === 'play' || m.type === 'pause' || m.type === 'seek' || m.type === 'tick') {
      r.time = m.time; r.ts = Date.now();
      if (m.type === 'play') r.playing = true;
      if (m.type === 'pause') r.playing = false;
    }
    broadcast(ws, m);
  });

  ws.on('close', () => {
    const r = rooms.get(ws.room);
    if (!r) return;
    r.clients.delete(ws);
    if (r.clients.size === 0) rooms.delete(ws.room);
    else broadcast(ws, { type: 'peers', peers: r.clients.size });
  });
});

function broadcast(from, msg, includeSelf = false) {
  const r = rooms.get(from.room);
  if (!r) return;
  const data = JSON.stringify(msg);
  for (const c of r.clients) if ((includeSelf || c !== from) && c.readyState === 1) c.send(data);
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('http://localhost:' + PORT));
