// «Вдвоём» — сервер совместного просмотра.
// Хранит состояние каждой комнаты (какое видео, играет или пауза, на какой секунде)
// и рассылает его всем, кто в комнате. Плюс чат.

const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { Readable } = require('stream');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
// На хостинге (Render сам выставляет RENDER=true) выключаем то, что нужно только дома:
// прокси для чужих видео и раздачу фильмов из папки videos.
const ONLINE = !!process.env.RENDER || process.env.PUBLIC === '1';
const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (req, res) => res.send('ok'));

// ---------- Видео с ноутбука (папка videos) ----------
const VIDEO_DIR = path.join(__dirname, 'videos');
if (!fs.existsSync(VIDEO_DIR)) fs.mkdirSync(VIDEO_DIR);
if (!ONLINE) app.use('/videos', express.static(VIDEO_DIR)); // поддерживает перемотку (Range-запросы)

app.get('/api/videos', (req, res) => {
  if (ONLINE) return res.json([]);
  fs.readdir(VIDEO_DIR, (err, files) => {
    if (err) return res.json([]);
    res.json(files.filter(f => /\.(mp4|m4v|webm|mov)$/i.test(f)).sort());
  });
});

// ---------- Прокси для .m3u8 ----------
// Некоторые сайты не разрешают браузеру на компьютере читать плейлист напрямую (CORS).
// Тогда ноутбук скачивает его сам и отдаёт браузеру. iPhone обычно справляется и без этого.
const PRIVATE_HOST = /^(localhost|127\.|10\.|192\.168\.|169\.254\.|172\.(1[6-9]|2\d|3[01])\.|0\.|\[?::1\]?$)/i;

app.get('/proxy', async (req, res) => {
  if (ONLINE) return res.status(404).end();
  let target;
  try { target = new URL(String(req.query.url || '')); } catch { return res.status(400).end(); }
  if (!/^https?:$/.test(target.protocol) || PRIVATE_HOST.test(target.hostname)) return res.status(400).end();

  try {
    const headers = { 'user-agent': req.get('user-agent') || 'Mozilla/5.0' };
    if (req.get('range')) headers.range = req.get('range');
    const upstream = await fetch(target, { headers, redirect: 'follow' });
    const type = upstream.headers.get('content-type') || '';
    const isPlaylist = /mpegurl/i.test(type) || /\.m3u8$/i.test(target.pathname);

    if (isPlaylist) {
      const text = await upstream.text();
      const base = upstream.url;
      const wrap = u => '/proxy?url=' + encodeURIComponent(new URL(u, base).href);
      const rewritten = text.split('\n').map(line => {
        const l = line.trim();
        if (!l) return line;
        if (l.startsWith('#')) return line.replace(/URI="([^"]+)"/g, (m, u) => `URI="${wrap(u)}"`);
        return wrap(l);
      }).join('\n');
      res.set('content-type', 'application/vnd.apple.mpegurl');
      return res.status(upstream.status).send(rewritten);
    }

    res.status(upstream.status);
    const encoded = upstream.headers.get('content-encoding');
    for (const h of ['content-type', 'content-range', 'accept-ranges', 'content-length']) {
      if (h === 'content-length' && encoded) continue;
      const v = upstream.headers.get(h);
      if (v) res.set(h, v);
    }
    if (!upstream.body) return res.end();
    Readable.fromWeb(upstream.body).on('error', () => res.end()).pipe(res);
  } catch {
    if (!res.headersSent) res.status(502).end();
  }
});

// ---------- Комнаты ----------
const rooms = new Map();

function getRoom(id) {
  if (!rooms.has(id) && rooms.size >= 1000) return null; // защита от засорения
  if (!rooms.has(id)) {
    rooms.set(id, { media: null, playing: false, time: 0, updatedAt: Date.now(), messages: [], users: new Map(), cleanup: null });
  }
  return rooms.get(id);
}

function snapshot(r) {
  return { media: r.media, playing: r.playing, time: r.time, updatedAt: r.updatedAt, serverNow: Date.now() };
}

function clean(value, max) {
  return String(value ?? '').replace(/[\u0000-\u001f\u007f]/g, ' ').trim().slice(0, max);
}

function sanitizeMedia(m) {
  if (!m || typeof m !== 'object') return null;
  const title = clean(m.title, 80) || 'Видео';
  if (m.type === 'yt') {
    return /^[\w-]{11}$/.test(m.id) ? { type: 'yt', id: m.id, title } : null;
  }
  if (m.type === 'rutube') {
    if (!/^([a-f0-9]{32}|\d{1,12})$/.test(m.id)) return null;
    const p = /^[\w-]{1,64}$/.test(m.p || '') ? m.p : '';
    return { type: 'rutube', id: m.id, p, title };
  }
  if (m.type === 'vk') {
    if (!/^-?\d{1,20}$/.test(m.oid) || !/^\d{1,20}$/.test(m.vid)) return null;
    const hash = /^[a-f0-9]{1,40}$/i.test(m.hash || '') ? m.hash : '';
    return { type: 'vk', oid: m.oid, vid: m.vid, hash, title };
  }
  if (m.type === 'file' || m.type === 'hls') {
    const src = String(m.src || '').slice(0, 2000);
    if (/^https?:\/\//i.test(src) || src.startsWith('/videos/')) return { type: m.type, src, title };
  }
  return null;
}

let msgCounter = 0;
const REACTIONS = ['👌🏻', '👍🏻', '❤️', '😭', '😱', '😋', '😆'];

io.on('connection', socket => {
  let room = null, roomId = null, me = null;

  socket.on('join', (p = {}) => {
    const id = String(p.roomId || '').replace(/[^\w-]/g, '').slice(0, 40);
    if (!id || room) return;
    roomId = id;
    room = getRoom(id);
    if (!room) return;
    clearTimeout(room.cleanup);
    me = { clientId: clean(p.clientId, 40) || socket.id, name: clean(p.name, 24) || 'Гость' };
    room.users.set(socket.id, me);
    socket.join(id);
    socket.emit('init', { state: snapshot(room), messages: room.messages });
    emitPeers();
  });

  socket.on('ping-time', cb => { if (typeof cb === 'function') cb(Date.now()); });

  socket.on('load', (p = {}) => {
    if (!room) return;
    const media = sanitizeMedia(p.media);
    if (!media) return;
    Object.assign(room, { media, playing: false, time: 0, updatedAt: Date.now() });
    pushMessage({ system: true, text: `${me.name} включает «${media.title}»` });
    broadcast('load');
  });

  socket.on('control', (p = {}) => {
    if (!room || !room.media) return;
    const action = p.action;
    if (action === 'play') room.playing = true;
    else if (action === 'pause') room.playing = false;
    else if (action !== 'seek') return;
    room.time = Math.max(0, Number(p.time) || 0);
    room.updatedAt = Date.now();
    broadcast(action);
  });

  let chatTimes = [];
  socket.on('chat', (p = {}) => {
    if (!room) return;
    const now = Date.now();
    chatTimes = chatTimes.filter(t => now - t < 10000);
    if (chatTimes.length >= 15) return; // не больше 15 сообщений за 10 секунд
    chatTimes.push(now);
    const text = clean(p.text, 500);
    if (!text) return;
    const msg = { clientId: me.clientId, name: me.name, text };
    const orig = room.messages.find(x => x.id === Number(p.replyTo) && !x.system);
    if (orig) msg.replyTo = { id: orig.id, name: orig.name, text: orig.text.slice(0, 140) };
    pushMessage(msg);
  });

  // Реакция: у каждого одна на сообщение; та же ещё раз — убрать, другая — заменить
  socket.on('react', (p = {}) => {
    if (!room) return;
    const msg = room.messages.find(x => x.id === Number(p.id) && !x.system);
    if (!msg || !REACTIONS.includes(p.emoji)) return;
    msg.reactions = msg.reactions || {};
    let had = false;
    for (const [emoji, list] of Object.entries(msg.reactions)) {
      const i = list.findIndex(u => u.clientId === me.clientId);
      if (i === -1) continue;
      if (emoji === p.emoji) had = true;
      list.splice(i, 1);
      if (!list.length) delete msg.reactions[emoji];
    }
    if (!had) (msg.reactions[p.emoji] = msg.reactions[p.emoji] || []).push({ clientId: me.clientId, name: me.name });
    io.to(roomId).emit('reaction', { id: msg.id, reactions: msg.reactions });
  });

  socket.on('disconnect', () => {
    if (!room) return;
    room.users.delete(socket.id);
    emitPeers();
    if (room.users.size === 0) {
      const id = roomId;
      room.cleanup = setTimeout(() => rooms.delete(id), 6 * 60 * 60 * 1000); // пустая комната живёт 6 часов
    }
  });

  function broadcast(action) {
    io.to(roomId).emit('state', { ...snapshot(room), action, by: me.clientId, byName: me.name });
  }

  function pushMessage(msg) {
    const full = { id: ++msgCounter, at: Date.now(), ...msg };
    room.messages.push(full);
    if (room.messages.length > 150) room.messages.shift();
    io.to(roomId).emit('chat', full);
  }

  function emitPeers() {
    const unique = new Map();
    for (const u of room.users.values()) unique.set(u.clientId, u);
    io.to(roomId).emit('peers', [...unique.values()]);
  }
});

server.listen(PORT, '0.0.0.0', () => {
  if (ONLINE) return console.log(`«Вдвоём» запущено на порту ${PORT}`);
  console.log(`\n  «Вдвоём» запущено.\n  На этом ноутбуке:  http://localhost:${PORT}`);
  for (const list of Object.values(os.networkInterfaces())) {
    for (const a of list || []) {
      if (a.family === 'IPv4' && !a.internal) console.log(`  В той же Wi-Fi сети: http://${a.address}:${PORT}`);
    }
  }
  console.log('\n  Чтобы закрыть — нажми Ctrl+C\n');
});
