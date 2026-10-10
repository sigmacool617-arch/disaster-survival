// Сервер мультиплеера: комнаты, мир сохраняется на диск, чат.
// Запуск: npm install ws  →  node server.js
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAIN = 'Главный';
const DATA = process.env.DATA_DIR || path.join(__dirname, 'data');   /* на Render: путь постоянного диска */   // сюда сохраняются миры комнат
const HTML = fs.readFileSync(path.join(__dirname, 'game.html'));
fs.mkdirSync(DATA, { recursive: true });
const PROF = path.join(DATA, 'profiles');   /* прогресс игроков: по одному файлу на код игрока */
fs.mkdirSync(PROF, { recursive: true });

const clean = s => String(s || '').replace(/[\\\/:*?"'<>|&]/g, '').trim().slice(0, 24) || MAIN;
const fileOf = name => path.join(DATA, Buffer.from(name, 'utf8').toString('hex') + '.json');
const TORN_HP = { tornado1: 5, tornado2: 10, death: 10, chaos: 10 };
const DIS_T = { rain: 60, downpour: 60, wind: 60, tornado1: 120, tsunami1: 120, fog: 120, tornado2: 240, tsunami2: 240, death: 240, chaos: 240 };
const MOBK = { pig: { hp: 10, drop: 'pork', n: 3, sp: .9 }, chicken: { hp: 3, drop: 'chicken', n: 1, sp: 1.4 }, cow: { hp: 7, drop: 'steak', n: 8, sp: .8 } };
const ROOMS = new Map();
let nextId = 1;

// ---- Комнаты и сохранение ----
/* Хранилище миров: Upstash Redis (если заданы переменные) или папка DATA на диске */
const KV_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const KV_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const USE_KV = !!(KV_URL && KV_TOKEN);
const SAVE_MS = USE_KV ? 10000 : 3000;   /* в облаке реже, чтобы не тратить лимит запросов */
const PRELOAD = new Map();                /* миры, прочитанные из хранилища при запуске */
async function kv(cmd) {                  /* одна команда Redis через REST Upstash */
  const r = await fetch(KV_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + KV_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(cmd) });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}
function spawnMob(r) {
  const ks = Object.keys(MOBK), k = ks[Math.floor(Math.random() * ks.length)];
  const a = Math.random() * Math.PI * 2, d = Math.random() * 55, id = String(r.mobSeq++);
  r.mobs.set(id, { id, k, x: Math.cos(a) * d, z: Math.sin(a) * d, h: Math.random() * 6.28, hp: MOBK[k].hp, t: 2 });
}
function storeWrite(name, data) {
  const text = JSON.stringify(data);
  if (USE_KV) return kv(['SET', 'room:' + name, text]).catch(e => console.error('Не удалось сохранить в Upstash: ' + e.message));
  try { fs.writeFileSync(fileOf(name), text); } catch (e) { console.error('Не удалось сохранить комнату ' + name + ': ' + e.message); }
}
function saveRoom(r) {
  r.dirty = false;
  storeWrite(r.name, { seed: r.seed, blocks: [...r.blocks], furn: [...r.furn], caul: [...r.caul], cars: [...r.cars.values()].map(c => Object.assign({}, c, { seats: [null, null, null, null] })), carSeq: r.carSeq });
}
function makeRoom(name) {
  const d = PRELOAD.get(name) || {};
  const r = { name, seed: name === MAIN ? (process.env.SEED || 'lan-world') : Math.random().toString(36).slice(2, 10),
    blocks: new Map(), players: new Map(), chat: [], mobs: new Map(), mobSeq: 1, furn: new Map(), caul: new Map(), cars: new Map(), carSeq: 1,
    dis: null, disNext: Date.now() + 180000, dirty: false, thp: null, tdead: false };
  if (d.seed) r.seed = d.seed;
  for (const [k, v] of d.blocks || []) r.blocks.set(k, v);
  for (const [k, st] of d.furn || []) r.furn.set(k, st);
  for (const [k, v] of d.caul || []) r.caul.set(k, v);
  for (const c of d.cars || []) r.cars.set(c.id, Object.assign(c, { seats: [null, null, null, null] }));
  r.carSeq = d.carSeq || 1;
  for (let i = 0; i < 10; i++) spawnMob(r);
  ROOMS.set(name, r);
  saveRoom(r);
  if (USE_KV) kv(['SET', 'rooms', JSON.stringify([...ROOMS.keys()])]).catch(() => {});
  return r;
}
const getRoom = name => ROOMS.get(name) || makeRoom(name);
async function storeRead() {
  if (USE_KV) {
    const names = JSON.parse((await kv(['GET', 'rooms'])) || '[]');
    for (const name of names) { const raw = await kv(['GET', 'room:' + name]); if (raw) PRELOAD.set(name, JSON.parse(raw)); }
    return;
  }
  for (const f of fs.readdirSync(DATA)) {
    if (!f.endsWith('.json')) continue;
    try { PRELOAD.set(Buffer.from(f.slice(0, -5), 'hex').toString('utf8'), JSON.parse(fs.readFileSync(path.join(DATA, f), 'utf8'))); } catch (e) { /* битый файл пропускаем */ }
  }
}
async function loadStore() {
  await storeRead();
  for (const name of PRELOAD.keys()) getRoom(name);
  getRoom(MAIN);
  console.log(USE_KV ? 'Миры загружены из Upstash Redis' : 'Миры загружены из папки: ' + DATA);
}
setInterval(() => { for (const r of ROOMS.values()) if (r.dirty) saveRoom(r); }, SAVE_MS);
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { for (const r of ROOMS.values()) saveRoom(r); setTimeout(() => process.exit(0), 1500); });

// ---- HTTP: игра, сид комнаты, список комнат ----
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS' };
const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); return res.end(); }
  if (u.pathname === '/seed') {
    res.writeHead(200, { ...CORS, 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end(getRoom(clean(u.searchParams.get('room') || MAIN)).seed);
  }
  if (u.pathname === '/rooms') {
    res.writeHead(200, { ...CORS, 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify([...ROOMS.values()].map(r => ({ name: r.name, players: r.players.size }))));
  }
  if (u.pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(HTML);
  }
  res.writeHead(404, CORS);
  res.end();
});

const wss = new WebSocketServer({ server });
const publicOf = p => ({ id: p.id, name: p.name, x: p.x, y: p.y, z: p.z, yaw: p.yaw, hp: p.hp, hand: p.hand });
function bcast(r, msg, exceptId) {
  const s = JSON.stringify(msg);
  for (const p of r.players.values()) if (p.id !== exceptId && p.ws.readyState === 1) p.ws.send(s);
}

wss.on('connection', (ws, req) => {
  const room = getRoom(clean(new URL(req.url, 'http://x').searchParams.get('room') || MAIN));
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  const id = 'p' + nextId++;
  const me = { id, name: 'Игрок', x: 0, y: 3, z: 0, yaw: 0, ws };
  const others = [...room.players.values()].map(publicOf);
  room.players.set(id, me);
  ws.send(JSON.stringify({ t: 'welcome', id, players: others, blocks: [...room.blocks], now: Date.now(),
    dis: room.dis ? { id: room.dis.id, start: room.dis.start } : null, chat: room.chat.slice(-30), next: room.disNext, furn: [...room.furn], caul: [...room.caul], cars: [...room.cars.values()], thp: room.thp, tdead: room.tdead }));

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'hello') {
      me.name = String(m.name || 'Игрок').slice(0, 16);
      me.uid = String(m.uid || '').slice(0, 40);
      bcast(room, { t: 'join', p: publicOf(me) }, id);
    } else if (m.t === 'blk' && typeof m.k === 'string' && m.k.length < 40) {
      const v = String(m.v || '');
      room.blocks.set(m.k, v);
      room.dirty = true;
      bcast(room, { t: 'blk', k: m.k, v }, id);
    } else if (m.t === 'hit') {
      const mb = room.mobs.get(String(m.id));
      if (!mb) return;
      mb.hp -= Math.max(1, +m.dmg || 1);
      if (mb.hp <= 0) {
        room.mobs.delete(mb.id);
        bcast(room, { t: 'mobdead', id: mb.id });
        ws.send(JSON.stringify({ t: 'loot', k: MOBK[mb.k].drop, n: MOBK[mb.k].n }));
        setTimeout(() => spawnMob(room), 20000);
      }
    } else if (m.t === 'dis_set') {
      setDis(room, m.id && DIS_T[m.id] ? m.id : null);
    } else if (m.t === 'pos') {
      Object.assign(me, { x: +m.x || 0, y: +m.y || 0, z: +m.z || 0, yaw: +m.yaw || 0, hp: +m.hp || 0, hand: String(m.hand || '') });
      bcast(room, { t: 'pos', id, x: me.x, y: me.y, z: me.z, yaw: me.yaw, hp: me.hp, hand: me.hand }, id);
    } else if (m.t === 'fst' && typeof m.k === 'string' && m.k.length < 40) {
      if (m.s && typeof m.s === 'object') room.furn.set(m.k, m.s); else room.furn.delete(m.k);
      room.dirty = true;
      bcast(room, { t: 'fst', k: m.k, s: m.s || null }, id);
    } else if (m.t === 'cst' && typeof m.k === 'string' && m.k.length < 40) {
      room.caul.set(m.k, +m.v || 0);
      room.dirty = true;
      bcast(room, { t: 'cst', k: m.k, v: +m.v || 0 }, id);
    } else if (m.t === 'carnew') {
      const car = { owner: me.uid || id, id: 'c' + room.carSeq++, x: +m.x || 0, y: +m.y || 0, z: +m.z || 0, yaw: +m.yaw || 0, hp: Math.max(1, Math.min(3000, +m.hp || 3000)), seats: [null, null, null, null] };
      room.cars.set(car.id, car);
      room.dirty = true;
      bcast(room, { t: 'carnew', car });
    } else if (m.t === 'carpos') {
      const c = room.cars.get(String(m.id));
      if (!c || c.seats[0] !== id) return;
      Object.assign(c, { x: +m.x || 0, y: +m.y || 0, z: +m.z || 0, yaw: +m.yaw || 0 });
      if (m.hp !== undefined) c.hp = +m.hp || 0;
      room.dirty = true;
      if (c.hp <= 0) { room.cars.delete(c.id); bcast(room, { t: 'cardie', id: c.id }); return; }
      bcast(room, { t: 'carpos', id: c.id, x: c.x, y: c.y, z: c.z, yaw: c.yaw, hp: c.hp }, id);
    } else if (m.t === 'carseat') {
      const c = room.cars.get(String(m.id)), seat = +m.seat;
      if (!c || !(seat >= 0 && seat < 4)) return;
      if (m.on && c.seats[seat] === null && !c.seats.includes(id)) c.seats[seat] = id;
      if (!m.on && c.seats[seat] === id) c.seats[seat] = null;
      bcast(room, { t: 'carseat', id: c.id, seats: c.seats });
    } else if (m.t === 'carpick') {
      const c = room.cars.get(String(m.id));
      if (!c) return;
      if (c.owner !== (me.uid || id)) return ws.send(JSON.stringify({ t: 'note', text: 'Забрать может только тот, кто поставил машину' }));
      if (c.seats.some(x => x !== null)) return ws.send(JSON.stringify({ t: 'note', text: 'В машине кто-то есть, забрать нельзя' }));
      room.cars.delete(c.id); room.dirty = true;
      ws.send(JSON.stringify({ t: 'carpicked', id: c.id, hp: c.hp }));
      bcast(room, { t: 'cardie', id: c.id });
    } else if (m.t === 'thit') {
      if (!room.dis || room.thp == null || room.tdead) return;
      room.thp = Math.max(0, room.thp - Math.max(1, +m.dmg || 1));
      bcast(room, { t: 'thp', hp: room.thp });
      if (room.thp > 0) return;
      const did = room.dis.id;
      if (did === 'death' || did === 'chaos') {
        room.tdead = true;
        bcast(room, { t: 'tdead', on: true });
        setTimeout(() => {
          if (!room.dis || room.dis.id !== did) return;
          room.tdead = false; room.thp = TORN_HP[did];
          bcast(room, { t: 'tdead', on: false });
          bcast(room, { t: 'thp', hp: room.thp });
        }, 30000);
      } else {
        setDis(room, null);
      }
    } else if (m.t === 'chat') {
      const text = String(m.text || '').trim().slice(0, 200);
      if (!text) return;
      const line = { name: me.name, text };
      room.chat.push(line);
      if (room.chat.length > 100) room.chat.shift();
      bcast(room, { t: 'chat', name: line.name, text: line.text });
    }
  });
  ws.on('close', () => {
    room.players.delete(id);
    for (const c of room.cars.values()) if (c.seats.includes(id)) { c.seats = c.seats.map(x => x === id ? null : x); bcast(room, { t: 'carseat', id: c.id, seats: c.seats }); }
    bcast(room, { t: 'leave', id });
  });
});

// ---- Погода: один таймер на комнату ----
function pickDis() {
  const r = Math.random();
  const pool = r < .45 ? ['rain', 'downpour', 'wind'] : r < .75 ? ['tornado1', 'tsunami1', 'fog'] : ['tornado2', 'tsunami2', 'death', 'chaos'];
  return pool[Math.floor(Math.random() * pool.length)];
}
function setDis(r, id) {
  r.dis = id ? { id, start: Date.now() } : null;
  r.thp = id ? (TORN_HP[id] ?? null) : null;   /* хп торнадо на время бедствия */
  r.tdead = false;
  if (!id) r.disNext = Date.now() + 180000;    /* отсчёт 3 минуты начинается после окончания бедствия */
  bcast(r, { t: 'dis', id: r.dis ? r.dis.id : null, start: r.dis ? r.dis.start : 0, next: r.disNext, thp: r.thp });
}
setInterval(() => {
  const now = Date.now();
  for (const r of ROOMS.values()) {
    if (r.dis && now - r.dis.start >= DIS_T[r.dis.id] * 1000) setDis(r, null);
    else if (!r.dis && now >= r.disNext) setDis(r, pickDis());
  }
}, 1000);

// ---- Мобы: бродят в каждой комнате, где есть игроки ----
setInterval(() => {
  for (const r of ROOMS.values()) {
    if (!r.players.size) continue;
    for (const m of r.mobs.values()) {
      m.t -= .1;
      if (m.t <= 0) { m.t = 1.5 + Math.random() * 2.5; if (Math.random() > .35) m.h = Math.random() * 6.28; }
      const sp = MOBK[m.k].sp * .1;
      m.x += Math.sin(m.h) * sp; m.z += Math.cos(m.h) * sp;
      if (Math.hypot(m.x, m.z) > 62) m.h += Math.PI;
    }
    bcast(r, { t: 'mobs', list: [...r.mobs.values()].map(m => ({ id: m.id, k: m.k, x: m.x, z: m.z, h: m.h })) });
  }
}, 100);

// ---- Проверка подключений: зависшие и спящие клиенты удаляются ----
setInterval(() => {
  for (const r of ROOMS.values()) for (const p of r.players.values()) {
    if (!p.ws.isAlive) { p.ws.terminate(); continue; }
    p.ws.isAlive = false;
    p.ws.ping();
  }
}, 20000);

loadStore().then(() => server.listen(PORT, '0.0.0.0', () => {
  console.log('Игра: http://localhost:' + PORT);
  console.log('С телефона или другого устройства: http://<IP компьютера>:' + PORT);
})).catch(e => { console.error('Не удалось загрузить миры, сервер не запущен: ' + e.message); process.exit(1); });
