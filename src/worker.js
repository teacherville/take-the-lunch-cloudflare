// 급식을 가져가! — 우리반 대회 중계 서버 (Cloudflare Worker + Durable Object)
//
// 핵심 원칙: 이 서버는 "말 전달"만 합니다. 아무것도 저장하지 않아요.
//  - 점수·순위·이름은 전부 선생님 브라우저 메모리에만 있어요 (원래 게임과 같음).
//  - Durable Object 저장소(storage)에는 한 글자도 쓰지 않아요.
//  - 선생님이 "대회 끝내기"를 누르면 모든 연결을 끊고 storage.deleteAll()까지 호출해요.
//  - 연결이 하나도 없으면 객체는 저절로 메모리에서 사라져요 → 쌓이는 기록 0.
//  - WebSocket Hibernation API를 써서, 조용할 때는 사용 시간(duration)이 청구되지 않아요.

import { DurableObject } from "cloudflare:workers";

const ROOM_RE = /^\d{6}$/;      // 참가 코드 숫자 6개
const ID_RE = /^[a-z0-9]{6,40}$/;
const MAX_PEERS = 80;           // 한 대회 최대 학생 수
const MAX_MSG = 4096;           // 메시지 최대 크기(바이트) — 장난성 큰 메시지 차단

// 닫힘 코드 (게임 화면 코드와 맞춰야 함)
const C_TAKEN = 4001;     // 같은 코드의 대회가 이미 열려 있음
const C_HOSTGONE = 4002;  // 선생님 화면 연결이 끊김
const C_REPLACED = 4003;  // 같은 대회를 다른 창에서 다시 열어서 이 창은 쉬어요
const C_NOROOM = 4004;    // 그 코드의 대회가 없음
const C_ENDED = 4010;     // 선생님이 대회를 끝냄
const C_FULL = 4029;      // 인원 초과

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ws") {
      if (req.headers.get("Upgrade") !== "websocket") {
        return new Response("websocket only", { status: 426 });
      }
      // 다른 사이트가 이 서버를 몰래 가져다 쓰지 못하게:
      // 같은 주소(클라우드플레어)에서 연 게임 + ALLOWED_ORIGINS에 적은 주소(GitHub Pages)만 허용
      const origin = req.headers.get("Origin");
      if (origin && !originOk(origin, url.host, env.ALLOWED_ORIGINS)) {
        return new Response("forbidden", { status: 403 });
      }
      const room = url.searchParams.get("room") || "";
      if (!ROOM_RE.test(room)) return new Response("bad room", { status: 400 });
      const stub = env.ROOMS.get(env.ROOMS.idFromName("room:" + room));
      return stub.fetch(req);
    }
    // 나머지는 게임 파일(index.html) — 정적 파일 요청은 무료·무제한
    return env.ASSETS.fetch(req);
  },
};

// ALLOWED_ORIGINS 예: "https://myid.github.io, https://*.github.io"
function originOk(origin, selfHost, list) {
  let o;
  try { o = new URL(origin); } catch (e) { return false; }
  if (o.host === selfHost) return true;
  for (let rule of String(list || "").split(",")) {
    rule = rule.trim().replace(/\/+$/, "");
    if (!rule) continue;
    const m = rule.match(/^(https?):\/\/\*\.(.+)$/);
    if (m) {
      if (o.protocol === m[1] + ":" && o.hostname.endsWith("." + m[2])) return true;
    } else if (o.origin === rule) return true;
  }
  return false;
}

function refuse(code, reason) {
  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);
  server.accept();
  // 닫힘 코드는 중간 장비(학교 방화벽 등)에서 지워질 수 있어서, 이유를 메시지로 먼저 보내요
  try { server.send(JSON.stringify({ k: "bye", code })); } catch (e) {}
  server.close(code, reason);
  return new Response(null, { status: 101, webSocket: client });
}

const att = (ws) => {
  try { return ws.deserializeAttachment() || {}; } catch (e) { return {}; }
};
const live = (ws) => ws.readyState === 1 && !att(ws).gone;
const sendJ = (ws, m) => { try { ws.send(JSON.stringify(m)); } catch (e) {} };
const shut = (ws, code, reason) => {
  try { ws.serializeAttachment({ ...att(ws), gone: true }); } catch (e) {}
  if (code !== 1000) sendJ(ws, { k: "bye", code });
  try { ws.close(code, reason); } catch (e) {}
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // 30초마다 오는 "ping"에는 객체를 깨우지 않고 자동으로 "pong" 응답 (요금 절약)
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  host() { return this.ctx.getWebSockets("host").find(live) || null; }
  peer(id) { return this.ctx.getWebSockets("p:" + id).find(live) || null; }
  peers() { return this.ctx.getWebSockets("peer").filter(live); }

  async fetch(req) {
    const url = new URL(req.url);
    const role = url.searchParams.get("role");

    if (role === "host") {
      const token = url.searchParams.get("token") || "";
      if (!ID_RE.test(token)) return new Response("bad token", { status: 400 });
      const old = this.host();
      if (old) {
        // 같은 선생님 화면이 다시 붙는 경우(와이파이 끊김 등)만 교체 허용
        if (att(old).token !== token) return refuse(C_TAKEN, "taken");
        shut(old, C_REPLACED, "replaced"); // 같은 선생님이 다른 창(또는 새로고침)에서 다시 연 경우
      }
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server, ["host"]);
      server.serializeAttachment({ role: "host", token });
      sendJ(server, { k: "ready" });
      return new Response(null, { status: 101, webSocket: client });
    }

    if (role === "peer") {
      const id = url.searchParams.get("id") || "";
      if (!ID_RE.test(id)) return new Response("bad id", { status: 400 });
      const host = this.host();
      if (!host) return refuse(C_NOROOM, "no room");
      const same = this.peer(id);
      if (same) shut(same, 1000, "replaced");
      else if (this.peers().length >= MAX_PEERS) return refuse(C_FULL, "full");
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server, ["peer", "p:" + id]);
      server.serializeAttachment({ role: "peer", id });
      sendJ(host, { k: "conn", cid: id });
      sendJ(server, { k: "open" });
      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response("bad role", { status: 400 });
  }

  async webSocketMessage(ws, msg) {
    if (typeof msg !== "string" || msg.length > MAX_MSG) return;
    let m;
    try { m = JSON.parse(msg); } catch (e) { return; }
    if (!m || typeof m !== "object") return;
    const a = att(ws);
    if (a.gone) return;

    if (a.role === "host") {
      if (m.k === "send") {
        const p = this.peer(String(m.to || ""));
        if (p) sendJ(p, { k: "data", d: m.d });
      } else if (m.k === "close") {
        const p = this.peer(String(m.to || ""));
        if (p) shut(p, 1000, "closed by host");
      } else if (m.k === "destroy") {
        await this.endRoom();
      }
    } else if (a.role === "peer") {
      if (m.k === "send") {
        const h = this.host();
        if (h) sendJ(h, { k: "data", cid: a.id, d: m.d });
      }
    }
  }

  async webSocketClose(ws, code, reason) {
    await this.gone(ws);
  }

  async webSocketError(ws) {
    await this.gone(ws);
  }

  async gone(ws) {
    const a = att(ws);
    try { ws.close(1000, "bye"); } catch (e) {}
    if (a.gone) return; // 서버가 일부러 닫은 연결은 따로 처리할 것 없음
    try { ws.serializeAttachment({ ...a, gone: true }); } catch (e) {}
    if (a.role === "host") {
      // 선생님 화면이 끊김 → 학생들도 끊어 둠 (학생 화면은 잠시 뒤 다시 붙어 봄)
      for (const p of this.peers()) shut(p, C_HOSTGONE, "host gone");
      await this.cleanIfEmpty();
    } else if (a.role === "peer") {
      const h = this.host();
      if (h) sendJ(h, { k: "close", cid: a.id });
      await this.cleanIfEmpty();
    }
  }

  // 선생님이 "대회 끝내기" → 모든 연결 끊고 저장소 싹 비우기
  async endRoom() {
    for (const ws of this.ctx.getWebSockets()) {
      const a = att(ws);
      if (a.gone) continue;
      shut(ws, a.role === "host" ? 1000 : C_ENDED, "ended");
    }
    await this.wipe();
  }

  async cleanIfEmpty() {
    if (!this.ctx.getWebSockets().some(live)) await this.wipe();
  }

  async wipe() {
    // 애초에 아무것도 저장하지 않지만, 혹시 몰라 한 번 더 깨끗이 지워요.
    try { await this.ctx.storage.deleteAlarm(); } catch (e) {}
    try { await this.ctx.storage.deleteAll(); } catch (e) {}
  }
}
