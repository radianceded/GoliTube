"use strict";

const el = (id) => document.getElementById(id);
const state = {
  user: null, authReady: false, authBusy: false, authMode: "login", identityVersion: 0,
  room: "1001", targetRoom: null, socket: null, reconnectTimer: null, retries: 0,
  received: 0, liveIDs: new Set(),
  onlineTimer: null, onlineAbort: null,
  likes: null, likesAbort: null, likeBusy: false, likesUnavailable: false,
  historyVersion: 0, historyAbort: null, historyQuery: null, cursor: "", historyIDs: new Set(),
};

function notify(message, error = false) {
  el("notice").textContent = message;
  el("notice").dataset.error = String(error);
  el("notice").hidden = !message;
}

function statusText(id, message, error = false) {
  el(id).textContent = message;
  el(id).dataset.error = String(error);
}

async function api(path, body, method = "POST") {
  const options = { credentials: "same-origin", signal: AbortSignal.timeout(8000) };
  if (body !== undefined) {
    options.method = method;
    options.headers = { "Content-Type": "application/json" };
    options.body = JSON.stringify(body);
  }
  let response;
  try {
    response = await fetch(path, options);
  } catch {
    throw new Error("暂时无法连接服务，请稍后再试。");
  }
  if (response.status === 204) return null;
  const data = await response.json();
  if (!response.ok) {
    const message = response.status === 429 ? "操作太频繁，请等待一分钟后重试。" : data.message || "请求失败，请稍后重试。";
    throw Object.assign(new Error(message), { status: response.status, code: data.code });
  }
  return data;
}

function renderIdentity() {
  const user = state.user;
  el("account-open").disabled = !state.authReady || state.authBusy;
  el("account-open").hidden = Boolean(user);
  el("logout").hidden = !user;
  el("logout").disabled = state.authBusy;
  el("account-label").textContent = user ? user.username : state.authReady ? "游客" : "确认登录状态中";
  el("composer-avatar").textContent = user ? user.username[0].toUpperCase() : "G";
  el("content").disabled = !user || state.authBusy;
  const connected = state.socket?.readyState === WebSocket.OPEN;
  el("send").disabled = !user || !connected || state.authBusy;
  el("send").textContent = !user ? "登录后发送" : !connected ? "请先进入房间" : "发送弹幕";
  renderLikes();
}

function renderLikes() {
  const button = el("room-like");
  const connected = state.socket?.readyState === WebSocket.OPEN;
  button.disabled = !connected || !state.user || !state.likes || state.authBusy || state.likeBusy;
  button.setAttribute("aria-pressed", String(Boolean(state.likes?.liked)));
  if (!connected) button.textContent = "进入房间后点赞";
  else if (state.likesUnavailable) button.textContent = "点赞暂不可用";
  else if (!state.likes) button.textContent = "点赞加载中";
  else button.textContent = (!state.user ? "登录后点赞" : state.likes.liked ? "已点赞" : "点赞") + " · " + state.likes.likes;
}

function updateLikes(data, socket) {
  if (state.socket !== socket) return;
  if (data.room_id !== state.room || !Number.isSafeInteger(data.likes) || data.likes < 0 || typeof data.liked !== "boolean") {
    throw new Error("点赞响应无效，请稍后重试。");
  }
  state.likes = data;
  state.likesUnavailable = false;
  renderLikes();
}

async function refreshLikes(socket) {
  if (state.socket !== socket || socket.readyState !== WebSocket.OPEN || state.likeBusy) return;
  state.likesAbort?.abort();
  const controller = new AbortController();
  state.likesAbort = controller;
  try {
    const params = new URLSearchParams({ room: state.room });
    const response = await fetch("/rooms/likes?" + params, {
      credentials: "same-origin", cache: "no-store",
      signal: AbortSignal.any([controller.signal, AbortSignal.timeout(4000)]),
    });
    if (!response.ok) throw new Error("likes unavailable");
    const data = await response.json();
    if (state.likesAbort === controller && !controller.signal.aborted) updateLikes(data, socket);
  } catch {
    if (state.socket === socket && state.likesAbort === controller && !controller.signal.aborted) {
      state.likes = null;
      state.likesUnavailable = true;
      renderLikes();
    }
  } finally {
    if (state.likesAbort === controller) state.likesAbort = null;
  }
}

el("room-like").addEventListener("click", async () => {
  if (el("room-like").disabled) return;
  const socket = state.socket;
  const room = state.room;
  const liked = !state.likes.liked;
  state.likeBusy = true;
  // 取消旧的读取，避免它在 PUT 完成后覆盖新状态。
  state.likesAbort?.abort();
  state.likesAbort = null;
  renderLikes();
  try {
    const data = await api("/rooms/likes?" + new URLSearchParams({ room }), { liked }, "PUT");
    updateLikes(data, socket);
  } catch (error) {
    if (state.socket === socket) {
      notify(error.message, true);
      if (error.status === 401 && !state.authBusy) {
        await refreshIdentity();
        if (state.targetRoom) connectSocket();
      }
    }
  } finally {
    state.likeBusy = false;
    renderLikes();
    if (state.socket === socket) refreshLikes(socket);
  }
});

async function refreshIdentity() {
  const version = ++state.identityVersion;
  try {
    const data = await api("/auth/me");
    if (version === state.identityVersion) state.user = data.user;
  } catch (error) {
    if (version !== state.identityVersion) return;
    state.user = null;
    if (error.status !== 401) notify("登录状态暂时无法确认。" + error.message, true);
  } finally {
    if (version === state.identityVersion) {
      state.authReady = true;
      renderIdentity();
    }
  }
}

function setAuthMode(mode) {
  state.authMode = mode;
  const register = mode === "register";
  el("account-heading").textContent = register ? "注册 GoliTube" : "登录 GoliTube";
  el("account-submit").textContent = register ? "注册并登录" : "登录";
  el("password").autocomplete = register ? "new-password" : "current-password";
  el("login-mode").setAttribute("aria-pressed", String(!register));
  el("register-mode").setAttribute("aria-pressed", String(register));
  statusText("account-status", "");
}

function openAccount() {
  if (state.user) {
    notify("当前登录账号为 " + state.user.username + "。");
    return;
  }
  el("account-dialog").showModal();
  el("username").focus();
}

el("account-open").addEventListener("click", openAccount);
el("nav-account").addEventListener("click", openAccount);
el("account-close").addEventListener("click", () => el("account-dialog").close());
el("login-mode").addEventListener("click", () => setAuthMode("login"));
el("register-mode").addEventListener("click", () => setAuthMode("register"));
el("account-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (state.authBusy || !state.authReady) return;
  const credentials = { username: el("username").value, password: el("password").value };
  if (Array.from(credentials.password).length < 8 || new TextEncoder().encode(credentials.password).length > 72) {
    statusText("account-status", "密码至少 8 个字符，且不超过 72 个 UTF-8 字节。", true);
    return;
  }
  state.authBusy = true;
  ++state.identityVersion;
  renderIdentity();
  el("account-fields").disabled = true;
  statusText("account-status", "正在处理，请稍候…");
  let registered = false;
  try {
    if (state.authMode === "register") {
      await api("/auth/register", credentials);
      registered = true;
    }
    const data = await api("/auth/login", credentials);
    state.user = data.user;
    el("password").value = "";
    el("account-dialog").close();
    notify(registered ? "注册并登录成功，可以进入房间发送弹幕了。" : "登录成功，房间连接已更新。");
    if (state.targetRoom) connectSocket();
  } catch (error) {
    if (registered) setAuthMode("login");
    statusText("account-status", (registered ? "账号已注册，请重试登录。" : "") + error.message, true);
  } finally {
    state.authBusy = false;
    el("account-fields").disabled = false;
    renderIdentity();
  }
});

el("logout").addEventListener("click", async () => {
  if (state.authBusy) return;
  state.authBusy = true;
  ++state.identityVersion;
  renderIdentity();
  try {
    await api("/auth/logout", {});
    state.user = null;
    notify("已退出登录，仍可在房间接收弹幕。");
    if (state.targetRoom) connectSocket();
  } catch (error) {
    notify("退出失败。" + error.message, true);
  } finally {
    state.authBusy = false;
    renderIdentity();
  }
});

function setConnection(mode, label) {
  el("connection-status").dataset.state = mode;
  el("connection-status").textContent = label;
  el("leave-room").disabled = !state.targetRoom;
  el("stage-join").hidden = Boolean(state.targetRoom);
  el("stage-hint").textContent = mode === "open" ? "已进入房间，发一条弹幕开始交流" : state.targetRoom ? "正在连接房间，请稍候" : "输入房间号，和朋友一起收发弹幕";
  el("chat-footer-text").textContent = mode === "open" ? "已连接 · 显示最近 200 条弹幕" : mode === "connecting" ? "连接恢复后会继续接收，可查询历史补看" : "连接房间后开始接收";
  el("chat-footer-text").parentElement.dataset.open = String(mode === "open");
  renderIdentity();
}

function closeSocket() {
  stopOnline();
  clearTimeout(state.reconnectTimer);
  state.reconnectTimer = null;
  const previous = state.socket;
  state.socket = null;
  if (previous) previous.close();
}

function stopOnline() {
  state.likesAbort?.abort();
  state.likesAbort = null;
  state.likes = null;
  state.likesUnavailable = false;
  renderLikes();
  clearTimeout(state.onlineTimer);
  state.onlineTimer = null;
  state.onlineAbort?.abort();
  state.onlineAbort = null;
  el("online-count").textContent = "在线连接数待更新";
}

async function refreshOnline(socket) {
  if (state.socket !== socket) return;
  refreshLikes(socket);
  const room = state.room;
  const controller = new AbortController();
  state.onlineAbort = controller;
  try {
    const params = new URLSearchParams({ room });
    const response = await fetch("/rooms/online?" + params, {
      signal: AbortSignal.any([controller.signal, AbortSignal.timeout(4000)]),
      cache: "no-store",
    });
    if (!response.ok) throw new Error("online unavailable");
    const data = await response.json();
    if (data.room_id !== room || !Number.isSafeInteger(data.online_connections) || data.online_connections < 0) {
      throw new Error("invalid online count");
    }
    if (state.socket === socket && !controller.signal.aborted) {
      el("online-count").textContent = "在线连接 " + data.online_connections + " · 约每 3 秒更新";
    }
  } catch {
    if (state.socket === socket && !controller.signal.aborted) {
      el("online-count").textContent = "在线统计暂不可用";
    }
  } finally {
    if (state.socket === socket && state.onlineAbort === controller && !controller.signal.aborted) {
      state.onlineAbort = null;
      state.onlineTimer = setTimeout(() => refreshOnline(socket), 3000);
    }
  }
}

function connectSocket() {
  closeSocket();
  if (!state.targetRoom) return;
  const url = new URL("/ws", location.href);
  url.protocol = location.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("room", state.targetRoom);
  const socket = new WebSocket(url);
  state.socket = socket;
  setConnection("connecting", "连接中");
  socket.addEventListener("open", () => {
    if (state.socket !== socket) return;
    state.retries = 0;
    setConnection("open", "已连接");
    refreshOnline(socket);
  });
  socket.addEventListener("message", async (event) => {
    if (state.socket !== socket) return;
    let message;
    try { message = JSON.parse(event.data); } catch { return; }
    if (message.type === "error") {
      statusText("send-status", message.message || "发送失败，请重试。", true);
      if (message.code === "unauthenticated" && !state.authBusy) {
        await refreshIdentity();
        if (state.socket === socket && state.targetRoom) connectSocket();
      }
      return;
    }
    if (message.type !== "danmaku" || message.room_id !== state.room || !message.id || typeof message.content !== "string") return;
    appendLive(message);
    if (state.user && message.user_id === state.user.id) {
      statusText("send-status", "已收到房间回传。历史记录可能稍后才可查询。");
    }
  });
  socket.addEventListener("close", () => {
    if (state.socket !== socket) return;
    stopOnline();
    state.socket = null;
    if (!state.targetRoom) return;
    const delay = Math.min(1000 * 2 ** Math.min(state.retries++, 5), 15000);
    setConnection("connecting", "等待重连");
    state.reconnectTimer = setTimeout(connectSocket, delay);
  });
}

function selectRoom(room) {
  state.room = room;
  el("room-input").value = room;
  el("room-title").textContent = "房间 " + room;
  el("stage-room").textContent = "房间 " + room;
  el("history-room").textContent = "房间 " + room + " · 已保存的消息";
  const url = new URL(location.href);
  url.searchParams.set("room", room);
  history.replaceState(null, "", url);
}

function joinRoom() {
  const room = el("room-input").value.trim() || "lobby";
  if (new TextEncoder().encode(room).length > 128) {
    notify("房间号最多 128 个 UTF-8 字节，请缩短后重试。", true);
    return;
  }
  if (room !== state.room) {
    state.received = 0;
    state.liveIDs.clear();
    el("live-list").replaceChildren();
    el("danmaku-layer").replaceChildren();
    el("chat-empty").hidden = false;
    el("stage-empty").hidden = false;
    el("live-count").textContent = "0 条";
    resetHistory();
  }
  selectRoom(room);
  state.targetRoom = room;
  state.retries = 0;
  connectSocket();
  notify("");
}

el("room-form").addEventListener("submit", (event) => { event.preventDefault(); joinRoom(); });
el("stage-join").addEventListener("click", joinRoom);
el("leave-room").addEventListener("click", () => {
  state.targetRoom = null;
  closeSocket();
  setConnection("idle", "已离开");
  statusText("send-status", "已离开房间。重新进入后可以继续接收和发送。");
});

function playbackPosition(ms) {
  return (Number(ms) / 1000).toLocaleString("zh-CN", { maximumFractionDigits: 3 }) + " 秒";
}

function messageNode(message, live) {
  const item = document.createElement("li");
  item.dataset.id = message.id;
  const own = Boolean(state.user && message.user_id === state.user.id);
  item.dataset.own = String(own);
  const leading = document.createElement("span");
  leading.className = live ? "avatar" : "play-position";
  leading.textContent = live ? own ? "我" : "G" : playbackPosition(message.position_ms);
  leading.setAttribute("aria-hidden", String(live));
  const body = document.createElement("div");
  const author = document.createElement("span");
  author.className = "message-author";
  author.textContent = message.username || (own ? state.user.username : "作者未知");
  const meta = document.createElement("span");
  meta.className = "message-meta";
  meta.textContent = live ? playbackPosition(message.position_ms) : new Date(message.created_at_ms).toLocaleString("zh-CN", { hour12: false });
  const content = document.createElement("p");
  content.className = "message-content";
  content.textContent = message.content;
  body.append(author, meta, content);
  item.append(leading, body);
  return item;
}

function appendLive(message) {
  if (state.liveIDs.has(message.id)) return;
  state.liveIDs.add(message.id);
  const scroll = el("chat-scroll");
  const nearBottom = scroll.scrollHeight - scroll.scrollTop - scroll.clientHeight < 90;
  el("chat-empty").hidden = true;
  el("stage-empty").hidden = true;
  el("live-list").append(messageNode(message, true));
  while (el("live-list").children.length > 200) {
    const first = el("live-list").firstElementChild;
    state.liveIDs.delete(first.dataset.id);
    first.remove();
  }
  state.received++;
  el("live-count").textContent = Math.min(state.received, 200) + " 条";
  if (nearBottom) scroll.scrollTop = scroll.scrollHeight;
  if (!el("show-danmaku").checked) return;
  const occupied = new Set(Array.from(el("danmaku-layer").children, (node) => Number(node.dataset.lane)));
  const lane = [0, 1, 2, 3, 4].find((index) => !occupied.has(index));
  if (lane === undefined) return; // 舞台满轨时仍保留右侧完整消息，避免文字重叠。
  const flying = document.createElement("span");
  flying.className = "flying-message";
  flying.textContent = message.content;
  flying.style.color = /^#[0-9a-f]{6}$/i.test(message.color) ? message.color : "#ffffff";
  flying.dataset.lane = String(lane);
  flying.style.top = lane * 19 + "%";
  flying.style.setProperty("--travel", el("stage").clientWidth + "px");
  el("danmaku-layer").append(flying);
  flying.addEventListener("animationend", () => flying.remove(), { once: true });
  setTimeout(() => flying.remove(), 10000);
}

el("show-danmaku").addEventListener("change", () => {
  el("danmaku-layer").hidden = !el("show-danmaku").checked;
  el("danmaku-layer").replaceChildren();
});
el("content").addEventListener("input", () => {
  el("content").setCustomValidity("");
  el("content-count").textContent = Array.from(el("content").value.trim()).length + " / 255";
});
el("content").addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    if (!el("send").disabled) el("send-form").requestSubmit();
  }
});
el("send-form").addEventListener("submit", (event) => {
  event.preventDefault();
  if (!state.user || state.authBusy) { openAccount(); return; }
  if (state.socket?.readyState !== WebSocket.OPEN) {
    statusText("send-status", "房间尚未连接，请连接后重试。", true);
    return;
  }
  const content = el("content").value.trim();
  const count = Array.from(content).length;
  if (!count || count > 255 || /[\u0000-\u001f\u007f-\u009f]/u.test(content)) {
    el("content").setCustomValidity("请输入 1～255 个字符，且不含换行或控制字符。");
    el("content").reportValidity();
    return;
  }
  const position = Math.round(Number(el("position").value) * 1000);
  if (!Number.isSafeInteger(position) || position < 0) {
    statusText("send-status", "播放位置须为有效的非负秒数。", true);
    return;
  }
  try {
    state.socket.send(JSON.stringify({ content, position_ms: position, color: el("color").value }));
    statusText("send-status", "已提交，等待房间回传。草稿已保留。");
  } catch {
    statusText("send-status", "发送失败，请检查连接后重试。草稿已保留。", true);
  }
});

function resetHistory() {
  ++state.historyVersion;
  state.historyAbort?.abort();
  state.historyAbort = null;
  state.historyQuery = null;
  state.cursor = "";
  state.historyIDs.clear();
  el("history-list").replaceChildren();
  el("history-more").hidden = true;
  el("history-more").disabled = false;
  el("history-search").disabled = false;
  statusText("history-status", "选择最长 60 秒的时间段。刚发送的弹幕可能稍后才出现在历史中。");
}

async function loadHistory(append) {
  if (append && (!state.cursor || el("history-more").disabled)) return;
  if (!append) {
    const start = Math.round(Number(el("history-start").value) * 1000);
    const end = Math.round(Number(el("history-end").value) * 1000);
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end <= start || end - start > 60000) {
      statusText("history-status", "结束位置须大于开始位置，每次最多查询 60 秒。", true);
      return;
    }
    resetHistory();
    state.historyQuery = { room: state.room, start_ms: String(start), end_ms: String(end), limit: el("history-limit").value };
  }
  const version = state.historyVersion;
  const controller = new AbortController();
  state.historyAbort = controller;
  const params = new URLSearchParams(state.historyQuery);
  if (append) params.set("cursor", state.cursor);
  el("history-search").disabled = true;
  el("history-more").disabled = true;
  statusText("history-status", "正在查询已保存的弹幕…");
  try {
    const response = await fetch("/danmaku/history?" + params, { credentials: "same-origin", signal: AbortSignal.any([controller.signal, AbortSignal.timeout(8000)]) });
    const data = await response.json();
    if (version !== state.historyVersion) return;
    if (!response.ok) throw new Error(data.message || "历史查询失败。");
    for (const message of data.items) {
      if (state.historyIDs.has(message.id) || state.historyIDs.size >= 1000) continue;
      state.historyIDs.add(message.id);
      el("history-list").append(messageNode(message, false));
    }
    state.cursor = data.next_cursor || "";
    const capped = state.historyIDs.size >= 1000;
    el("history-more").hidden = !state.cursor || capped;
    statusText("history-status", capped ? "已显示 1,000 条，请缩小时间范围继续查询。" : state.historyIDs.size ? "已加载 " + state.historyIDs.size + " 条" + (state.cursor ? "，可继续加载。" : "，当前时间段已加载完。") : "这个时间段还没有已保存的弹幕。刚发送的消息可稍后重新查询。");
  } catch (error) {
    if (version !== state.historyVersion || controller.signal.aborted) return;
    statusText("history-status", error.name === "TimeoutError" || error instanceof TypeError ? "历史查询暂时不可用，请稍后重试。" : error.message, true);
  } finally {
    if (version === state.historyVersion) {
      state.historyAbort = null;
      el("history-search").disabled = false;
      el("history-more").disabled = false;
    }
  }
}

el("history-form").addEventListener("submit", (event) => { event.preventDefault(); loadHistory(false); });
el("history-more").addEventListener("click", () => loadHistory(true));
for (const id of ["history-start", "history-end", "history-limit"]) el(id).addEventListener("input", resetHistory);
el("menu-toggle").addEventListener("click", () => {
  el("navigation").hidden = !el("navigation").hidden;
  el("menu-toggle").setAttribute("aria-expanded", String(!el("navigation").hidden));
});
el("navigation").addEventListener("click", () => {
  el("navigation").hidden = true;
  el("menu-toggle").setAttribute("aria-expanded", "false");
});
window.addEventListener("pagehide", () => {
  state.targetRoom = null;
  closeSocket();
  state.historyAbort?.abort();
});

const initialRoom = new URL(location.href).searchParams.get("room")?.trim();
if (initialRoom && new TextEncoder().encode(initialRoom).length <= 128) state.room = initialRoom;
selectRoom(state.room);
refreshIdentity();
