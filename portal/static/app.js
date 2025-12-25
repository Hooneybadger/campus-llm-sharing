'use strict';
// 부경대학교 컴퓨터인공지능공학부 AI 실습 플랫폼 — 화면 하나짜리 앱(해시 라우팅).
// 데이터는 모두 같은 출처의 /api/* (portal/app.py)에서 받는다. 모델 출력은 textContent로만 넣는다(XSS 방지).
(() => {
  document.documentElement.classList.add('js');

  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => [...r.querySelectorAll(s)];
  const store = {
    get(k) { try { return sessionStorage.getItem(k); } catch { return null; } },
    set(k, v) { try { sessionStorage.setItem(k, v); } catch { /* 저장 불가: 이번 창에서만 유지 */ } },
    del(k) { try { sessionStorage.removeItem(k); } catch { /* 무시 */ } },
  };

  function h(tag, attrs, ...kids) {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (v == null || v === false) continue;
      if (k === 'class') e.className = v;
      else if (k === 'text') e.textContent = v;
      else if (k.startsWith('on')) e.addEventListener(k.slice(2), v);
      else e.setAttribute(k, v === true ? '' : v);
    }
    for (const kid of kids.flat()) {
      if (kid == null || kid === false) continue;
      e.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
    }
    return e;
  }

  // ------------------------------------------------------------------ 상태
  const S = {
    key: store.get('pknu.key'), me: null, cfg: {}, models: [], timers: [], view: null,
    chat: { id: null, log: [], busy: false, ctrl: null, list: [] },
  };

  const TIER = { undergrad: '학부생', capstone: '캡스톤', grad: '대학원생', staff: '운영자' };
  const nf = new Intl.NumberFormat('ko-KR');
  const int = (n) => (n == null || Number.isNaN(n) ? '-' : nf.format(Math.round(n)));
  const pad = (x) => String(x).padStart(2, '0');
  function kst(tsSec, { date = false, sec = true } = {}) {
    if (!tsSec) return '-';
    const d = new Date(tsSec * 1000 + 9 * 3600 * 1000);
    const t = `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}${sec ? ':' + pad(d.getUTCSeconds()) : ''}`;
    return date ? `${pad(d.getUTCMonth() + 1)}.${pad(d.getUTCDate())} ${t}` : t;
  }
  const secs = (s) => (s == null ? '-' : `${s < 10 ? s.toFixed(2) : s.toFixed(1)} s`);
  function left(untilTs) {
    const m = Math.max(0, Math.round((untilTs - Date.now() / 1000) / 60));
    return m >= 60 ? `${Math.floor(m / 60)}시간 ${m % 60}분` : `${m}분`;
  }
  function figure(label, value, unit, of) {
    return h('div', null, h('dt', { text: label }),
      h('dd', null, value, of != null ? h('span', { class: 'of', text: ` / ${of}` }) : null, unit ? h('span', { class: 'unit', text: unit }) : null));
  }
  // 표지판: 잉크 바탕에 숫자 하나(Wayfind의 plate)
  function plate(label, value, unit, of) {
    return h('div', { class: 'plate' }, h('p', { class: 'plate__label', text: label }),
      h('p', { class: 'plate__fig' }, value, of != null ? h('span', { class: 'of', text: ` / ${of}` }) : null, unit ? h('span', { class: 'unit', text: unit }) : null));
  }

  // ------------------------------------------------------------------ 통신
  class ApiError extends Error {
    constructor(status, message, body) { super(message); this.status = status; this.body = body; }
  }
  async function api(path, opts = {}) {
    const headers = { ...(opts.headers || {}) };
    if (S.key) headers.authorization = 'Bearer ' + S.key;
    let body = opts.body;
    if (opts.json !== undefined) { headers['content-type'] = 'application/json'; body = JSON.stringify(opts.json); }
    const r = await fetch(path, { method: opts.method || 'GET', headers, body, signal: opts.signal });
    if (r.status === 401 && S.me) { logout('로그인 정보가 바뀌어 다시 로그인해야 합니다.'); throw new ApiError(401, 'unauthorized'); }
    return r;
  }
  async function getJSON(path, opts) {
    const r = await api(path, opts);
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new ApiError(r.status, (d.error && (d.error.message || d.error)) || r.statusText, d);
    return d;
  }

  // ------------------------------------------------------------------ 영역 상태: 불러오는 중, 비어 있음, 오류, 채움
  function loading(region, lines = 4) {
    region.replaceChildren(h('div', { class: 'skeleton', 'aria-hidden': 'true' }, Array.from({ length: lines }, () => h('span'))));
    region.setAttribute('aria-busy', 'true');
  }
  function empty(region, title, why, action) {
    region.removeAttribute('aria-busy');
    region.replaceChildren(h('div', { class: 'empty' }, h('p', { text: title }), why ? h('p', { text: why }) : null, action || null));
  }
  function failed(region, what, err, retry) {
    region.removeAttribute('aria-busy');
    const detail = err instanceof ApiError ? '잠시 후에 다시 시도해주세요.' : '서버에 연결할 수 없습니다. 잠시 후에 다시 시도해주세요.';
    region.replaceChildren(h('div', { class: 'failed', role: 'alert' },
      h('p', { text: what }), h('p', { text: detail }),
      retry ? h('button', { class: 'btn btn--quiet btn--s', type: 'button', onclick: retry, text: '다시 불러오기' }) : null));
  }
  function filled(region, ...nodes) { region.removeAttribute('aria-busy'); region.replaceChildren(...nodes); }

  function table(cols, rows, { stack = true, cls = '' } = {}) {
    const t = h('table', { class: 'table' + (stack ? ' table--stack' : '') + (cls ? ' ' + cls : '') },
      h('thead', null, h('tr', null, cols.map((c) => h('th', { scope: 'col', class: c.cls || null, text: c.label })))),
      h('tbody', null, rows.map((r) => h('tr', null, cols.map((c, i) => {
        const v = c.cell(r);
        return h('td', { class: c.cls || null, 'data-label': i ? c.label : null }, h('div', { class: 'cell' }, v));
      })))));
    return h('div', { class: 'table-wrap' }, t);
  }

  function copyText(text, help) {
    const ok = () => { help.dataset.tone = 'ok'; help.textContent = '복사했습니다.'; };
    const bad = () => { help.dataset.tone = 'error'; help.textContent = '복사하지 못했습니다. 직접 선택해 복사해주세요.'; };
    if (navigator.clipboard && window.isSecureContext) { navigator.clipboard.writeText(text).then(ok, bad); return; }
    const ta = h('textarea', { 'aria-hidden': 'true', class: 'visually-hidden' });
    ta.value = text; document.body.append(ta); ta.select();
    try { document.execCommand('copy') ? ok() : bad(); } catch { bad(); }
    ta.remove();
  }

  function every(ms, fn) { S.timers.push(setInterval(() => { if (!document.hidden) fn(); }, ms)); }
  function clearTimers() { S.timers.forEach(clearInterval); S.timers = []; }

  // ------------------------------------------------------------------ 로그인·셸
  async function loadMe() { S.me = await getJSON('/api/me'); return S.me; }

  function showChrome() {
    const on = !!S.me;
    document.body.classList.toggle('is-auth', on);
    $('#who-id').textContent = on ? S.me.user_id : '';
    $('#who-tier').textContent = on ? TIER[S.me.tier] || S.me.tier : '';
    $$('[data-staff-only]').forEach((e) => { e.hidden = !(on && S.me.tier === 'staff'); });
  }

  function logout(msg) {
    S.key = null; S.me = null; S.chat.log = []; S.chat.id = null; S.chat.list = []; store.del('pknu.key'); store.del('pknu.chat.id');
    if (S.chat.ctrl) S.chat.ctrl.abort();
    showChrome();
    location.hash = '#/login';
    route();
    if (msg) { const help = $('#login-help'); help.dataset.tone = 'error'; help.textContent = msg; }
  }

  $('#login-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const input = $('#login-key'), help = $('#login-help'), btn = e.submitter || $('#login-form .btn');
    const key = input.value.trim();
    if (!key) {
      input.setAttribute('aria-invalid', 'true'); help.dataset.tone = 'error';
      help.textContent = 'API 키를 입력해주세요.'; input.focus(); return;
    }
    btn.disabled = true; btn.setAttribute('aria-busy', 'true'); btn.textContent = '확인 중';
    S.key = key;
    try {
      await loadMe();
      store.set('pknu.key', key);
      input.value = ''; input.removeAttribute('aria-invalid'); delete help.dataset.tone;
      help.textContent = '정보전산원에서 발급받은 sk-pknu로 시작하는 키입니다.';
      showChrome();
      location.hash = '#/chat';
      route();
    } catch (err) {
      S.key = null; S.me = null;
      input.setAttribute('aria-invalid', 'true'); help.dataset.tone = 'error';
      help.textContent = err instanceof ApiError && err.status === 401
        ? '로그인할 수 없는 키입니다. 키를 다시 확인해주세요.'
        : '서버에 연결할 수 없습니다. 잠시 후에 다시 시도해주세요.';
      input.focus();
    } finally {
      btn.disabled = false; btn.removeAttribute('aria-busy'); btn.textContent = '로그인';
    }
  });
  $('#login-key').addEventListener('input', (e) => e.target.removeAttribute('aria-invalid'));
  $('[data-logout]').addEventListener('click', () => logout());

  const bar = $('.bar'), menuBtn = $('.menu-btn');
  menuBtn.addEventListener('click', () => {
    const open = !bar.hasAttribute('data-open');
    bar.toggleAttribute('data-open', open); menuBtn.setAttribute('aria-expanded', String(open));
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && bar.hasAttribute('data-open')) { bar.removeAttribute('data-open'); menuBtn.setAttribute('aria-expanded', 'false'); menuBtn.focus(); }
  });

  // ------------------------------------------------------------------ 라우터
  const TITLES = { login: '로그인', chat: '실습', models: '모델 상태', usage: '사용량', docs: 'API 안내', ops: '운영 현황' };
  const VIEWS = {};
  function route() {
    clearTimers();
    bar.removeAttribute('data-open'); menuBtn.setAttribute('aria-expanded', 'false');
    let name = (location.hash.match(/^#\/(\w+)/) || [])[1] || 'chat';
    if (!S.me) name = 'login';
    else if (name === 'login' || !VIEWS[name] || (name === 'ops' && S.me.tier !== 'staff')) name = 'chat';
    const first = S.view === null;
    S.view = name;
    $$('[data-view]').forEach((v) => { v.hidden = v.dataset.view !== name; });
    $$('.nav a').forEach((a) => (a.dataset.route === name ? a.setAttribute('aria-current', 'page') : a.removeAttribute('aria-current')));
    document.title = `${TITLES[name]} | AI 실습 플랫폼`;
    if (name !== 'login') VIEWS[name]();
    if (!first) { const hd = $(`[data-view="${name}"] h1`); if (hd) { hd.setAttribute('tabindex', '-1'); hd.focus({ preventScroll: true }); window.scrollTo(0, 0); } }
  }
  window.addEventListener('hashchange', route);

  // ------------------------------------------------------------------ 모델 상태(공통): 부하 = 토큰 예산 대비 처리 중인 양
  function loadOf(m) {
    if (m.token_budget) return m.inflight_tokens / m.token_budget;
    if (m.max_inflight) return m.inflight / m.max_inflight;
    return 0;
  }
  function stateOf(m) {
    if (!m.available || m.backends_healthy === 0) return ['down', '중단'];
    if (m.queue > 0 || loadOf(m) >= 0.8) return ['busy', '혼잡'];
    return ['ok', '사용 가능'];
  }
  function loadBar(m) {
    const v = Math.min(1, Math.max(0, loadOf(m)));
    const fill = h('span', { class: 'load__fill' });
    fill.style.setProperty('--v', String(v));
    return h('div', { class: 'load', 'data-high': v >= 0.8 || null },
      h('span', { class: 'load__track', role: 'img', 'aria-label': `부하 ${Math.round(v * 100)}%` }, fill),
      h('span', { class: 'load__val num', text: `${Math.round(v * 100)}%` }));
  }
  function boardTable(rows, extra = [], { ids = true } = {}) {
    return h('div', { class: 'board' }, table([
      { label: '모델', cell: (m) => [h('span', { class: 'cell-title', text: m.title }), ids ? h('span', { class: 'cell-sub mono', text: m.id }) : null] },
      { label: '상태', cell: (m) => { const [k, t] = stateOf(m); return h('span', { class: 'state', 'data-state': k, text: t }); } },
      { label: '처리 중', cls: 'num', cell: (m) => h('span', { class: 'fig', text: int(m.inflight) }) },
      { label: '대기', cls: 'num', cell: (m) => [h('span', { class: 'fig', text: int(m.queue) }), h('span', { class: 'unit', text: ` / ${int(m.queue_max)}` })] },
      { label: '부하', cell: loadBar },
      { label: '문맥 길이', cls: 'num', cell: (m) => [int(m.max_model_len), h('span', { class: 'unit', text: ' 토큰' })] },
      ...extra,
    ], rows));
  }
  async function refreshModels() {
    const d = await getJSON('/api/models');
    S.models = d.models;
    return d;
  }

  // ------------------------------------------------------------------ 모델 화면
  VIEWS.models = () => {
    const region = $('#models-board'), at = $('#models-at');
    const load = async (first) => {
      if (first) loading(region, 6);
      try {
        const d = await refreshModels();
        if (!d.models.length) { empty(region, '등록된 모델이 없습니다.', '운영자가 모델을 등록하면 여기에 나타납니다.'); return; }
        filled(region, boardTable(d.models));
        at.textContent = `5초마다 갱신, 마지막 ${kst(d.at)} KST`;
      } catch (err) { if (first || !region.querySelector('table')) failed(region, '모델 상태를 불러오지 못했습니다.', err, () => load(true)); }
    };
    load(true);
    every(5000, () => load(false));
  };

  // ------------------------------------------------------------------ 실습(채팅)
  const chat = {
    log: $('#chat-log'), empty: $('#chat-empty'), form: $('#chat-form'), input: $('#chat-input'), help: $('#chat-help'),
    model: $('#chat-model'), system: $('#chat-system'), send: $('[data-send]'), stop: $('[data-stop]'),
    history: $('#chat-history'), note: $('#history-note'),
  };
  const EXAMPLE = '파이썬으로 리스트에서 중복을 없애면서 순서를 지키는 함수를 작성하고, 시간 복잡도를 설명해 줘.';

  // 대화 기록은 포털 서버에 사용자별로 저장한다(다른 PC에서 로그인해도 이어 보기). 지금 열린 대화의 ID만 이 창에 둔다.
  const newId = () => (crypto.randomUUID ? crypto.randomUUID() : `c-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`);
  const stamp = (t) => new Date(t * 1000).toLocaleString('ko-KR', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false });

  async function saveChat() {
    if (!S.chat.log.some((m) => m.role === 'user')) return;
    if (!S.chat.id) S.chat.id = newId();
    store.set('pknu.chat.id', S.chat.id);
    const messages = S.chat.log.map(({ streaming, ...m }) => m);
    try {
      await getJSON(`/api/chats/${S.chat.id}`, { method: 'PUT', json: { model: chat.model.value, system: chat.system.value, messages } });
      chat.note.textContent = '';
      await loadHistory();
    } catch (err) {
      if (!(err instanceof ApiError && err.status === 401)) chat.note.textContent = '대화를 저장하지 못했습니다. 다음 답을 받을 때 다시 저장합니다.';
    }
  }
  async function loadHistory() {
    try { S.chat.list = (await getJSON('/api/chats')).chats || []; } catch { return; }
    renderHistory();
  }
  function renderHistory() {
    const rows = S.chat.list.slice(0, 20).map((c) => h('li', { class: 'history__item' },
      h('button', { class: 'history__open', type: 'button', 'aria-current': c.id === S.chat.id ? 'true' : null, onclick: () => openChat(c.id) },
        h('span', { class: 'history__title', text: c.title }), h('span', { class: 'history__at', text: stamp(c.updated) })),
      h('button', { class: 'btn btn--quiet btn--s history__del', type: 'button', text: '삭제', 'aria-label': `${c.title} 삭제`, onclick: () => removeChat(c.id) })));
    chat.history.replaceChildren(...rows);
    if (!rows.length && !chat.note.textContent) chat.note.textContent = '보낸 대화가 여기에 쌓입니다.';
    else if (rows.length && chat.note.textContent === '보낸 대화가 여기에 쌓입니다.') chat.note.textContent = '';
  }
  async function openChat(id) {
    if (S.chat.busy) return;
    try {
      const c = await getJSON(`/api/chats/${id}`);
      S.chat.id = c.id; S.chat.log = c.messages || [];
      chat.system.value = c.system || '';
      if (c.model && S.models.some((m) => m.id === c.model)) chat.model.value = c.model;
      store.set('pknu.chat.id', c.id);
      renderLog(); renderHistory(); toBottom();
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) { store.del('pknu.chat.id'); loadHistory(); }
    }
  }
  function newChat() {
    if (S.chat.busy && S.chat.ctrl) S.chat.ctrl.abort();
    S.chat.id = null; S.chat.log = []; store.del('pknu.chat.id');
    renderLog(); renderHistory(); chat.input.focus();
  }
  async function removeChat(id) {
    try { await getJSON(`/api/chats/${id}`, { method: 'DELETE' }); } catch { return; }
    if (id === S.chat.id) newChat();
    loadHistory();
  }


  // 모델 답의 마크다운 일부(코드 블록, 인라인 코드, 굵게, 제목, 인용)만 DOM 노드로 그린다. HTML은 해석하지 않는다.
  function inline(text) {
    const out = [];
    const re = /(`[^`\n]+`|\*\*[^*\n]+\*\*)/g;
    let last = 0, mt;
    while ((mt = re.exec(text))) {
      if (mt.index > last) out.push(text.slice(last, mt.index));
      const t = mt[0];
      out.push(t[0] === '`' ? h('code', { class: 'md-c', text: t.slice(1, -1) }) : h('strong', { text: t.slice(2, -2) }));
      last = mt.index + t.length;
    }
    if (last < text.length) out.push(text.slice(last));
    return out;
  }
  function md(src) {
    const nodes = [];
    const parts = src.split(/^```[^\n]*\n?/m);
    parts.forEach((part, i) => {
      if (i % 2 === 1) { nodes.push(h('pre', { class: 'code md-pre' }, h('code', { text: part.replace(/\n$/, '') }))); return; }
      let para = [];
      const flush = () => { if (para.length) { nodes.push(h('p', null, inline(para.join('\n')))); para = []; } };
      for (const line of part.split('\n')) {
        const hd = line.match(/^#{1,6}\s+(.*)$/), q = line.match(/^>\s?(.*)$/);
        if (!line.trim()) flush();
        else if (/^\s*(-{3,}|\*{3,})\s*$/.test(line)) { flush(); nodes.push(h('hr')); }
        else if (hd) { flush(); nodes.push(h('p', { class: 'md-h' }, inline(hd[1]))); }
        else if (q) { flush(); nodes.push(h('p', { class: 'md-q' }, inline(q[1]))); }
        else para.push(line);
      }
      flush();
    });
    return nodes;
  }

  function msgNode(m) {
    const li = h('li', { class: `msg msg--${m.role}` });
    const who = m.role === 'user' ? '나' : m.role === 'error' ? '요청 실패' : (S.models.find((x) => x.id === m.model) || {}).title || m.model;
    li.append(h('span', { class: 'msg__who', text: who }));
    if (m.reasoning) {
      li.append(h('details', { class: 'msg__reason' }, h('summary', { text: '추론 과정 보기' }), h('p', { text: m.reasoning })));
    }
    const body = m.role === 'assistant' ? h('div', { class: 'msg__body md' }, md(m.content || '')) : h('div', { class: 'msg__body', text: m.content || '' });
    if (m.streaming) (body.lastElementChild || body).append(h('span', { class: 'cursor', 'aria-hidden': 'true' }));
    li.append(body);
    if (m.meta) {
      const mt = m.meta;
      li.append(h('p', { class: 'msg__meta' },
        h('span', null, '첫 응답 ', h('span', { class: 'num', text: secs(mt.ttft) })),
        mt.out != null ? h('span', null, '출력 ', h('span', { class: 'num', text: int(mt.out) }), ' 토큰') : null,
        mt.tps ? h('span', null, '속도 ', h('span', { class: 'num', text: mt.tps.toFixed(1) }), ' 토큰/s') : null,
        h('span', null, '전체 ', h('span', { class: 'num', text: secs(mt.total) })),
        mt.cut ? h('span', { text: '최대 출력 토큰에서 멈췄습니다' }) : null));
    }
    if (m.role === 'error' && m.retry) {
      li.append(h('div', { class: 'msg__actions' }, h('button', { class: 'btn btn--quiet btn--s', type: 'button', text: '다시 시도', onclick: () => resend(m) })));
    }
    return li;
  }
  function renderLog() {
    chat.log.replaceChildren(...S.chat.log.map(msgNode));
    chat.empty.hidden = S.chat.log.length > 0;
  }
  const atBottom = () => window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 160;
  const toBottom = () => window.scrollTo(0, document.documentElement.scrollHeight);
  function updateLast() {
    const m = S.chat.log[S.chat.log.length - 1];
    const node = chat.log.lastElementChild;
    const follow = atBottom();
    if (m && node) node.replaceWith(msgNode(m));
    if (follow) toBottom();
  }

  function modelOptions() {
    const keep = chat.model.value || store.get('pknu.model');
    chat.model.replaceChildren(...S.models.map((m) => {
      const [, t] = stateOf(m);
      return h('option', { value: m.id, text: t === '사용 가능' ? m.title : `${m.title}  (${t})` });
    }));
    const pick = S.models.find((m) => m.id === keep && stateOf(m)[0] !== 'down') || S.models.find((m) => stateOf(m)[0] !== 'down') || S.models[0];
    if (pick) chat.model.value = pick.id;
    describeModel();
  }
  function describeModel() {
    const m = S.models.find((x) => x.id === chat.model.value);
    const state = $('#chat-state');
    state.textContent = '';
    if (!m) return;
  }
  chat.model.addEventListener('change', () => { store.set('pknu.model', chat.model.value); describeModel(); });

  function explain(status, d) {
    const code = d && d.error && d.error.code;
    if (status === 400 && code === 'context_length') return ['대화가 이 모델이 처리할 수 있는 길이를 넘었습니다. 대화를 지운 뒤 다시 시도해주세요.', false];
    if (status === 400) return ['요청을 처리할 수 없습니다. 입력을 확인해주세요.', false];
    if (status === 404) return ['지금은 이 모델을 사용할 수 없습니다. 다른 모델을 선택해주세요.', false];
    if (status === 429 && code === 'user_concurrency') return ['이전 요청이 아직 처리 중입니다. 잠시 후에 다시 시도해주세요.', true];
    if (status === 429) return ['요청이 너무 많습니다. 잠시 후에 다시 시도해주세요.', true];
    if (status === 503 && code === 'no_backend') return ['지금은 이 모델을 사용할 수 없습니다. 잠시 후에 다시 시도해주세요.', true];
    if (status === 503) return ['사용자가 많아 요청을 처리하지 못했습니다. 잠시 후에 다시 시도해주세요.', true];
    return ['요청을 처리하지 못했습니다. 잠시 후에 다시 시도해주세요.', true];
  }

  function setBusy(on) {
    S.chat.busy = on;
    chat.send.disabled = on; chat.send.toggleAttribute('aria-busy', on);
    chat.send.textContent = on ? '답 받는 중' : '보내기';
    chat.stop.hidden = !on;
  }

  async function ask() {
    const model = chat.model.value;
    const sys = chat.system.value.trim();
    const messages = [
      ...(sys ? [{ role: 'system', content: sys }] : []),
      ...S.chat.log.filter((m) => m.role === 'user' || (m.role === 'assistant' && m.content)).map((m) => ({ role: m.role, content: m.content })),
    ];
    const a = { role: 'assistant', model, content: '', reasoning: '', streaming: true };
    S.chat.log.push(a); renderLog(); toBottom();
    const ctrl = new AbortController(); S.chat.ctrl = ctrl;
    setBusy(true);
    const t0 = performance.now();
    let usage = null, finish = null, frame = 0;
    const paint = () => { frame = 0; updateLast(); };
    try {
      const r = await api('/api/chat', { method: 'POST', signal: ctrl.signal,
        json: { model, messages, stream: true, stream_options: { include_usage: true } } });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        const [text, retry] = explain(r.status, d);
        S.chat.log.pop();
        S.chat.log.push({ role: 'error', content: text, retry });
        return;
      }
      const reader = r.body.getReader(), dec = new TextDecoder();
      let buf = '';
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        const events = buf.split('\n\n'); buf = events.pop();
        for (const ev of events) {
          const line = ev.split('\n').find((l) => l.startsWith('data: '));
          if (!line) continue;
          const data = line.slice(6);
          if (data === '[DONE]') continue;
          let o; try { o = JSON.parse(data); } catch { continue; }
          if (o.error) { throw new ApiError(502, o.error.message || 'stream error', o); }
          if (o.usage) usage = o.usage;
          const c = o.choices && o.choices[0];
          if (!c) continue;
          const dl = c.delta || {};
          const piece = dl.content || '', think = dl.reasoning_content || dl.reasoning || '';
          if ((piece || think) && !a.meta) a.meta = { ttft: (performance.now() - t0) / 1000 };
          a.content += piece; a.reasoning += think;
          if (c.finish_reason) finish = c.finish_reason;
          if (!frame) frame = requestAnimationFrame(paint);
        }
      }
      a.streaming = false;
      const total = (performance.now() - t0) / 1000;
      a.meta = { ...(a.meta || { ttft: total }), total, out: usage ? usage.completion_tokens : null, cut: finish === 'length' };
      if (usage && total > a.meta.ttft) a.meta.tps = usage.completion_tokens / (total - a.meta.ttft);
      if (!a.content && !a.reasoning) a.content = '(답이 비어 있습니다. 다시 시도해주세요.)';
    } catch (err) {
      a.streaming = false;
      if (err.name === 'AbortError') {
        a.meta = { ...(a.meta || {}), total: (performance.now() - t0) / 1000 };
        a.content += a.content ? '\n\n(여기서 멈췄습니다.)' : '(보내기를 멈췄습니다.)';
      } else if (err instanceof ApiError && err.status === 401) {
        return;
      } else {
        if (!a.content) S.chat.log.pop();
        S.chat.log.push({ role: 'error', content: err instanceof ApiError ? '응답이 중간에 끊겼습니다. 잠시 후에 다시 시도해주세요.' : '서버에 연결할 수 없습니다. 잠시 후에 다시 시도해주세요.', retry: true });
      }
    } finally {
      if (frame) cancelAnimationFrame(frame);
      S.chat.ctrl = null; setBusy(false); renderLog(); saveChat();
      toBottom();
    }
  }

  function resend(m) {
    const i = S.chat.log.indexOf(m);
    if (i >= 0) S.chat.log.splice(i, 1);
    renderLog();
    ask();
  }

  chat.form.addEventListener('submit', (e) => {
    e.preventDefault();
    if (S.chat.busy) return;
    const text = chat.input.value.trim();
    if (!text) {
      chat.input.setAttribute('aria-invalid', 'true'); chat.help.dataset.tone = 'error';
      chat.help.textContent = '메시지를 입력해주세요.'; chat.input.focus(); return;
    }
    chat.input.removeAttribute('aria-invalid'); delete chat.help.dataset.tone;
    chat.help.textContent = '';
    S.chat.log.push({ role: 'user', content: text });
    chat.input.value = '';
    ask();
  });
  chat.input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey && !e.isComposing && e.keyCode !== 229) { e.preventDefault(); chat.form.requestSubmit(); }
  });
  chat.input.addEventListener('input', () => chat.input.removeAttribute('aria-invalid'));
  chat.stop.addEventListener('click', () => S.chat.ctrl && S.chat.ctrl.abort());
  $('[data-new]').addEventListener('click', newChat);
  $('[data-example]').addEventListener('click', () => { chat.input.value = EXAMPLE; chat.input.focus(); });

  VIEWS.chat = () => {
    renderLog();
    loadHistory().then(() => {
      const keep = store.get('pknu.chat.id');
      if (keep && !S.chat.id && !S.chat.log.length) openChat(keep);
    });
    const state = $('#chat-state');
    const load = async () => {
      try { await refreshModels(); modelOptions(); } catch (err) {
        state.textContent = '모델 상태를 불러오지 못했습니다. 잠시 후 자동으로 다시 불러옵니다.';
      }
    };
    if (!S.models.length) { chat.model.replaceChildren(h('option', { text: '불러오는 중' })); state.textContent = '모델 상태를 불러오는 중입니다.'; }
    else modelOptions();
    load();
    every(10000, async () => { try { await refreshModels(); describeModel(); } catch { /* 다음 주기에 다시 */ } });
  };

  // ------------------------------------------------------------------ 사용량
  const RESULT = {
    ok: '성공', stream_broken: '도중에 끊김', client_closed: '멈춤', user_rpm: '분당 한도 초과', user_concurrency: '동시 요청 초과',
    queue_full: '대기열 가득 참', queue_timeout: '대기 시간 초과', no_backend: '서버 없음', context_length: '문맥 길이 초과',
    unknown_model: '없는 모델', bad_json: '형식 오류', backend: '모델 서버 오류', backend_connect: '연결 실패',
  };
  VIEWS.usage = () => {
    const fig = $('#usage-figures'), rec = $('#usage-recent');
    const load = async (first) => {
      if (first) { loading(fig, 2); loading(rec, 6); }
      try {
        const me = await loadMe();
        const t = me.today, codes = t.codes || {};
        const ok = codes['200'] || 0;
        $('#usage-who').textContent = `${me.user_id}, ${TIER[me.tier] || me.tier}, ${me.group}`;
        filled(fig,
          h('div', { class: 'usage-now' },
            plate('최근 1분 요청', int(me.now.rpm_used), '회', int(me.limits.rpm)),
            plate('지금 처리 중', int(me.now.inflight), '건', int(me.limits.max_concurrent))),
          h('dl', { class: 'cells cells--4' },
            figure('오늘 요청', int(t.requests), '건'),
            figure('거절된 요청', int(t.requests - ok), '건'),
            figure('입력 토큰', int(t.prompt_tokens)),
            figure('출력 토큰', int(t.completion_tokens))));
        if (!me.recent.length) {
          empty(rec, '오늘 보낸 요청이 없습니다.', '실습 화면이나 API로 요청을 보내면 결과와 걸린 시간이 여기에 남습니다.',
            h('a', { class: 'btn btn--quiet btn--s', href: '#/chat', text: '실습으로 가기' }));
          return;
        }
        const names = Object.fromEntries(S.models.map((m) => [m.id, m.title]));
        filled(rec, table([
          { label: '시각(KST)', cls: 'time', cell: (x) => kst(x.ts) },
          { label: '모델', cell: (x) => names[x.model] || x.model },
          { label: '결과', cell: (x) => (x.code === '200' && x.reason === 'ok' ? `${x.code} ${RESULT.ok}` : h('span', { class: 'state', 'data-state': 'busy', text: `${x.code} ${RESULT[x.reason] || x.reason}` })) },
          { label: '입력 토큰', cls: 'num', cell: (x) => (x.code === '200' ? int(x.prompt_tokens) : '-') },
          { label: '출력 토큰', cls: 'num', cell: (x) => (x.code === '200' ? int(x.completion_tokens) : '-') },
          { label: '첫 응답', cls: 'num', cell: (x) => secs(x.ttft_s) },
          { label: '전체', cls: 'num', cell: (x) => secs(x.dur_s) },
        ], me.recent));
      } catch (err) {
        if (first) { failed(fig, '사용량을 불러오지 못했습니다.', err, () => load(true)); rec.replaceChildren(); }
      }
    };
    (S.models.length ? Promise.resolve() : refreshModels().catch(() => {})).then(() => load(true));
    every(10000, () => load(false));
  };

  // ------------------------------------------------------------------ 시작
  (async () => {
    try { S.cfg = await fetch('/api/config').then((r) => r.json()); } catch { S.cfg = {}; }
    if (S.key) {
      try { await loadMe(); } catch { S.key = null; S.me = null; store.del('pknu.key'); }
    }
    showChrome();
    route();
  })();
})();
