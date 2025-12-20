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
    let name = (location.hash.match(/^#\/(\w+)/) || [])[1] || 'models';
    if (!S.me) name = 'login';
    else if (name === 'login' || !VIEWS[name] || (name === 'ops' && S.me.tier !== 'staff')) name = 'models';
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
