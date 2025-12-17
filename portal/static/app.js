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

})();
