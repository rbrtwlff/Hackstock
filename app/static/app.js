function showTab(id) {
  document.querySelectorAll('.tab').forEach(t => t.classList.add('hidden'));
  document.getElementById(id).classList.remove('hidden');
}

let statusPoller = null;

function renderRunStatus(state, infoMessage = '') {
  const el = document.getElementById('runStatus');
  if (!el) return;
  const msg = infoMessage ? `<div><b>${escapeHtml(infoMessage)}</b></div>` : '';
  el.innerHTML = `${msg}
    <div>Status: <b>${escapeHtml(state.phase || 'unknown')}</b> ${state.running ? '⏳' : '✅'}</div>
    <div>Job: ${escapeHtml(state.job_id || '-')}</div>
    <div>Docs: ${state.docs_done || 0} / ${state.docs_total || 0}</div>
    <div>Blocks: ${state.blocks_done || 0} / ${state.blocks_total || 0}</div>
    <div>LLM done: ${state.llm_done || 0}, Failed: ${state.failed || 0}</div>
    <div>Last error: ${escapeHtml(state.last_error || '-')}</div>`;
}

async function fetchStatus() {
  const resp = await fetch('/api/status');
  const state = await resp.json();
  renderRunStatus(state);

  if (!state.running && statusPoller) {
    clearInterval(statusPoller);
    statusPoller = null;
    await Promise.all([loadSearch(), loadOutline(), loadMatrixView(), loadTables()]);
  }
}

function ensureStatusPolling() {
  if (statusPoller) return;
  statusPoller = setInterval(() => {
    fetchStatus().catch(err => console.error('status poll failed', err));
  }, 2000);
}

async function runAll() {
  const resp = await fetch('/api/run-all', {method: 'POST'});
  const data = await resp.json();

  if (resp.status === 409) {
    renderRunStatus(data, 'Bereits laufender Job. Zeige Status.');
    ensureStatusPolling();
    await fetchStatus();
    return;
  }

  if (!resp.ok) {
    renderRunStatus({phase: 'error', running: false, last_error: data?.error || 'run-all failed'}, 'RUN ALL fehlgeschlagen');
    return;
  }

  renderRunStatus({phase: 'starting', running: true, job_id: data.job_id}, `RUN ALL gestartet (Job ${data.job_id})`);
  ensureStatusPolling();
  await fetchStatus();
}

async function retryFailed() {
  await fetch('/api/retry-failed', {method: 'POST'});
  await Promise.all([loadSearch(), loadMatrixView()]);
}

function escapeHtml(value) {
  return (value || '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

const DOC_ORDER = ['Klage', 'Klageerwiderung', 'Replik', 'Duplik', 'Stellungnahme'];
const LINK_COLORS = {
  ATTACKS_FACTS: '#c0392b',
  ATTACKS_LEGAL: '#8e44ad',
  RAISES_DEFENSE: '#2c3e50',
  SUPPORTS: '#7f8c8d',
  DISTINGUISHES: '#16a085',
};

let matrixRows = [];
let matrixLinks = [];

async function loadSearch() {
  const q = document.getElementById('q').value;
  const includeRubrum = document.getElementById('showRubrum').checked;
  const data = await (await fetch('/api/paragraphs?q=' + encodeURIComponent(q) + '&include_rubrum=' + includeRubrum)).json();
  const el = document.getElementById('searchResults');
  const statsEl = document.getElementById('mergeStats');

  const stats = {};
  data.forEach(x => {
    if (!stats[x.doc_id]) {
      stats[x.doc_id] = {raw: x.raw_paragraph_count, semantic: x.semantic_block_count};
    }
  });
  statsEl.innerHTML = Object.entries(stats)
    .map(([doc, s]) => `<div class='card'><b>${doc}</b> Paragraphs merged: ${s.raw} raw -> ${s.semantic} blocks</div>`)
    .join('');

  el.innerHTML = data.map(x => {
    const intro = x.intro_text ? `<div><b>Einleitung</b><br>${escapeHtml(x.intro_text)}</div>` : '';
    const quote = x.quote_text ? `<blockquote>${escapeHtml(x.quote_text)}</blockquote>` : '';
    const body = (!x.quote_text || x.block_type === 'QUOTE_BLOCK') ? `<div>${escapeHtml(x.text)}</div>` : '';
    return `<div class="card"><b>${x.doc_id} (${x.side})</b> <span>${x.block_type}</span><br>${escapeHtml(x.hierarchy_path || '')}<hr>${intro}${quote}${body}<hr><i>${escapeHtml(x.summary||'')}</i><br>Role: ${escapeHtml(x.role||'')}<br>Issues: ${(x.issues||[]).join(', ')}</div>`;
  }).join('');
}

async function loadOutline() {
  const data = await (await fetch('/api/outline')).json();
  const el = document.getElementById('outlineData');
  el.innerHTML = `<div class="grid"><div><h3>Argumentbaum</h3>${data.arguments.map(a=>`<div class='card'>#${a.id} ${a.title}</div>`).join('')}</div><div><h3>Block-Mapping</h3>${data.mapping.map(m=>`<div class='card'>Block ${m.block_id} -> Argument ${m.argument_id}</div>`).join('')}</div><div><h3>Details</h3>Klicken Sie in Search auf Blöcke für Details (MVP).</div></div>`;
}

async function setLinkStatus(id, status) {
  await fetch('/api/links/' + id, {method:'PATCH', headers:{'Content-Type':'application/json'}, body: JSON.stringify({status})});
  await loadMatrixView();
}

async function deleteLink(id) {
  await fetch('/api/links/' + id, {method:'DELETE'});
  await loadMatrixView();
}

function buildFilterQuery() {
  const issue = document.getElementById('matrixIssue').value;
  const linkType = document.getElementById('matrixLinkType').value;
  const status = document.getElementById('matrixStatus').value;
  const minConf = document.getElementById('matrixConfidence').value || '0.7';
  const unanswered = document.getElementById('matrixUnansweredOnly').checked;
  const ourGapsOnly = document.getElementById('matrixOurGapsOnly').checked;

  const params = new URLSearchParams({min_conf: minConf});
  if (issue) params.set('issue', issue);
  if (linkType) params.set('link_type', linkType);
  if (status) params.set('status', status);
  if (unanswered) params.set('unanswered_only', 'true');
  if (ourGapsOnly) params.set('our_gaps_only', 'true');
  return params.toString();
}

function renderSelect(id, values, includeAllLabel = 'Alle') {
  const el = document.getElementById(id);
  if (!el.dataset.loaded) {
    el.innerHTML = `<option value="">${includeAllLabel}</option>` + values.map(v => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join('');
    el.dataset.loaded = '1';
  }
}

function badge(text, css = 'status') {
  return `<span class="badge ${css}">${escapeHtml(text)}</span>`;
}

function cardHtml(item) {
  if (item.type === 'GAP_OPPONENT') {
    return `<div class="matrix-card gap"><div>${escapeHtml(item.text)}</div></div>`;
  }
  const issues = (item.issues || []).map(i => badge(i, 'issue')).join(' ');
  const roles = (item.roles || []).map(r => badge(r, 'role')).join(' ');
  const badges = (item.badges || []).map(b => badge(b, b.includes('Unbeantwortet') ? 'warning' : 'danger')).join(' ');
  const docIdx = DOC_ORDER.indexOf(item.doc_type);
  return `<div class="matrix-card" onclick="loadThread(${item.id})">
    <div class="matrix-card-title">${escapeHtml(item.short_title || item.title)}</div>
    <div class="matrix-meta">${badge(item.doc_type || 'n/a', 'doc')} ${badge(item.side || 'n/a', 'side')} ${badge((docIdx >= 0 ? docIdx + 1 : '-') + '. Schritt', 'doc')}</div>
    <div class="matrix-meta">${issues} ${roles}</div>
    <div class="matrix-meta">${badges}</div>
    <div class="matrix-links">
      ${(item.out_links || []).map(l => `<span class="link-badge" style="background:${LINK_COLORS[l.link_type] || '#34495e'}" title="${escapeHtml(l.rationale_short || '')}">${escapeHtml(l.link_type)} (${l.confidence.toFixed(2)}) • ${escapeHtml(l.status)}</span>`).join('')}
    </div>
  </div>`;
}

function buildVirtualRows(rows) {
  const rowHeight = 280;
  const buffer = 6;
  const viewport = document.getElementById('matrixData');
  const totalHeight = rows.length * rowHeight;

  viewport.innerHTML = `<div class="virtual-spacer" style="height:${totalHeight}px"></div><div class="virtual-content"></div>`;
  const content = viewport.querySelector('.virtual-content');

  function render() {
    const scrollTop = viewport.scrollTop;
    const viewportHeight = viewport.clientHeight;
    const start = Math.max(0, Math.floor(scrollTop / rowHeight) - buffer);
    const end = Math.min(rows.length, Math.ceil((scrollTop + viewportHeight) / rowHeight) + buffer);
    const visible = rows.slice(start, end);

    content.style.transform = `translateY(${start * rowHeight}px)`;
    content.innerHTML = visible.map(row => `
      <div class="issue-row" style="height:${rowHeight - 10}px">
        <div class="issue-title">${escapeHtml(row.issue)}</div>
        <div class="matrix-column">
          <h4>PLAINTIFF</h4>
          ${row.plaintiff.map(cardHtml).join('') || '<div class="matrix-empty">—</div>'}
        </div>
        <div class="matrix-column">
          <h4>DEFENDANT</h4>
          ${row.defendant.map(cardHtml).join('') || '<div class="matrix-empty">—</div>'}
        </div>
      </div>
    `).join('');
  }

  viewport.onscroll = render;
  render();
}

function renderEdges() {
  const legend = document.createElement('div');
  legend.className = 'edge-legend card';
  legend.innerHTML = `<b>Verbindungen (farbcodiert)</b><br>` + matrixLinks.map(l => {
    return `<div><span class="edge-dot" style="background:${LINK_COLORS[l.link_type] || '#34495e'}"></span> ${escapeHtml(l.link_type)}: #${l.from_argument_id} → #${l.to_argument_id} <span title="${escapeHtml(l.rationale_short || '')}">ℹ</span></div>`;
  }).join('');
  const root = document.getElementById('matrixData');
  root.prepend(legend);
}

async function loadMatrixView() {
  const data = await (await fetch('/api/matrix-view?' + buildFilterQuery())).json();
  matrixRows = data.rows || [];
  matrixLinks = data.links || [];

  renderSelect('matrixIssue', data.meta?.issues || []);
  renderSelect('matrixLinkType', data.meta?.link_types || []);
  renderSelect('matrixStatus', data.meta?.statuses || []);

  buildVirtualRows(matrixRows);
  renderEdges();
}

async function loadThread(argumentId) {
  const minConf = document.getElementById('matrixConfidence').value || '0.7';
  const data = await (await fetch(`/api/thread/${argumentId}?min_conf=${minConf}`)).json();
  const el = document.getElementById('threadData');
  const seq = data.sequence || [];
  if (!seq.length) {
    el.innerHTML = 'Kein Thread gefunden.';
    return;
  }
  el.innerHTML = seq.map(node => {
    if (node.type === 'argument') {
      return `<div class="thread-item"><b>${escapeHtml(node.doc_type)} (${escapeHtml(node.side)})</b><br>${escapeHtml(node.title)}</div>`;
    }
    if (node.type === 'link') {
      return `<div class="thread-link">↓ ${escapeHtml(node.link_type)} <span title="${escapeHtml(node.rationale_short || '')}">(${node.confidence.toFixed(2)})</span></div>`;
    }
    return `<div class="thread-gap">${escapeHtml(node.text)}</div>`;
  }).join('');
}

async function loadTables() {
  const data = await (await fetch('/api/tables')).json();
  const el = document.getElementById('tablesData');
  el.innerHTML = data.map(t => `<div class='card'><b>${t.doc_id} / Block ${t.block_index}</b><div>${t.render_html}</div></div>`).join('');
}

window.onload = async () => {
  await Promise.all([loadSearch(), loadOutline(), loadMatrixView(), loadTables()]);
  await fetchStatus();
};
