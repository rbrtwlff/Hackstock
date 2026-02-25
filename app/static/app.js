function showTab(id) {
  document.querySelectorAll('.tab').forEach(t => t.classList.add('hidden'));
  document.getElementById(id).classList.remove('hidden');
}

async function runAll() {
  await fetch('/api/run-all', {method: 'POST'});
  await Promise.all([loadSearch(), loadOutline(), loadMatrix(), loadTables()]);
}

async function retryFailed() {
  await fetch('/api/retry-failed', {method: 'POST'});
  await Promise.all([loadSearch(), loadMatrix()]);
}

function escapeHtml(value) {
  return (value || '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

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
  await loadMatrix();
}

async function deleteLink(id) {
  await fetch('/api/links/' + id, {method:'DELETE'});
  await loadMatrix();
}

async function loadMatrix() {
  const data = await (await fetch('/api/matrix')).json();
  const el = document.getElementById('matrixData');
  el.innerHTML = data.map(x => `<div class='card'><b>${x.from_title}</b> ↔ <b>${x.to_title}</b><br>Typ: ${x.link_type} | Konfidenz: ${x.confidence.toFixed(2)} | Status: ${x.status}<br>${x.rationale_short}<br><button onclick="setLinkStatus(${x.id},'confirmed')">confirm</button><button onclick="setLinkStatus(${x.id},'rejected')">reject</button><button onclick="setLinkStatus(${x.id},'proposed')">proposed</button><button onclick="deleteLink(${x.id})">löschen</button></div>`).join('');
}

async function loadTables() {
  const data = await (await fetch('/api/tables')).json();
  const el = document.getElementById('tablesData');
  el.innerHTML = data.map(t => `<div class='card'><b>${t.doc_id} / Block ${t.block_index}</b><div>${t.render_html}</div></div>`).join('');
}

window.onload = async () => {
  await Promise.all([loadSearch(), loadOutline(), loadMatrix(), loadTables()]);
};
