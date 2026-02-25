const state = {
  data: null,
  activeView: 'outline',
  search: '',
  selectedIssues: new Set(),
  party: 'all',
  role: 'all',
  brief: 'all',
  page: 1,
  perPage: 3,
  selectedParagraph: null,
  expandedNodes: new Set(),
  linkType: 'all',
  confidence: 'all',
  status: 'all',
};

const el = (id) => document.getElementById(id);

async function fetchData() {
  try {
    const res = await fetch('/api/data');
    if (!res.ok) throw new Error('api');
    state.data = await res.json();
  } catch (_err) {
    const fallback = await fetch('/static/viewer_data.json');
    state.data = await fallback.json();
  }
}

function filteredParagraphs() {
  return state.data.paragraphs.filter((p) => {
    const text = `${p.original} ${p.summary}`.toLowerCase();
    const okSearch = text.includes(state.search);
    const okIssue = !state.selectedIssues.size || state.selectedIssues.has(p.issue);
    const okParty = state.party === 'all' || p.party === state.party;
    const briefLabel = p.path?.[0] ?? 'Unbekannt';
    const okRole = state.role === 'all' || p.role === state.role;
    const okBrief = state.brief === 'all' || briefLabel === state.brief;
    return okSearch && okIssue && okParty && okRole && okBrief;
  });
}

function renderFilters() {
  const issues = [...new Set(state.data.paragraphs.map((p) => p.issue))];
  const parties = ['all', ...new Set(state.data.paragraphs.map((p) => p.party))];
  const roles = ['all', ...new Set(state.data.paragraphs.map((p) => p.role))];
  const briefs = ['all', ...new Set(state.data.paragraphs.map((p) => p.path?.[0] ?? 'Unbekannt'))];

  const issueWrap = el('issueChips');
  issueWrap.innerHTML = '';
  issues.forEach((issue) => {
    const b = document.createElement('button');
    b.className = `chip ${state.selectedIssues.has(issue) ? 'active' : ''}`;
    b.textContent = issue;
    b.onclick = () => {
      if (state.selectedIssues.has(issue)) state.selectedIssues.delete(issue);
      else state.selectedIssues.add(issue);
      state.page = 1;
      renderAll();
    };
    issueWrap.appendChild(b);
  });

  const setSelectOptions = (id, values, selected) => {
    const s = el(id);
    s.innerHTML = values
      .map((v) => `<option value="${v}">${v === 'all' ? 'Alle' : v}</option>`)
      .join('');
    s.value = selected;
  };

  setSelectOptions('briefSelect', briefs, state.brief);
  setSelectOptions('partySelect', parties, state.party);
  setSelectOptions('roleSelect', roles, state.role);
}

function renderOutlineTree() {
  const tree = {};
  filteredParagraphs().forEach((p) => {
    const [root, child] = p.path;
    tree[root] ??= {};
    tree[root][child] ??= [];
    tree[root][child].push(p);
  });

  const root = el('outlineTree');
  root.innerHTML = '<h3>Outline</h3>';
  Object.entries(tree).forEach(([section, children]) => {
    root.appendChild(renderNode(section, children, section));
  });
}

function renderNode(label, children, key) {
  const wrap = document.createElement('div');
  wrap.className = 'tree-node';
  const hasChildren = typeof children === 'object';
  const expanded = state.expandedNodes.has(key);

  const head = document.createElement('div');
  const toggle = document.createElement('span');
  toggle.className = 'tree-toggle';
  toggle.textContent = hasChildren ? (expanded ? '▼' : '▶') : '•';
  head.append(toggle, document.createTextNode(label));
  head.onclick = () => {
    if (!hasChildren) return;
    if (expanded) state.expandedNodes.delete(key);
    else state.expandedNodes.add(key);
    renderOutlineTree();
  };
  wrap.appendChild(head);

  if (hasChildren && expanded) {
    Object.entries(children).forEach(([childLabel, entries]) => {
      const child = document.createElement('div');
      child.className = 'tree-node';
      child.textContent = `${childLabel} (${entries.length})`;
      wrap.appendChild(child);
    });
  }
  return wrap;
}

function renderParagraphList() {
  const list = filteredParagraphs();
  const totalPages = Math.max(1, Math.ceil(list.length / state.perPage));
  state.page = Math.min(state.page, totalPages);
  const start = (state.page - 1) * state.perPage;
  const pageItems = list.slice(start, start + state.perPage);

  el('paragraphList').innerHTML = pageItems
    .map(
      (p) => `<article class="paragraph-item" data-id="${p.id}"><strong>${p.id}</strong> — ${p.title}<br/><small>${p.summary}</small></article>`,
    )
    .join('');
  el('pageInfo').textContent = `Seite ${state.page} / ${totalPages} (${list.length} Absätze)`;

  document.querySelectorAll('#paragraphList .paragraph-item').forEach((item) => {
    item.onclick = () => {
      const para = state.data.paragraphs.find((p) => p.id === item.dataset.id);
      state.selectedParagraph = para;
      renderDetail();
    };
  });
}

function renderDetail() {
  if (!state.selectedParagraph) {
    el('detailPane').textContent = 'Absatz auswählen…';
    return;
  }
  const p = state.selectedParagraph;
  el('detailPane').innerHTML = `
    <h3>${p.id} – ${p.title}</h3>
    <p><strong>Partei:</strong> ${p.party} | <strong>Rolle:</strong> ${p.role} | <strong>Issue:</strong> ${p.issue}</p>
    <p><strong>Original:</strong> ${p.original}</p>
    <p><strong>Summary:</strong> ${p.summary}</p>
  `;
}

function filteredLinks() {
  return state.data.links.filter((l) => {
    const okType = state.linkType === 'all' || l.link_type === state.linkType;
    const okStatus = state.status === 'all' || l.status === state.status;
    const okConf = state.confidence === 'all' || (l.confidence ?? 0) >= Number(state.confidence);
    return okType && okStatus && okConf;
  });
}

function renderMatrix() {
  const paragraphs = filteredParagraphs();
  const plaintiffs = paragraphs.filter((p) => p.party === 'Kläger');
  const defendants = paragraphs.filter((p) => p.party === 'Beklagter');
  el('plaintiffList').innerHTML = plaintiffs.map((p) => `<li>${p.id}: ${p.title}</li>`).join('');
  el('defendantList').innerHTML = defendants.map((p) => `<li>${p.id}: ${p.title}</li>`).join('');

  const linkTypes = ['all', ...new Set(state.data.links.map((l) => l.link_type))];
  const statuses = ['all', 'proposed', 'confirmed', 'rejected'];
  const setSelect = (id, values, value) => {
    const s = el(id);
    s.innerHTML = values.map((v) => `<option value="${v}">${v}</option>`).join('');
    s.value = value;
  };
  setSelect('linkTypeFilter', linkTypes, state.linkType);
  setSelect('statusFilter', statuses, state.status);

  const links = filteredLinks();
  el('linkList').innerHTML = links
    .map((l) => {
      const rowLabel = el('edgeMode').checked
        ? `${l.id}: ${l.source_id} → ${l.target_id}`
        : `${l.source_id} ↔ ${l.target_id}`;
      return `
      <div class="link-row">
        <div>${rowLabel} | Typ: ${l.link_type} | Conf: ${l.confidence ?? '-'} | Status: <strong>${l.status}</strong></div>
        <div class="link-actions">
          ${['proposed', 'confirmed', 'rejected']
            .map(
              (status) => `<button class="status-btn ${l.status === status ? 'active' : ''}" data-link="${l.id}" data-status="${status}">${status}</button>`,
            )
            .join('')}
          <button class="delete-btn" data-link="${l.id}">Löschen</button>
        </div>
      </div>`;
    })
    .join('');

  const options = state.data.paragraphs
    .map((p) => `<option value="${p.id}">${p.id} ${p.title}</option>`)
    .join('');
  el('sourceSelect').innerHTML = options;
  el('targetSelect').innerHTML = options;

  document.querySelectorAll('.status-btn').forEach((btn) => {
    btn.onclick = async () => {
      await fetch(`/api/links/${btn.dataset.link}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: btn.dataset.status }),
      });
      await fetchData();
      renderMatrix();
    };
  });

  document.querySelectorAll('.delete-btn').forEach((btn) => {
    btn.onclick = async () => {
      await fetch(`/api/links/${btn.dataset.link}`, { method: 'DELETE' });
      await fetchData();
      renderMatrix();
    };
  });
}

function renderTables() {
  el('tableList').innerHTML = state.data.tables
    .map((t) => `<li class="table-item" data-id="${t.id}"><strong>${t.id}</strong> – ${t.title}<br/><small>${t.description}</small></li>`)
    .join('');

  document.querySelectorAll('#tableList .table-item').forEach((row) => {
    row.onclick = () => {
      const table = state.data.tables.find((t) => t.id === row.dataset.id);
      el('tableTitle').textContent = `${table.id} – ${table.title}`;
      el('tableView').innerHTML = table.html;
    };
  });
}

function bindEvents() {
  document.querySelectorAll('.tab').forEach((tab) => {
    tab.onclick = () => {
      state.activeView = tab.dataset.view;
      document.querySelectorAll('.tab').forEach((b) => b.classList.remove('active'));
      document.querySelectorAll('.view').forEach((v) => v.classList.remove('active'));
      tab.classList.add('active');
      el(tab.dataset.view).classList.add('active');
      renderAll();
    };
  });

  el('search').addEventListener('input', (e) => {
    state.search = e.target.value.trim().toLowerCase();
    state.page = 1;
    renderAll();
  });
  el('briefSelect').onchange = (e) => {
    state.brief = e.target.value;
    state.page = 1;
    renderAll();
  };
  el('partySelect').onchange = (e) => {
    state.party = e.target.value;
    state.page = 1;
    renderAll();
  };
  el('roleSelect').onchange = (e) => {
    state.role = e.target.value;
    state.page = 1;
    renderAll();
  };
  el('prevPage').onclick = () => {
    state.page = Math.max(1, state.page - 1);
    renderParagraphList();
  };
  el('nextPage').onclick = () => {
    state.page += 1;
    renderParagraphList();
  };

  el('linkTypeFilter').onchange = (e) => {
    state.linkType = e.target.value;
    renderMatrix();
  };
  el('confidenceFilter').onchange = (e) => {
    state.confidence = e.target.value;
    renderMatrix();
  };
  el('statusFilter').onchange = (e) => {
    state.status = e.target.value;
    renderMatrix();
  };
  el('edgeMode').onchange = () => renderMatrix();

  el('addLinkForm').onsubmit = async (e) => {
    e.preventDefault();
    const body = {
      source_id: el('sourceSelect').value,
      target_id: el('targetSelect').value,
      link_type: el('newLinkType').value,
      status: el('newStatus').value,
    };
    const confidence = el('newConfidence').value;
    if (confidence) body.confidence = Number(confidence);
    await fetch('/api/links', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    e.target.reset();
    await fetchData();
    renderMatrix();
  };
}

function renderAll() {
  renderFilters();
  renderOutlineTree();
  renderParagraphList();
  renderDetail();
  renderMatrix();
  renderTables();
}

(async function init() {
  await fetchData();
  bindEvents();
  renderAll();
})();
