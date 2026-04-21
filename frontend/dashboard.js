/* ── SyndicPro Scanner — dashboard.js v8 ── */

const API      = '';
const PAGE_SZ  = 50;
let   currentOffset    = 0;
let   currentFilter    = 'all';
let   currentMinConf   = 0;
let   totalCount       = 0;
let   allRows          = [];   // rows from last fetch (for client-side conf filter)

/* ── Init ── */
loadStats();
loadResults(0);

document.getElementById('filterSelect').addEventListener('change', (e) => {
  currentFilter = e.target.value;
  loadResults(0);
});

document.getElementById('confFilter').addEventListener('change', (e) => {
  currentMinConf = parseInt(e.target.value, 10) || 0;
  renderRows();
});

document.getElementById('refreshBtn').addEventListener('click', () => {
  loadStats();
  loadResults(currentOffset);
});

document.getElementById('clearBtn').addEventListener('click', async () => {
  if (!confirm('Supprimer tous les résultats de la base de données ?')) return;
  try {
    const res = await fetch(API + '/results/clear', { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok') { loadStats(); loadResults(0); }
  } catch (e) {
    alert('Erreur lors de la suppression.');
  }
});

/* ── Stats ── */
async function loadStats() {
  try {
    const data = await (await fetch(API + '/stats')).json();
    document.getElementById('statTotal').textContent     = data.total ?? '—';
    document.getElementById('statFound').textContent     = data.found ?? '—';
    document.getElementById('statRate').textContent      = (data.success_rate ?? '—') + '%';
    document.getElementById('statConf').textContent      = (data.avg_confidence ?? '—') + '%';
    const we = document.getElementById('statWithEmail');
    const em = document.getElementById('statEmailed');
    if (we) we.textContent = data.with_email ?? '—';
    if (em) em.textContent = data.emailed ?? '—';
  } catch (e) { console.error('Stats error:', e); }
}

/* ── Results ── */
async function loadResults(offset) {
  currentOffset = offset;
  const body = document.getElementById('resultsBody');
  body.innerHTML = `<tr><td colspan="10"><div class="empty-state"><div class="spinner-large"></div><p>Chargement…</p></div></td></tr>`;

  try {
    const found = currentFilter === 'found' ? '1' : '0';
    const url   = `${API}/results?limit=${PAGE_SZ}&offset=${offset}&found=${found}`;
    const data  = await (await fetch(url)).json();

    allRows    = data.results || [];
    totalCount = data.count  || 0;
    renderRows();
    renderPagination(offset);
  } catch (e) {
    body.innerHTML = `<tr><td colspan="10"><div class="alert-error">Erreur chargement : ${e.message}</div></td></tr>`;
  }
}

function renderRows() {
  const body = document.getElementById('resultsBody');
  const rows = allRows.filter(r => (r.confidence || 0) >= currentMinConf);

  document.getElementById('countLabel').textContent =
    `${rows.length} résultat(s) affiché(s) sur ${totalCount}`;

  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="10"><div class="empty-state"><div class="empty-icon">📭</div><p>Aucun résultat trouvé.</p></div></td></tr>`;
    return;
  }

  body.innerHTML = rows.map((r, i) => {
    const conf      = r.confidence || 0;
    const confColor = conf >= 60 ? '#16a34a' : conf >= 30 ? '#d97706' : '#dc2626';
    const date      = r.created_at ? r.created_at.slice(0, 16).replace('T', ' ') : '—';
    const phone     = r.phone ? `<a href="tel:${r.phone}">${r.phone}</a>` : '<span style="color:#94a3b8">—</span>';
    const email     = r.email ? `<a href="mailto:${r.email}">${escHtml(r.email)}</a>` : '<span style="color:#94a3b8">—</span>';

    let statusBadge = r.found
      ? `<span class="badge-found">✔ Trouvé</span>`
      : `<span class="badge-not-found">✘ Non trouvé</span>`;
    if (r.verified) {
      statusBadge += ` <span class="badge-verified" title="Vérifié manuellement">✓ Vérifié</span>`;
    }

    let president = '<span style="color:#94a3b8">—</span>';
    if (r.members && r.members.length) {
      president = r.members
        .map(m => `<span style="display:block;font-size:.78rem">${escHtml(m.nom)} <span style="color:#94a3b8">(${escHtml(m.qualite)})</span></span>`)
        .join('');
    } else if (r.president) {
      president = escHtml(r.president);
    }

    return `<tr id="row-${r.id}">
      <td style="color:#94a3b8;font-size:.8rem">${currentOffset + i + 1}</td>
      <td><strong>${escHtml(r.name)}</strong></td>
      <td>${escHtml(r.city)}</td>
      <td>${phone}</td>
      <td style="word-break:break-all">${email}</td>
      <td style="font-size:.82rem">${president}</td>
      <td><span class="conf-pill" style="background:${confColor}20;color:${confColor}">${conf}%</span></td>
      <td>${statusBadge}</td>
      <td style="font-size:.78rem;color:#94a3b8;white-space:nowrap">${date}</td>
      <td class="actions-cell">
        <button class="btn-icon" title="Modifier" onclick="openModal(${r.id})">✏️</button>
        <button class="btn-icon btn-icon-rescrape" title="Relancer le scraping" onclick="rescrape(${r.id}, this)">🔄</button>
      </td>
    </tr>`;
  }).join('');
}

function renderPagination(offset) {
  const pag     = document.getElementById('pagination');
  const pages   = Math.ceil(totalCount / PAGE_SZ);
  const current = Math.floor(offset / PAGE_SZ);

  if (pages <= 1) { pag.innerHTML = ''; return; }

  let html = '';
  for (let i = 0; i < pages; i++) {
    const active = i === current
      ? 'style="background:var(--primary);color:white;border-color:var(--primary)"'
      : '';
    html += `<button class="btn-secondary" ${active} onclick="loadResults(${i * PAGE_SZ})">${i + 1}</button>`;
  }
  pag.innerHTML = html;
}

/* ── Rescrape ── */
async function rescrape(rowId, btn) {
  if (!confirm('Relancer le scraping pour cette entrée ? (peut prendre 30–60 secondes)')) return;
  const origText = btn.textContent;
  btn.textContent = '⏳';
  btn.disabled    = true;

  try {
    const res  = await fetch(`${API}/results/${rowId}/rescrape`, { method: 'POST' });
    const data = await res.json();
    if (data.error) {
      alert('Erreur : ' + data.error);
    } else {
      // Mettre à jour la ligne dans allRows
      const idx = allRows.findIndex(r => r.id === rowId);
      if (idx >= 0) {
        allRows[idx] = { ...allRows[idx], ...data, id: rowId };
      }
      renderRows();
    }
  } catch (e) {
    alert('Erreur réseau : ' + e.message);
  } finally {
    btn.textContent = origText;
    btn.disabled    = false;
  }
}

/* ── Modal édition ── */
async function openModal(rowId) {
  try {
    const res = await fetch(`${API}/results/${rowId}`);
    if (!res.ok) { alert('Impossible de charger ce résultat.'); return; }
    const r = await res.json();

    document.getElementById('modalRowId').value    = rowId;
    document.getElementById('modalTitle').textContent = `Modifier : ${r.name}`;
    document.getElementById('modalPhone').value    = r.phone    || '';
    document.getElementById('modalEmail').value    = r.email    || '';
    document.getElementById('modalWebsite').value  = r.website  || '';
    document.getElementById('modalPresident').value = r.president || '';
    document.getElementById('modalAddress').value  = r.address  || '';
    document.getElementById('modalNotes').value    = r.notes    || '';
    document.getElementById('modalVerified').checked = !!r.verified;

    document.getElementById('editModal').classList.remove('hidden');
  } catch (e) {
    alert('Erreur : ' + e.message);
  }
}

function closeModal() {
  document.getElementById('editModal').classList.add('hidden');
}

async function saveModal() {
  const rowId    = parseInt(document.getElementById('modalRowId').value, 10);
  const saveBtn  = document.getElementById('modalSaveBtn');
  saveBtn.textContent = 'Enregistrement…';
  saveBtn.disabled    = true;

  const payload = {
    phone:     document.getElementById('modalPhone').value.trim(),
    email:     document.getElementById('modalEmail').value.trim(),
    website:   document.getElementById('modalWebsite').value.trim(),
    president: document.getElementById('modalPresident').value.trim(),
    address:   document.getElementById('modalAddress').value.trim(),
    notes:     document.getElementById('modalNotes').value.trim(),
    verified:  document.getElementById('modalVerified').checked ? 1 : 0,
  };

  try {
    const res = await fetch(`${API}/results/${rowId}`, {
      method:  'PUT',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });
    const data = await res.json();
    if (data.status === 'ok') {
      // Mettre à jour le cache local
      const idx = allRows.findIndex(r => r.id === rowId);
      if (idx >= 0) { Object.assign(allRows[idx], payload); }
      renderRows();
      closeModal();
    } else {
      alert('Erreur : ' + (data.error || 'Inconnue'));
    }
  } catch (e) {
    alert('Erreur réseau : ' + e.message);
  } finally {
    saveBtn.textContent = 'Enregistrer';
    saveBtn.disabled    = false;
  }
}

// Fermer le modal en cliquant en dehors
document.getElementById('editModal').addEventListener('click', (e) => {
  if (e.target === document.getElementById('editModal')) closeModal();
});

/* ── Helpers ── */
function escHtml(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
