/* ── SyndicPro Scanner — dashboard.js ── */

const API      = '';
const PAGE_SZ  = 50;
let   currentOffset = 0;
let   currentFilter = 'all';
let   totalCount    = 0;

/* ── Init ── */
loadStats();
loadResults(0);

document.getElementById('filterSelect').addEventListener('change', (e) => {
  currentFilter = e.target.value;
  loadResults(0);
});

document.getElementById('clearBtn').addEventListener('click', async () => {
  if (!confirm('Supprimer tous les résultats de la base de données ?')) return;
  try {
    const res = await fetch(API + '/results/clear', { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok') {
      loadStats();
      loadResults(0);
    }
  } catch (e) {
    alert('Erreur lors de la suppression.');
  }
});

/* ── Stats ── */
async function loadStats() {
  try {
    const res  = await fetch(API + '/stats');
    const data = await res.json();

    document.getElementById('statTotal').textContent = data.total ?? '—';
    document.getElementById('statFound').textContent = data.found ?? '—';
    document.getElementById('statRate').textContent  = (data.success_rate ?? '—') + '%';
    document.getElementById('statConf').textContent  = (data.avg_confidence ?? '—') + '%';
  } catch (e) {
    console.error('Stats error:', e);
  }
}

/* ── Results ── */
async function loadResults(offset) {
  currentOffset = offset;
  const body = document.getElementById('resultsBody');
  body.innerHTML = `<tr><td colspan="9"><div class="empty-state"><div class="spinner-large"></div><p>Chargement…</p></div></td></tr>`;

  try {
    const found = currentFilter === 'found' ? '1' : '0';
    const url   = `${API}/results?limit=${PAGE_SZ}&offset=${offset}&found=${found}`;
    const res   = await fetch(url);
    const data  = await res.json();

    const rows = data.results || [];
    totalCount = data.count || 0;

    document.getElementById('countLabel').textContent =
      `${rows.length} résultat(s) affiché(s) sur ${totalCount}`;

    if (!rows.length) {
      body.innerHTML = `<tr><td colspan="9"><div class="empty-state"><div class="empty-icon">📭</div><p>Aucun résultat trouvé.</p></div></td></tr>`;
      renderPagination(0);
      return;
    }

    body.innerHTML = rows.map((r, i) => {
      const conf    = r.confidence || 0;
      const confColor = conf >= 60 ? '#16a34a' : conf >= 30 ? '#d97706' : '#dc2626';
      const date    = r.created_at ? r.created_at.slice(0, 16).replace('T', ' ') : '—';
      const phone   = r.phone   ? `<a href="tel:${r.phone}">${r.phone}</a>` : '<span style="color:#94a3b8">—</span>';
      const email   = r.email   ? `<a href="mailto:${r.email}">${r.email}</a>` : '<span style="color:#94a3b8">—</span>';
      const badge   = r.found
        ? `<span class="badge-found">✔ Trouvé</span>`
        : `<span class="badge-not-found">✘ Non trouvé</span>`;
      let president = '<span style="color:#94a3b8">—</span>';
      if (r.members && r.members.length) {
        president = r.members
          .map(m => `<span title="${escHtml(m.qualite)}" style="display:block;font-size:.78rem">${escHtml(m.nom)} <span style="color:#94a3b8">(${escHtml(m.qualite)})</span></span>`)
          .join('');
      } else if (r.president) {
        president = escHtml(r.president);
      }

      return `<tr>
        <td style="color:#94a3b8;font-size:.8rem">${offset + i + 1}</td>
        <td><strong>${escHtml(r.name)}</strong></td>
        <td>${escHtml(r.city)}</td>
        <td>${phone}</td>
        <td style="word-break:break-all">${email}</td>
        <td style="font-size:.82rem">${president}</td>
        <td>
          <span class="conf-pill" style="background:${confColor}20;color:${confColor}">${conf}%</span>
        </td>
        <td>${badge}</td>
        <td style="font-size:.78rem;color:#94a3b8;white-space:nowrap">${date}</td>
      </tr>`;
    }).join('');

    renderPagination(offset);

  } catch (e) {
    body.innerHTML = `<tr><td colspan="9"><div class="alert-error">Erreur chargement : ${e.message}</div></td></tr>`;
  }
}

function renderPagination(offset) {
  const pag = document.getElementById('pagination');
  const pages = Math.ceil(totalCount / PAGE_SZ);
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

function escHtml(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
