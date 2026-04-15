/* ── SyndicPro Scanner — app.js ── */

const API = '';  // même origine (Flask sert le frontend et l'API)

/* ── Onglets ── */
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const target = btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(s => s.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + target).classList.add('active');
  });
});

/* ── Recherche simple ── */
const searchForm    = document.getElementById('searchForm');
const searchBtn     = document.getElementById('searchBtn');
const searchBtnText = document.getElementById('searchBtnText');
const searchSpinner = document.getElementById('searchBtnSpinner');
const resultBox     = document.getElementById('resultBox');
const errorBox      = document.getElementById('errorBox');

searchForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  const name = document.getElementById('inputName').value.trim();
  const city = document.getElementById('inputCity').value.trim();
  if (!name || !city) return;

  // État chargement
  searchBtnText.textContent = 'Recherche en cours…';
  searchSpinner.classList.remove('hidden');
  searchBtn.disabled = true;
  resultBox.classList.add('hidden');
  errorBox.classList.add('hidden');

  try {
    const res = await fetch(API + '/scrape', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, city })
    });

    const data = await res.json();

    if (!res.ok) {
      showError(data.error || 'Erreur serveur');
      return;
    }

    renderResult(data);

  } catch (err) {
    showError('Impossible de joindre le serveur. Vérifiez votre connexion.');
  } finally {
    searchBtnText.textContent = 'Lancer la recherche';
    searchSpinner.classList.add('hidden');
    searchBtn.disabled = false;
  }
});

function renderResult(data) {
  document.getElementById('resName').textContent = data.name || '—';
  document.getElementById('resCity').textContent = data.city || '';

  // Badge trouvé / non trouvé
  const badge = document.getElementById('resBadge');
  if (data.found) {
    badge.textContent = '✔ Trouvé';
    badge.className = 'found-badge found';
  } else {
    badge.textContent = '✘ Non trouvé';
    badge.className = 'found-badge not-found';
  }

  // Contacts
  const grid = document.getElementById('contactsGrid');
  grid.innerHTML = '';
  grid.appendChild(contactItem('📞 Téléphone', data.phone,
    data.phone ? `tel:${data.phone}` : null,
    data.phone_conf ? `Confiance : ${data.phone_conf}%` : null));
  grid.appendChild(contactItem('✉ Email', data.email,
    data.email ? `mailto:${data.email}` : null,
    data.email_conf ? `Confiance : ${data.email_conf}%` : null));
  grid.appendChild(contactItem('🌐 Site web', data.website,
    data.website ? ('https://' + data.website.replace(/^https?:\/\//, '')) : null, null));

  // Barre de confiance
  const conf = data.global_conf || 0;
  document.getElementById('confValue').textContent = conf + '%';
  const bar = document.getElementById('confBar');
  bar.style.width = conf + '%';
  bar.style.background = conf >= 60 ? '#16a34a' : conf >= 30 ? '#d97706' : '#dc2626';

  // Sources
  const sourcesRow = document.getElementById('sourcesRow');
  if (data.sources_hit && data.sources_hit.length) {
    sourcesRow.innerHTML = '<strong style="font-size:.8rem;color:#475569">Sources : </strong>'
      + data.sources_hit.map(s => `<span class="source-chip">${s}</span>`).join('');
  } else {
    sourcesRow.innerHTML = '';
  }

  // Tous les contacts (dépliables)
  const allRow = document.getElementById('allContactsRow');
  const extras = [];
  if (data.all_phones && data.all_phones.length > 1)
    extras.push(`<strong>Tous les téléphones :</strong> ${data.all_phones.join(' · ')}`);
  if (data.all_emails && data.all_emails.length > 1)
    extras.push(`<strong>Tous les emails :</strong> ${data.all_emails.join(' · ')}`);
  if (extras.length) {
    allRow.innerHTML = extras.join('<br>');
    allRow.classList.remove('hidden');
  } else {
    allRow.classList.add('hidden');
  }

  resultBox.classList.remove('hidden');
}

function contactItem(label, value, href, subtext) {
  const div = document.createElement('div');
  div.className = 'contact-item';
  const labelEl = document.createElement('div');
  labelEl.className = 'contact-label';
  labelEl.textContent = label;
  const valueEl = document.createElement('div');
  valueEl.className = 'contact-value';
  if (value) {
    if (href) {
      const a = document.createElement('a');
      a.href = href;
      a.target = '_blank';
      a.textContent = value;
      valueEl.appendChild(a);
    } else {
      valueEl.textContent = value;
    }
    if (subtext) {
      const sub = document.createElement('div');
      sub.style.cssText = 'font-size:.72rem;color:#94a3b8;margin-top:.2rem;font-weight:400';
      sub.textContent = subtext;
      valueEl.appendChild(sub);
    }
  } else {
    valueEl.innerHTML = '<span class="contact-empty">Non trouvé</span>';
  }
  div.appendChild(labelEl);
  div.appendChild(valueEl);
  return div;
}

function showError(msg) {
  errorBox.textContent = '⚠ ' + msg;
  errorBox.classList.remove('hidden');
}

/* ── Import Excel ── */
const fileInput  = document.getElementById('fileInput');
const uploadZone = document.getElementById('uploadZone');
const fileNameEl = document.getElementById('fileName');
const enrichBtn  = document.getElementById('enrichBtn');
const enrichText = document.getElementById('enrichBtnText');
const enrichSpin = document.getElementById('enrichSpinner');
const enrichProg = document.getElementById('enrichProgress');
const enrichErr  = document.getElementById('enrichError');

uploadZone.addEventListener('click', (e) => {
  if (e.target !== fileInput) fileInput.click();
});

uploadZone.addEventListener('dragover', (e) => {
  e.preventDefault();
  uploadZone.classList.add('drag-over');
});
uploadZone.addEventListener('dragleave', () => uploadZone.classList.remove('drag-over'));
uploadZone.addEventListener('drop', (e) => {
  e.preventDefault();
  uploadZone.classList.remove('drag-over');
  const file = e.dataTransfer.files[0];
  if (file) handleFileSelected(file);
});

fileInput.addEventListener('change', () => {
  if (fileInput.files[0]) handleFileSelected(fileInput.files[0]);
});

function handleFileSelected(file) {
  const ext = file.name.slice(file.name.lastIndexOf('.')).toLowerCase();
  if (!['.xlsx', '.xls'].includes(ext)) {
    enrichErr.textContent = '⚠ Format non supporté. Utilisez un fichier .xlsx ou .xls';
    enrichErr.classList.remove('hidden');
    return;
  }
  enrichErr.classList.add('hidden');
  fileNameEl.textContent = '📄 ' + file.name;
  fileNameEl.classList.remove('hidden');
  enrichBtn.disabled = false;
  enrichBtn._file = file;
}

enrichBtn.addEventListener('click', async () => {
  const file = enrichBtn._file;
  if (!file) return;

  enrichText.textContent = 'Traitement…';
  enrichSpin.classList.remove('hidden');
  enrichBtn.disabled = true;
  enrichProg.classList.remove('hidden');
  enrichErr.classList.add('hidden');

  try {
    const fd = new FormData();
    fd.append('file', file);

    const res = await fetch(API + '/enrich', {
      method: 'POST',
      body: fd
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'Erreur serveur' }));
      throw new Error(err.error || 'Erreur serveur');
    }

    const blob = await res.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = 'syndicats_enrichis.xlsx';
    a.click();
    URL.revokeObjectURL(url);

  } catch (err) {
    enrichErr.textContent = '⚠ ' + err.message;
    enrichErr.classList.remove('hidden');
  } finally {
    enrichText.textContent = 'Enrichir le fichier';
    enrichSpin.classList.add('hidden');
    enrichBtn.disabled = false;
    enrichProg.classList.add('hidden');
  }
});
