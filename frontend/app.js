/* ── SyndicPro Scanner — app.js ── */

const API = '';  // même origine (Flask sert le frontend et l'API)

/* ── Onglets ── */
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const target = btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(s => {
      s.classList.remove('active');
      s.classList.add('hidden');       // <-- fix : remettre hidden sur tous
    });
    btn.classList.add('active');
    const el = document.getElementById('tab-' + target);
    el.classList.remove('hidden');     // <-- fix : retirer hidden sur l'onglet cible
    el.classList.add('active');
  });
});

/* ── Recherche simple ── */
const searchForm    = document.getElementById('searchForm');
const searchBtn     = document.getElementById('searchBtn');
const searchBtnText = document.getElementById('searchBtnText');
const searchSpinner = document.getElementById('searchBtnSpinner');
const resultBox     = document.getElementById('resultBox');
const errorBox      = document.getElementById('errorBox');

/* ── Cold start detection ── */
let _coldTimer = null;
const COLD_THRESHOLD_MS = 8000;

function _startColdTimer() {
  _coldTimer = setTimeout(() => {
    const b = document.getElementById('coldStartBanner');
    if (b) b.classList.remove('hidden');
  }, COLD_THRESHOLD_MS);
}

function _stopColdTimer() {
  if (_coldTimer) { clearTimeout(_coldTimer); _coldTimer = null; }
  const b = document.getElementById('coldStartBanner');
  if (b) b.classList.add('hidden');
}

searchForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  const name    = document.getElementById('inputName').value.trim();
  const city    = document.getElementById('inputCity').value.trim();
  const context = (document.getElementById('inputContext')?.value || '').trim();
  if (!name || !city) return;

  hideCandidates();
  searchBtnText.textContent = 'Recherche RNE…';
  searchSpinner.classList.remove('hidden');
  searchBtn.disabled = true;
  resultBox.classList.add('hidden');
  errorBox.classList.add('hidden');
  _startColdTimer();

  try {
    // ── Étape 1 : chercher les candidats RNE ──────────────────────────────
    const candRes  = await fetch(`${API}/rne/candidates?name=${encodeURIComponent(name)}&city=${encodeURIComponent(city)}`);
    _stopColdTimer();
    const candData = await candRes.json();
    const candidates = candData.candidates || [];

    if (candidates.length === 0) {
      // Pas de résultat RNE → scrape direct sans rne_id
      await doScrape(name, city, context, '');
    } else if (candidates.length === 1) {
      // Un seul candidat → sélection automatique
      await doScrape(name, city, context, candidates[0].rne_id);
    } else {
      // Plusieurs candidats → afficher la liste de sélection
      showCandidates(candidates, name, city, context);
      searchBtnText.textContent = 'Lancer la recherche';
      searchSpinner.classList.add('hidden');
      searchBtn.disabled = false;
    }
  } catch (err) {
    _stopColdTimer();
    showError('Impossible de joindre le serveur. Vérifiez votre connexion.');
    searchBtnText.textContent = 'Lancer la recherche';
    searchSpinner.classList.add('hidden');
    searchBtn.disabled = false;
  }
});

async function doScrape(name, city, context, rne_id) {
  searchBtnText.textContent = 'Recherche en cours…';
  searchSpinner.classList.remove('hidden');
  searchBtn.disabled = true;
  _startColdTimer();
  try {
    const res = await fetch(API + '/scrape', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, city, context, rne_id })
    });
    _stopColdTimer();

    let data;
    try {
      data = await res.json();
    } catch (_) {
      // Réponse non-JSON : probablement timeout serveur
      showError(`Délai dépassé (serveur trop lent). Réessayez dans quelques secondes. [HTTP ${res.status}]`);
      return;
    }

    if (!res.ok) { showError(data.error || `Erreur serveur (${res.status})`); return; }
    hideCandidates();
    renderResult(data);
  } catch (err) {
    _stopColdTimer();
    showError(`Impossible de joindre le serveur : ${err.message}`);
  } finally {
    searchBtnText.textContent = 'Lancer la recherche';
    searchSpinner.classList.add('hidden');
    searchBtn.disabled = false;
  }
}

function showCandidates(candidates, name, city, context) {
  const box = document.getElementById('candidatesBox');
  const list = document.getElementById('candidatesList');
  list.innerHTML = candidates.map((c, i) => `
    <div class="candidate-item" onclick="selectCandidate(${i})">
      <div class="candidate-name">${escHtmlSimple(c.name_fr)}</div>
      ${c.name_ar ? `<div class="candidate-name-ar">${escHtmlSimple(c.name_ar)}</div>` : ''}
      <div class="candidate-id">ID RNE : ${escHtmlSimple(c.rne_id)}</div>
    </div>
  `).join('');
  // Stocker les candidats pour la sélection
  list._candidates = candidates;
  list._ctx = { name, city, context };
  box.classList.remove('hidden');
}

function hideCandidates() {
  document.getElementById('candidatesBox')?.classList.add('hidden');
}

function selectCandidate(idx) {
  const list = document.getElementById('candidatesList');
  const { name, city, context } = list._ctx;
  const rne_id = list._candidates[idx].rne_id;
  hideCandidates();
  doScrape(name, city, context, rne_id);
}

function escHtmlSimple(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function renderResult(data) {
  document.getElementById('resName').textContent = data.name || '—';
  // Cache badge inline avec le nom
  const cityEl = document.getElementById('resCity');
  cityEl.textContent = data.city || '';
  const oldCacheBadge = document.getElementById('resCacheBadge');
  if (oldCacheBadge) oldCacheBadge.remove();
  if (data.from_cache) {
    const cb = document.createElement('span');
    cb.id = 'resCacheBadge';
    cb.className = 'cache-badge';
    cb.textContent = '⚡ Depuis le cache';
    cityEl.after(cb);
  }

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

  // Membres RNE — section dédiée avec cartes
  const allRow = document.getElementById('allContactsRow');
  let html = '';

  if (data.members && data.members.length) {
    html += '<div class="members-section"><div class="members-title">Bureau de la copropriété (RNE)</div><div class="members-grid">';
    for (const m of data.members) {
      const roleClass = m.qualite === 'Président' ? 'role-president' :
                         m.qualite === 'Trésorier' ? 'role-tresorier' : 'role-other';
      const roleIcon = m.qualite === 'Président' ? '\u{1F451}' :
                       m.qualite === 'Secrétaire Général' ? '\u{1F4DD}' :
                       m.qualite === 'Trésorier' ? '\u{1F4B0}' : '\u{1F464}';
      html += `<div class="member-card ${roleClass}">
        <div class="member-role">${roleIcon} ${escHtmlSimple(m.qualite)}</div>
        <div class="member-name">${escHtmlSimple(m.nom)}</div>
        ${m.nom_ar && m.nom_ar !== m.nom ? `<div class="member-name-latin">${escHtmlSimple(m.nom_latin || '')}</div>` : ''}
      </div>`;
    }
    html += '</div></div>';
  } else if (data.president) {
    html += `<div class="members-section"><div class="members-title">Responsable (RNE)</div>
      <div class="member-card role-president"><div class="member-role">\u{1F451} Président</div>
      <div class="member-name">${escHtmlSimple(data.president)}</div></div></div>`;
  }

  if (data.address) {
    html += `<div class="address-row"><strong>\u{1F4CD} Adresse officielle :</strong> ${escHtmlSimple(data.address)}</div>`;
  }

  if (data.all_phones && data.all_phones.length > 1)
    html += `<div class="extras-row"><strong>Tous les téléphones :</strong> ${data.all_phones.join(' · ')}</div>`;
  if (data.all_emails && data.all_emails.length > 1)
    html += `<div class="extras-row"><strong>Tous les emails :</strong> ${data.all_emails.join(' · ')}</div>`;

  // Bouton relancer sans cache
  if (data.from_cache) {
    html += `<div style="margin-top:.75rem"><button class="btn-secondary" id="retryNoCache">Relancer sans cache</button></div>`;
  }

  if (html) {
    allRow.innerHTML = html;
    allRow.classList.remove('hidden');
    // Bind retry button
    const retryBtn = document.getElementById('retryNoCache');
    if (retryBtn) {
      retryBtn.addEventListener('click', async () => {
        await fetch(API + '/cache/invalidate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: data.name, city: data.city })
        });
        doScrape(data.name, data.city, '', data.rne_id_found || '');
      });
    }
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
    const wrapper = document.createElement('span');
    wrapper.style.cssText = 'display:flex;align-items:center;gap:.4rem';
    if (href) {
      const a = document.createElement('a');
      a.href = href;
      a.target = '_blank';
      a.textContent = value;
      wrapper.appendChild(a);
    } else {
      const t = document.createElement('span');
      t.textContent = value;
      wrapper.appendChild(t);
    }
    const copyBtn = document.createElement('button');
    copyBtn.className = 'btn-copy';
    copyBtn.title = 'Copier';
    copyBtn.textContent = '\u{1F4CB}';
    copyBtn.addEventListener('click', () => {
      navigator.clipboard.writeText(value).then(() => {
        copyBtn.textContent = '\u2705';
        setTimeout(() => { copyBtn.textContent = '\u{1F4CB}'; }, 1200);
      });
    });
    wrapper.appendChild(copyBtn);
    valueEl.appendChild(wrapper);
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

/* ── Import RNE ── */
const rneUploadZone   = document.getElementById('rneUploadZone');
const rneFileInput    = document.getElementById('rneFileInput');
const rneFileNameEl   = document.getElementById('rneFileName');
const rneImportBtn    = document.getElementById('rneImportBtn');
const rneImportText   = document.getElementById('rneImportText');
const rneImportSpin   = document.getElementById('rneImportSpinner');
const rneResult       = document.getElementById('rneResult');
const rneError        = document.getElementById('rneError');

rneUploadZone.addEventListener('click', (e) => { if (e.target !== rneFileInput) rneFileInput.click(); });
rneUploadZone.addEventListener('dragover', (e) => { e.preventDefault(); rneUploadZone.classList.add('drag-over'); });
rneUploadZone.addEventListener('dragleave', () => rneUploadZone.classList.remove('drag-over'));
rneUploadZone.addEventListener('drop', (e) => {
  e.preventDefault(); rneUploadZone.classList.remove('drag-over');
  if (e.dataTransfer.files[0]) handleRneFile(e.dataTransfer.files[0]);
});
rneFileInput.addEventListener('change', () => { if (rneFileInput.files[0]) handleRneFile(rneFileInput.files[0]); });

function handleRneFile(file) {
  rneFileNameEl.textContent = '📄 ' + file.name;
  rneFileNameEl.classList.remove('hidden');
  rneImportBtn.disabled = false;
  rneImportBtn._file = file;
}

rneImportBtn.addEventListener('click', async () => {
  const file = rneImportBtn._file;
  if (!file) return;
  rneImportText.textContent = 'Import en cours…';
  rneImportSpin.classList.remove('hidden');
  rneImportBtn.disabled = true;
  rneResult.classList.add('hidden');
  rneError.classList.add('hidden');
  try {
    const fd = new FormData();
    fd.append('file', file);
    const res = await fetch(API + '/import/seed', { method: 'POST', body: fd });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Erreur serveur');
    rneResult.textContent = `✔ ${data.inserted} nouveaux syndics importés sur ${data.total} lignes. Allez sur le tableau de bord pour voir la liste.`;
    rneResult.classList.remove('hidden');
  } catch (err) {
    rneError.textContent = '⚠ ' + err.message;
    rneError.classList.remove('hidden');
  } finally {
    rneImportText.textContent = 'Importer les syndics';
    rneImportSpin.classList.add('hidden');
    rneImportBtn.disabled = false;
  }
});

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

  const context = (document.getElementById('excelContext')?.value || '').trim();

  enrichText.textContent = 'Envoi en cours…';
  enrichSpin.classList.remove('hidden');
  enrichBtn.disabled = true;
  enrichProg.classList.remove('hidden');
  enrichErr.classList.add('hidden');

  const progText = document.getElementById('enrichProgText');
  const progBar  = document.getElementById('enrichProgBar');

  try {
    // ── Étape 1 : démarrer le job ──────────────────────────────────────────
    const fd = new FormData();
    fd.append('file', file);
    if (context) fd.append('context', context);

    const startRes = await fetch(API + '/enrich/start', { method: 'POST', body: fd });
    const startData = await startRes.json();
    if (!startRes.ok) throw new Error(startData.error || 'Erreur démarrage');

    const jobId = startData.job_id;
    enrichText.textContent = 'Enrichissement en cours…';

    // ── Étape 2 : polling statut ───────────────────────────────────────────
    await new Promise((resolve, reject) => {
      const poll = setInterval(async () => {
        try {
          const st = await fetch(`${API}/enrich/status/${jobId}`).then(r => r.json());
          if (st.status === 'running' || st.status === 'pending') {
            const pct = st.total > 0 ? Math.round((st.progress / st.total) * 100) : 0;
            if (progText) progText.textContent = `${st.progress} / ${st.total} lignes traitées (${pct}%)`;
            if (progBar)  progBar.style.width = pct + '%';
          } else if (st.status === 'done') {
            clearInterval(poll);
            if (progText) progText.textContent = 'Terminé ! Téléchargement…';
            if (progBar)  progBar.style.width = '100%';
            resolve();
          } else if (st.status === 'error') {
            clearInterval(poll);
            reject(new Error(st.error || 'Erreur enrichissement'));
          }
        } catch (e) {
          clearInterval(poll);
          reject(e);
        }
      }, 3000);
    });

    // ── Étape 3 : télécharger le fichier ──────────────────────────────────
    const dlRes = await fetch(`${API}/enrich/download/${jobId}`);
    if (!dlRes.ok) {
      const err = await dlRes.json().catch(() => ({ error: 'Erreur téléchargement' }));
      throw new Error(err.error || 'Erreur téléchargement');
    }
    const blob = await dlRes.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = 'contacts_enrichis.xlsx';
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
    if (progBar) progBar.style.width = '0%';
  }
});
