// Mapping reindex is a separate, explicit plan. Connection identities and source
// UUIDs are captured when the editor opens, rather than read at submission time.
function remapName(prefix, name) {
  var bytes = new Uint8Array(8);
  crypto.getRandomValues(bytes);
  return prefix + Array.from(name).slice(0, 45).join('') + '_' + Array.from(bytes, function(b) { return b.toString(16).padStart(2, '0'); }).join('');
}
function startRemap() {
  var names = Object.keys(S.selected);
  if (!names.length) return;
  var source = S.dir === 'target->source' ? S.target : S.source;
  var draft = {
    names: names, description: S.desc || '', direction: S.dir, query: S.query || '',
    source_uuid: S.srcInfo && S.srcInfo.cluster_uuid,
    target_uuid: S.tgtInfo && S.tgtInfo.cluster_uuid,
    from: S.dir === 'target->source' ? S.tgtHost : S.srcHost,
    to: S.dir === 'target->source' ? S.srcHost : S.tgtHost,
    plans: names.map(function(name) {
      var index = source.find(function(i) { return i.name === name; });
      return {name: name, source_uuid: index && index.uuid, mapping: '', destination: remapName('tmp_reindexed_', name), original_action: 'keep', backup_index: remapName('original_', name)};
    })
  };
  if (draft.plans.some(function(p) { return !p.source_uuid; })) return alert('Select indices from the source side of the chosen direction.');
  showRemapEditor(draft);
}
function remapShell(title, body, buttons, step) {
  document.getElementById('modal-container').innerHTML = '<div class="modal-overlay remap-overlay"><section class="modal remap-modal" role="dialog" aria-modal="true" aria-labelledby="remap-title"><div class="remap-header"><h3 id="remap-title">' + E(title) + '</h3><span class="remap-step">' + (step === 2 ? '2 / 2 · Review' : '1 / 2 · Configure') + '</span></div><div class="remap-body">' + body + '</div><div class="remap-footer"><button class="btn remap-secondary" id="remap-cancel">Cancel</button><div class="remap-footer-actions">' + buttons + '</div></div></section></div>';
  document.getElementById('remap-cancel').addEventListener('click', closeModal);
}
function remapRoute(draft) {
  return '<div class="remap-route"><div><span class="remap-route-label">Source</span><span class="remap-route-host">' + E(draft.from || '') + '</span></div><span class="remap-route-arrow" aria-hidden="true">→</span><div><span class="remap-route-label">Destination</span><span class="remap-route-host">' + E(draft.to || '') + '</span></div></div>';
}
function remapIndexHeading(name, i) {
  return '<div class="remap-index-heading"><span>Index ' + (i + 1) + '</span><h4>' + E(name) + '</h4></div>';
}
function showRemapEditor(draft) {
  var h = '<p class="remap-intro">Create new indices with your mappings. Default verification requires write-blocked sources; live copy adds no write block and temporarily opens closed sources.</p>' + remapRoute(draft);
  h += liveSourceConsent('remap-live-source', !!draft.allow_live_source);
  draft.plans.forEach(function(p, i) {
    h += '<section class="remap-index">' + remapIndexHeading(p.name, i) + '<div class="remap-fields">';
    h += '<label class="remap-field"><span class="remap-label">New destination name</span><input id="remap-dest-' + i + '" value="' + E(p.destination) + '" spellcheck="false" autocomplete="off"></label>';
    h += '<div class="remap-field"><div class="remap-mapping-toolbar"><label class="remap-label" for="remap-json-' + i + '">Mapping JSON</label><input class="remap-file" aria-label="Upload mapping JSON for ' + E(p.name) + '" id="remap-file-' + i + '" type="file" accept=".json,application/json,text/plain"></div><textarea id="remap-json-' + i + '" spellcheck="false" rows="8" aria-describedby="remap-help-' + i + '" placeholder=\'{"properties":{"field":{"type":"keyword"}}}\'>' + E(p.mapping) + '</textarea>';
    h += '<details class="remap-help" id="remap-help-' + i + '"><summary>Accepted formats &amp; limits · 1 MiB max</summary><p>Paste a mappings object, {"mappings": ...}, or this index’s GET _mapping response. Settings and aliases are not accepted. Existing analysis settings are reused; new analyzers must already be defined there.</p></details></div>';
    h += '<div class="remap-options"><label class="remap-field"><span class="remap-label">After successful verification</span><select id="remap-action-' + i + '"><option value="keep"' + (p.original_action === 'keep' ? ' selected' : '') + '>Keep original unchanged</option><option value="backup"' + (p.original_action === 'backup' ? ' selected' : '') + '>Back up original, then remove old name</option><option value="delete"' + (p.original_action === 'delete' ? ' selected' : '') + '>Remove original without backup</option></select></label>';
    h += '<label class="remap-field" id="remap-backup-label-' + i + '" style="display:' + (p.original_action === 'backup' ? 'flex' : 'none') + '"><span class="remap-label">Backup name · source cluster</span><input id="remap-backup-' + i + '" value="' + E(p.backup_index) + '" spellcheck="false" autocomplete="off"></label></div></div></section>';
  });
  remapShell('Reindex with mapping', h, '<button class="btn" id="remap-review">Review steps →</button>', 1);
  var pending = 0, review = document.getElementById('remap-review');
  document.getElementById('remap-live-source').addEventListener('change', function() {
    draft.allow_live_source = this.checked;
    draft.plans.forEach(function(p, i) {
      var action = document.getElementById('remap-action-' + i);
      action.disabled = !!draft.allow_live_source;
      if (draft.allow_live_source) {
        p.original_action = 'keep'; action.value = 'keep';
        document.getElementById('remap-backup-label-' + i).style.display = 'none';
      }
    });
  });
  draft.plans.forEach(function(p, i) {
    document.getElementById('remap-action-' + i).disabled = !!draft.allow_live_source;
    [['dest','destination'],['json','mapping'],['backup','backup_index']].forEach(function(pair) {
      document.getElementById('remap-' + pair[0] + '-' + i).addEventListener('input', function() { p[pair[1]] = this.value; });
    });
    document.getElementById('remap-action-' + i).addEventListener('change', function() {
      p.original_action = this.value;
      document.getElementById('remap-backup-label-' + i).style.display = this.value === 'backup' ? 'flex' : 'none';
    });
    document.getElementById('remap-file-' + i).addEventListener('change', async function() {
      var file = this.files[0]; if (!file) return;
      if (file.size > 1048576) return alert('Mapping file exceeds 1 MiB.');
      var textarea = document.getElementById('remap-json-' + i);
      pending++; review.disabled = true; textarea.disabled = true; this.disabled = true;
      try { var raw = await file.text(); validateMappingText(raw); p.mapping = raw; textarea.value = raw; }
      catch (e) { alert('Cannot load mapping: ' + e.message); }
      finally { pending--; review.disabled = pending > 0; textarea.disabled = false; this.disabled = false; }
    });
  });
  review.addEventListener('click', function() {
    if (pending) return;
    try {
      draft.plans.forEach(function(p) {
        if (draft.allow_live_source && p.original_action !== 'keep') throw Error('Live copy requires Keep original unchanged; source backup/removal is disabled.');
        validateMappingText(p.mapping);
        p.destination = p.destination.trim(); p.backup_index = p.backup_index.trim();
        if (!p.destination || p.destination === p.name) throw Error('Choose a new destination name for ' + p.name);
        if (p.original_action === 'backup' && (!p.backup_index || p.backup_index === p.name || p.backup_index === p.destination)) throw Error('Choose a distinct backup name for ' + p.name);
        if (p.original_action === 'delete' && draft.query && draft.query !== '*') throw Error('Filtered reindex requires keeping the original or making a full backup.');
      });
      showRemapReview(draft);
    } catch (e) { alert(e.message); }
  });
}
function validateMappingText(raw) {
  if (new TextEncoder().encode(raw).length > 1048576) throw Error('Mapping exceeds 1 MiB.');
  var mapping = JSON.parse(raw);
  if (!mapping || Array.isArray(mapping) || typeof mapping !== 'object') throw Error('Mapping must be a JSON object.');
  // Preserve the original text in the request: JSON.parse/stringify would round
  // mapping parameters such as a long field's null_value above 2^53.
}
function remapRequest(draft, acknowledged) {
  var remaps = Object.create(null);
  draft.plans.forEach(function(p) {
    remaps[p.name] = {mapping: p.mapping, destination: p.destination, original_action: p.original_action,
      backup_index: p.original_action === 'backup' ? p.backup_index : '', source_uuid: p.source_uuid,
      confirm_removal: p.original_action !== 'keep' && acknowledged};
  });
  return {indices: draft.names.slice(), description: draft.description, direction: draft.direction, query: draft.query,
    source_uuid: draft.source_uuid, target_uuid: draft.target_uuid, allow_live_source: !!draft.allow_live_source, remaps: remaps};
}
function showRemapReview(draft) {
  var removal = draft.plans.some(function(p) { return p.original_action !== 'keep'; });
  var h = remapRoute(draft);
  if (draft.allow_live_source) h += '<p class="live-summary"><strong>Live copy — no write block.</strong> Source mappings/settings are retained. Closed sources are temporarily opened and closed again afterward, including on failure. After a forced shutdown, restart and Continue to restore their state. Verification covers captured documents only, not later source changes. Original removal and overwrite are disabled.</p>';
  if (draft.query) h += '<p class="remap-filter">Destination document filter: <code>' + E(draft.query) + '</code>. Any backup includes <strong>all</strong> original documents.</p>';
  draft.plans.forEach(function(p, i) {
    h += '<section class="remap-index">' + remapIndexHeading(p.name, i) + '<ol class="remap-steps"><li>' + (draft.allow_live_source ? 'Check cluster and source identities; require a new destination. Save the original source state, then temporarily open it if closed.' : 'Check cluster identities, source identity and write block; require unused destination and backup names.') + '</li>';
    h += '<li>Create <strong>' + E(p.destination) + '</strong> on the destination with the supplied mapping and compatible source settings.</li>';
    h += '<li>' + (draft.allow_live_source ? 'Record a fingerprint and count of documents captured during copying. Independently read the destination and compare IDs, routing and contents with that fingerprint; verify the supplied mapping. Later source changes are not synchronized or verified.' : 'Copy documents, then verify IDs, routing, contents, and the supplied mapping. Mapping or document errors stop the operation.') + '</li>';
    if (p.original_action === 'backup') h += '<li>Create <strong>' + E(p.backup_index) + '</strong> on the <strong>source cluster</strong>, using the original mapping and all original documents. Independently verify the backup and close it with writes blocked.</li>';
    if (p.original_action !== 'keep') h += '<li class="remap-destructive">Only after verification, permanently remove source index <strong>' + E(p.name) + '</strong> and its aliases. ' + (p.original_action === 'delete' ? '<strong>No original backup will be created.</strong>' : 'The original data and mapping remain in the verified backup.') + '</li>';
    else h += '<li>Keep the original index and mapping under <strong>' + E(p.name) + '</strong>; ' + (draft.allow_live_source ? 'retain its mappings/settings and restore its initial open/closed state.' : 'restore its initial open/closed state.') + '</li>';
    h += '<li>After successful verification, close the new index and match the source’s original write-block state. Reopening it and updating application aliases are separate operator actions.</li></ol>';
    h += '<details class="remap-mapping-preview"><summary>Supplied mapping</summary><pre>' + E(p.mapping) + '</pre></details></section>';
  });
  h += '<p class="remap-note">' + (draft.allow_live_source ? 'Partial copies are retained after failure. Continue retries incomplete indices into fresh copies using a new source read; it does not synchronize later writes into finished copies.' : 'Backup copies preserve documents and mappings, not aliases or lifecycle policies. Partial copies are retained after failure; Continue may allocate a fresh name, shown in the job log. An uncertain original deletion stops for manual reconciliation.') + '</p>';
  if (removal) h += '<label class="remap-ack"><input id="remap-ack" type="checkbox"><span>I confirm removal of the original indices listed above after verification, including their aliases.</span></label>';
  remapShell('Review reindex steps', h, '<button class="btn remap-secondary" id="remap-back">Back</button><button class="btn' + (removal ? ' btn-danger' : '') + '" id="remap-start">Start reindex</button>', 2);
  document.getElementById('remap-back').addEventListener('click', function() { showRemapEditor(draft); });
  document.getElementById('remap-start').addEventListener('click', async function() {
    var acknowledged = !removal || document.getElementById('remap-ack').checked;
    if (!acknowledged) return alert('Confirm removal of the original indices before starting.');
    var request = remapRequest(draft, acknowledged), button = this; button.disabled = true;
    try { await POST('/api/migrate', request); closeModal(); S.selected = {}; renderMigration(); }
    catch (e) { alert('Reindex did not start: ' + e); button.disabled = false; }
  });
}

function remapSummary(r) {
  if (!r || !r.options || !r.options.mapping) return '';
  var text = r.source_deleted ? 'Original removed' : 'Original retained';
  if (r.backup_index) text += ' · Backup on source: ' + r.backup_index;
  if (r.phase === 'delete_pending') text = 'Original deletion outcome unknown; manual reconciliation required · Destination and backup copies retained';
  return '<div style="font-size:11px;color:var(--muted-fg)">' + E(text) + '</div>';
}
