function liveSourceConsent(id, checked, change) {
  return '<label class="live-consent"><input type="checkbox" id="' + id + '" data-pagination-control="true"' + (checked ? ' checked' : '') + (change ? ' onchange="' + change + '"' : '') + '><span><strong>Allow live copy — no write block</strong><span>Important: estui will not block writes or alter source mappings/settings. Closed sources are temporarily opened, then closed again after copying or failure. A forced shutdown may leave them open until you restart and Continue. Verification checks the captured documents against the destination; later additions, updates and deletions are not synchronized. After successful verification, the destination is closed and its write block matches the source’s original state. The original is kept, and destination overwrite is disabled.</span></span></label>';
}
function liveCopySummary(index) {
  if (!index || !index.allow_live_source) return '';
  var text = index.status === 'copied_live'
    ? 'Captured documents verified. Current-source equality is not verified; later writes are not synchronized. Original source retained; initial open/closed state restored.'
    : 'Live copy: original source retained; closed sources are temporarily opened and restored. Verification covers captured documents only; later writes are not synchronized.';
  if (index.status === 'copied_live') text += index.source_write_blocked === false ? ' Destination closed; temporary write block removed to match the source.' : ' Destination closed with writes blocked.';
  return '<div class="live-summary">' + text + '</div>';
}
function migrationStatusLabel(status) { return status === 'copied_live' ? 'Live copy' : status; }
