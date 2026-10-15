// Newest release first. The first entry also supplies the header version.
// Keep entries about user-visible changes; do not invent historical release dates.
const ESTUI_RELEASES = [
  {
    version: '3.1.1',
    date: '2026-10-15',
    summary: 'Recovery and cluster-selection fixes',
    changes: [
      ['Fixed', 'Abandon restores temporarily opened sources before discarding recovery state. Connection, identity or restoration failures keep the checkpoint available.'],
      ['Fixed', 'Migration selects indices only on the source side. Changing direction or tabs clears selections to prevent applying actions on the wrong cluster. Indices named constructor can be selected normally.'],
      ['Fixed', 'Reverse migration preview uses the selected indices on Node B, and history shows the correct host direction.'],
      ['Fixed', 'An unavailable destination no longer hides a healthy source in the Indices tab.'],
      ['Fixed', 'Invalid or partial document counts and disk statistics are rejected. Disk watermarks reflect configured overrides.']
    ]
  },
  {
    version: '3.1.0',
    date: '2026-10-15',
    summary: 'Live copies and easier index browsing',
    changes: [
      ['Added', 'Optional live copy without adding a source write block. Verification compares captured document IDs, routing and contents with the destination, and clearly states that later source changes are not synchronized.'],
      ['Improved', 'Closed sources can be temporarily opened for live copy and restored afterward. Saved state supports recovery through Continue after an interruption.'],
      ['Changed', 'Destinations stay write-blocked during verification. After success, they are closed and match the source’s original write-block state. Continue reapplies a temporary block for verification.'],
      ['Added', 'Sort index tables by name, creation date, open/closed status or size. Sorting covers all matching indices before pagination and keeps selections and filters.'],
      ['Added', 'Clickable version label and changelog in the top-right corner.']
    ]
  },
  {
    version: '3.0',
    summary: 'Previous application baseline',
    changes: [
      ['Supported', 'Migrate between compatible clusters within Elasticsearch 7, 8 or 9; cross-major migrations are rejected. Elasticsearch 7 requires version 7.14 or later.'],
      ['Included', 'Mapping reindex with pasted or uploaded mappings, editable destination names, and explicit original-index retention or backup/removal choices.'],
      ['Included', 'Document verification, saved migration progress and index-level Continue, plus explicit destination collision handling.'],
      ['Included', 'Paginated index lists with search across all indices, filters preserved after actions, and closed-index search preview with state restoration.']
    ]
  }
];

(function initChangelog() {
  const button = document.getElementById('version-button');
  const dialog = document.getElementById('changelog-dialog');
  const entries = document.getElementById('changelog-entries');
  const current = ESTUI_RELEASES[0].version;
  button.textContent = 'v' + current;
  button.setAttribute('aria-label', 'estui v' + current + ' — view changelog');

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text) node.textContent = text;
    return node;
  }
  ESTUI_RELEASES.forEach(function(release, index) {
    const section = element('section', 'changelog-release');
    const heading = element('div', 'changelog-release-heading');
    heading.appendChild(element('h3', '', 'v' + release.version));
    if (index === 0) heading.appendChild(element('span', 'changelog-current', 'Current'));
    if (release.date) {
      const date = element('time', '', release.date);
      date.dateTime = release.date;
      heading.appendChild(date);
    }
    section.appendChild(heading);
    section.appendChild(element('p', 'changelog-summary', release.summary));
    const list = element('ul', 'changelog-list');
    release.changes.forEach(function(change) {
      const item = element('li');
      item.appendChild(element('span', 'changelog-kind', change[0]));
      item.appendChild(element('span', '', change[1]));
      list.appendChild(item);
    });
    section.appendChild(list);
    entries.appendChild(section);
  });
  button.addEventListener('click', function() { dialog.showModal(); });
  document.getElementById('changelog-close').addEventListener('click', function() { dialog.close(); });
  dialog.addEventListener('click', function(event) {
    const rect = dialog.getBoundingClientRect();
    if (event.target === dialog && (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom)) dialog.close();
  });
  dialog.addEventListener('close', function() { button.focus(); });
})();
