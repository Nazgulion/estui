// Keep complete cluster lists in S. Filtering and bulk selection operate on those
// lists; sorting precedes pagination and never mutates the cluster lists.
function compareIndexNames(a, b) {
  return a.name.localeCompare(b.name, undefined, {numeric: true}) || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
}
function sortIndices(list, field, direction) {
  function value(index) {
    if (field === 'status') return index.status === 'open' ? 0 : index.status === 'close' || index.status === 'closed' ? 1 : null;
    var number = index[field === 'created' ? 'created_at' : 'size_bytes'];
    return typeof number === 'number' && Number.isFinite(number) && number >= 0 ? number : null;
  }
  return list.slice().sort(function(a, b) {
    var order;
    if (field === 'name') order = compareIndexNames(a, b);
    else {
      var av = value(a), bv = value(b);
      // Missing metrics always come last, in either direction.
      if (av === null || bv === null) return av === bv ? compareIndexNames(a, b) : av === null ? 1 : -1;
      order = av - bv;
    }
    return (direction === 'desc' ? -order : order) || compareIndexNames(a, b);
  });
}
function indexCreatedCell(index) {
  if (typeof index.created_at !== 'number' || !Number.isFinite(index.created_at)) return '<td class="index-created">—</td>';
  var date = new Date(index.created_at);
  if (!Number.isFinite(date.getTime())) return '<td class="index-created">—</td>';
  var iso = date.toISOString();
  return '<td class="index-created" title="' + E(iso) + '">' + E(iso.slice(0,16).replace('T',' ')) + '</td>';
}
function indexPage(key, list, query, cluster) {
  if (!S.pages) S.pages = {};
  var state = S.pages[key];
  if (!state) state = S.pages[key] = {page: 1, size: 25};
  if (!state.sort) state.sort = 'name';
  if (!state.direction) state.direction = 'asc';
  if (state.query !== query || state.cluster !== cluster) state.page = 1;
  state.query = query;
  state.cluster = cluster;
  var matches = sortIndices(Q(list, query), state.sort, state.direction), pages = Math.max(1, Math.ceil(matches.length / state.size));
  state.page = Math.max(1, Math.min(state.page, pages));
  var start = (state.page - 1) * state.size;
  return {matches: matches, rows: matches.slice(start, start + state.size), page: state.page,
    pages: pages, size: state.size, sort: state.sort, direction: state.direction,
    start: matches.length ? start + 1 : 0, end: Math.min(start + state.size, matches.length)};
}
function indexPager(key, result) {
  var labels = {indices: 'Indices', source: 'Node A indices', target: 'Node B indices'};
  var label = labels[key], count = result.matches.length;
  var h = '<div class="index-pager" role="group" aria-label="' + label + ' pagination">';
  h += '<span class="index-range">' + result.start + '–' + result.end + ' of ' + count + '</span>';
  h += '<div class="index-sort"><label for="' + key + '-sort-field">Sort by</label><select id="' + key + '-sort-field" class="search-box" data-pagination-control="true" aria-label="' + label + ' sort by" onchange="setIndexSort(\'' + key + '\',this.value)">';
  [['name','Name'],['created','Date created'],['status','Open / closed'],['size','Size']].forEach(function(option) {
    h += '<option value="' + option[0] + '"' + (result.sort === option[0] ? ' selected' : '') + '>' + option[1] + '</option>';
  });
  var directions = {name:['A–Z','Z–A'],created:['Oldest first','Newest first'],status:['Open first','Closed first'],size:['Smallest first','Largest first']}[result.sort];
  h += '</select><select id="' + key + '-sort-direction" class="search-box" data-pagination-control="true" aria-label="' + label + ' sort order" onchange="setIndexSortDirection(\'' + key + '\',this.value)">';
  ['asc','desc'].forEach(function(direction, i) {
    h += '<option value="' + direction + '"' + (result.direction === direction ? ' selected' : '') + '>' + directions[i] + '</option>';
  });
  h += '</select></div>';
  h += '<label class="index-page-size">Rows <select class="search-box" data-pagination-control="true" aria-label="' + label + ' rows per page" onchange="setIndexPageSize(\'' + key + '\',this.value)">';
  [10,25,50,100].forEach(function(size) { h += '<option value="' + size + '"' + (size === result.size ? ' selected' : '') + '>' + size + '</option>'; });
  h += '</select></label><div class="index-page-nav">';
  function button(page, text, title, disabled) {
    return '<button class="page-btn" data-click="page" data-list="' + key + '" data-page="' + page + '" aria-label="' + label + ': ' + title + '" title="' + title + '"' + (disabled ? ' disabled' : '') + '>' + text + '</button>';
  }
  h += button(1, '«', 'First page', result.page === 1);
  h += button(result.page - 1, '‹', 'Previous page', result.page === 1);
  h += '<label class="index-page-number">Page <input class="search-box" type="number" inputmode="numeric" min="1" max="' + result.pages + '" value="' + result.page + '" data-pagination-control="true" aria-label="' + label + ' page number" onchange="setIndexPage(\'' + key + '\',this.value)"> <span>of ' + result.pages + '</span></label>';
  h += button(result.page + 1, '›', 'Next page', result.page === result.pages);
  h += button(result.pages, '»', 'Last page', result.page === result.pages);
  return h + '</div></div>';
}
function renderIndexPage(key) {
  var active = document.activeElement, focusID = active && active.id;
  if (key === 'indices') renderIndices(); else renderMigration();
  var body = document.getElementById(key + '-rows');
  if (body) body.scrollTop = 0;
  if (focusID === key + '-sort-field' || focusID === key + '-sort-direction') {
    var control = document.getElementById(focusID);
    if (control) control.focus();
  }
}
function setIndexSort(key, field) {
  if (!['name','created','status','size'].includes(field) || !S.pages || !S.pages[key]) return;
  var state = S.pages[key];
  state.sort = field;
  state.direction = field === 'created' || field === 'size' ? 'desc' : 'asc';
  state.page = 1;
  renderIndexPage(key);
}
function setIndexSortDirection(key, direction) {
  if (!['asc','desc'].includes(direction) || !S.pages || !S.pages[key]) return;
  S.pages[key].direction = direction;
  S.pages[key].page = 1;
  renderIndexPage(key);
}
function setIndexPage(key, value) {
  if (!S.pages || !S.pages[key]) return;
  var page = Number(value);
  if (!Number.isInteger(page)) return;
  S.pages[key].page = page;
  renderIndexPage(key); // indexPage clamps to the current filtered range.
}
function setIndexPageSize(key, value) {
  var size = Number(value);
  if (![10,25,50,100].includes(size) || !S.pages || !S.pages[key]) return;
  S.pages[key].size = size;
  S.pages[key].page = 1;
  renderIndexPage(key);
}
function setIndexFilter(key, value) {
  var filters = {indices: 'indicesFilter', source: 'migrationFilter', target: 'targetFilter'};
  S[filters[key]] = value;
  if (S.pages && S.pages[key]) S.pages[key].page = 1;
  renderIndexPage(key);
}
