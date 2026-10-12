// Keep complete cluster lists in S. Filtering and bulk selection operate on those
// lists; pagination only limits the rows rendered in an individual panel.
function indexPage(key, list, query, cluster) {
  if (!S.pages) S.pages = {};
  var state = S.pages[key];
  if (!state) state = S.pages[key] = {page: 1, size: 25};
  if (state.query !== query || state.cluster !== cluster) state.page = 1;
  state.query = query;
  state.cluster = cluster;
  var matches = Q(list, query), pages = Math.max(1, Math.ceil(matches.length / state.size));
  state.page = Math.max(1, Math.min(state.page, pages));
  var start = (state.page - 1) * state.size;
  return {matches: matches, rows: matches.slice(start, start + state.size), page: state.page,
    pages: pages, size: state.size, start: matches.length ? start + 1 : 0, end: Math.min(start + state.size, matches.length)};
}
function indexPager(key, result) {
  var labels = {indices: 'Indices', source: 'Node A indices', target: 'Node B indices'};
  var label = labels[key], count = result.matches.length;
  var h = '<div class="index-pager" role="group" aria-label="' + label + ' pagination">';
  h += '<span class="index-range">' + result.start + '–' + result.end + ' of ' + count + '</span>';
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
  if (key === 'indices') renderIndices(); else renderMigration();
  var body = document.getElementById(key + '-rows');
  if (body) body.scrollTop = 0;
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
