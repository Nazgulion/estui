const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const html=fs.readFileSync(__dirname+'/static/index.html','utf8');
const liveJS=fs.readFileSync(__dirname+'/static/live.js','utf8');
const part=(from,to)=>html.slice(html.indexOf(from),html.indexOf(to,html.indexOf(from)));
function harness(choice,ack=true,local=false,live=false){
 let modal='',sent,click,warning;
 const ctx={S:{dir:'source->target',query:'status:active',source:[{name:'test',uuid:'source-index'}],target:[{name:'test',uuid:'confirmed-destination'}],selected:{test:true},srcInfo:{cluster_uuid:'source'},tgtInfo:{cluster_uuid:local?'source':'target'}},E:String,renderMigration(){},alert(e){warning=e},fetch:async(u,o)=>{sent=JSON.parse(o.body);return{ok:true,json:async()=>({})}},document:{querySelector(){return modal&&!local?{value:choice}:null},getElementById(id){
 if(id==='modal-container')return{set innerHTML(v){modal=v}};
 if(id==='overwrite-ack')return modal?{checked:ack}:null;
 if(id==='migration-live-source')return{checked:live};
 if(id==='mig-query')return{value:'status:active'};
 if(id==='modal-confirm')return{addEventListener(_,fn){click=fn}};
 return{addEventListener(){}};
 }}};
 vm.createContext(ctx);
 vm.runInContext(liveJS+part('async function doMigrate','async function doContinue')+part('function confirmMigration','function showConfirm')+part('function closeModal','async function runBulk'),ctx);
 return{ctx,click:()=>click(),sent:()=>sent,modal:()=>modal,warning:()=>warning};
}
for(const choice of ['rename','overwrite'])test('confirmation preserves choice '+choice,()=>{
 const h=harness(choice);h.ctx.confirmMigration(['test'],'review');h.ctx.S.srcInfo.cluster_uuid='changed';h.ctx.S.dir='target->source';h.ctx.S.target[0].uuid='replacement';h.click();
 assert.equal(h.sent().collision_action,choice);assert.equal(h.sent().source_uuid,'source');assert.equal(h.sent().direction,'source->target');
 assert.equal(h.sent().overwrite_uuids.test,choice==='overwrite'?'confirmed-destination':undefined);
});
test('overwrite requires explicit loss acknowledgement',()=>{
 const h=harness('overwrite',false);h.ctx.confirmMigration(['test'],'review');h.click();assert.equal(h.sent(),undefined);assert.match(h.warning(),/permanently deleted/);assert.match(h.modal(),/overwrite-ack/);
});
test('same cluster dialog uses prefix and offers no overwrite',()=>{
 const h=harness('overwrite',false,true);h.ctx.confirmMigration(['test'],'review');assert.match(h.modal(),/reindex_test_/);assert.doesNotMatch(h.modal(),/value="overwrite"/);h.click();assert.equal(h.sent().collision_action,'rename');assert.deepEqual(h.sent().overwrite_uuids,{});
});
test('bulk failures are shown and remain selected',async()=>{
 const ctx={S:{selected:{a:true,b:true},srcInfo:{cluster_uuid:'source'}},E:String,renderIndices(){},loadAll:async()=>{},setTimeout(){},POST:async()=>({results:[{index:'a',ok:true},{index:'b',ok:false,error:'forbidden'}]})};
 vm.createContext(ctx);vm.runInContext(part('async function runBulk','function updateFooter'),ctx);await ctx.runBulk('close');
 assert.match(ctx.S.statusMsg,/1 succeeded, 1 failed/);assert.match(ctx.S.statusMsg,/forbidden/);assert.deepEqual(Object.keys(ctx.S.selected),['b']);
});
test('backend supplies displayed cluster identities',async()=>{
 const ctx={S:{},GET:async(u)=>u==='/api/clusters'?{source:{host:'https://production-a',cluster_uuid:'a'},target:{host:'https://production-b',cluster_uuid:'b'}}:u.includes('/indices/')?[{name:'logs'}]:{}};
 vm.createContext(ctx);vm.runInContext(part('async function loadAll','async function loadJobs'),ctx);await ctx.loadAll();assert.equal(ctx.S.srcHost,'https://production-a');assert.equal(ctx.S.tgtHost,'https://production-b');
});

const remapJS=fs.readFileSync(__dirname+'/static/remap.js','utf8');
function remapHarness(){
 let html='',sent,alerts=[]; const elements={};
 function el(id){return elements[id]||(elements[id]={value:'',checked:false,style:{},handlers:{},addEventListener(event,fn){this.handlers[event]=fn}})}
 const ctx={S:{selected:{test:true},dir:'source->target',source:[{name:'test',uuid:'source-index'}],target:[],srcInfo:{cluster_uuid:'source'},tgtInfo:{cluster_uuid:'dest'},srcHost:'https://source',tgtHost:'https://dest'},E:s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;'),crypto:require('node:crypto').webcrypto,TextEncoder,Uint8Array,alert:e=>alerts.push(String(e)),renderMigration(){},POST:async(_,r)=>{sent=r},closeModal(){html=''},document:{getElementById(id){if(id==='modal-container')return{set innerHTML(v){html=v}};return el(id)}}};
 vm.createContext(ctx);vm.runInContext(liveJS+remapJS,ctx);
 return{ctx,el,html:()=>html,sent:()=>sent,alerts,click:async id=>el(id).handlers.click.call(el(id)),input(id,value,event='input'){const e=el(id);e.value=value;e.handlers[event].call(e)}};
}
test('mapping editor reviews editable names and pins identities',async()=>{
 const h=remapHarness();h.ctx.startRemap();assert.match(h.html(),/tmp_reindexed_test_/);
 const raw='{"properties":{"v":{"type":"long","null_value":9007199254740993}}}';
 h.input('remap-json-0',raw);h.input('remap-dest-0','my_reindexed_test');h.input('remap-action-0','backup','change');h.input('remap-backup-0','original_saved_test');
 await h.click('remap-review');assert.match(h.html(),/my_reindexed_test/);assert.match(h.html(),/original_saved_test/);assert.match(h.html(),/all original documents/);
 h.ctx.S.srcInfo.cluster_uuid='changed';h.ctx.S.source[0].uuid='changed';h.ctx.S.dir='target->source';
 await h.click('remap-start');assert.equal(h.sent(),undefined);assert.match(h.alerts.at(-1),/Confirm removal/);
 h.el('remap-ack').checked=true;await h.click('remap-start');
 const req=h.sent();assert.equal(req.source_uuid,'source');assert.equal(req.direction,'source->target');assert.equal(req.remaps.test.source_uuid,'source-index');assert.equal(req.remaps.test.mapping,raw);assert.equal(req.remaps.test.destination,'my_reindexed_test');assert.equal(req.remaps.test.confirm_removal,true);
});
test('mapping file upload and Back preserve exact text',async()=>{
 const h=remapHarness();h.ctx.startRemap();const raw='{"mappings":{"properties":{"v":{"type":"keyword"}}}}';
 const file=h.el('remap-file-0');file.files=[{size:raw.length,text:async()=>raw}];await file.handlers.change.call(file);
 assert.equal(h.el('remap-json-0').value,raw);await h.click('remap-review');await h.click('remap-back');assert.match(h.html(),/keyword/);await h.click('remap-review');await h.click('remap-start');assert.equal(h.sent().remaps.test.mapping,raw);assert.equal(h.sent().remaps.test.original_action,'keep');
});
test('invalid or oversized uploaded mapping never starts migration',async()=>{
 const h=remapHarness();h.ctx.startRemap();h.input('remap-json-0','{oops');await h.click('remap-review');assert.equal(h.sent(),undefined);assert.ok(h.alerts.length);
 const file=h.el('remap-file-0');file.files=[{size:1048577,text:()=>{throw Error('must not read')}}];await file.handlers.change.call(file);assert.match(h.alerts.at(-1),/exceeds/);
});
test('filtered remap cannot remove original without a backup',async()=>{
 const h=remapHarness();h.ctx.S.query='v:1';h.ctx.startRemap();h.input('remap-json-0','{}');h.input('remap-action-0','delete','change');await h.click('remap-review');assert.match(h.alerts.at(-1),/full backup/);assert.equal(h.sent(),undefined);
});

const paginationJS=fs.readFileSync(__dirname+'/static/pagination.js','utf8');
function paginationHarness(){
 const main={innerHTML:'',scrollHeight:0,scrollTop:0,clientHeight:0,handlers:{},addEventListener(event,fn){this.handlers[event]=fn}};
 const ctx={document:{activeElement:null,getElementById:id=>id==='main'?main:null,querySelectorAll:()=>[]},setInterval(){},setTimeout(){},alert(e){throw Error(e)}};
 vm.createContext(ctx);
 vm.runInContext(liveJS+remapJS+paginationJS+part('var S=','\ninit();'),ctx);
 const rows=prefix=>Array.from({length:237},(_,i)=>({name:prefix+String(i).padStart(3,'0'),uuid:prefix+i,status:'open',health:'green',doc_count:1,size_human:'1kb',primary:1,replica:0}));
 Object.assign(ctx.S,{source:rows('a-'),target:rows('b-'),srcOK:true,tgtOK:true,srcInfo:{cluster_uuid:'a'},tgtInfo:{cluster_uuid:'b'}});
 return{ctx,main,visible:()=>Array.from(main.innerHTML.matchAll(/<tr[^>]*data-name="([^"]+)"/g),m=>m[1]),click(cmd,name){const el={dataset:{click:cmd,name}};main.handlers.click({target:{closest:selector=>selector==='[data-click]'?el:null}})}};
}
test('indices search reaches every index before pagination',()=>{
 const h=paginationHarness();h.ctx.renderIndices();assert.equal(h.visible().length,25);assert.equal(h.visible()[0],'a-000');
 h.ctx.setIndexPage('indices',7);assert.equal(h.visible()[0],'a-150');
 h.ctx.setIndexFilter('indices','^a-23[0-6]$');assert.deepEqual(h.visible(),['a-230','a-231','a-232','a-233','a-234','a-235','a-236']);assert.equal(h.ctx.S.pages.indices.page,1);
 h.ctx.setIndexFilter('indices','absent');assert.equal(h.visible().length,0);assert.match(h.main.innerHTML,/No matching indices/);assert.match(h.main.innerHTML,/0–0 of 0/);
 h.ctx.setIndexFilter('indices','');assert.equal(h.visible()[0],'a-000');assert.equal(h.ctx.S.source.length,237);
});
test('migration panels paginate and search independently across complete lists',()=>{
 const h=paginationHarness();h.ctx.renderMigration();assert.equal(h.visible().length,50);
 h.ctx.setIndexPage('source',4);h.ctx.setIndexPage('target',8);assert.equal(h.visible()[0],'a-075');assert.equal(h.visible()[25],'b-175');
 h.ctx.setIndexFilter('target','^b-236$');assert.equal(h.visible().at(-1),'b-236');assert.equal(h.ctx.S.pages.source.page,4);assert.equal(h.ctx.S.pages.target.page,1);
 h.ctx.setIndexFilter('source','^a-236$');assert.deepEqual(h.visible(),['a-236','b-236']);
});
test('selections persist across pages and All matches includes off-page results',()=>{
 const h=paginationHarness();h.ctx.renderIndices();h.click('idx','a-001');h.ctx.setIndexPage('indices',2);h.click('idx','a-026');assert.deepEqual(Object.keys(h.ctx.S.selected),['a-001','a-026']);
 h.ctx.setIndexFilter('indices','^a-1');h.click('idx-all');assert.equal(Object.keys(h.ctx.S.selected).length,102);assert.equal(h.ctx.S.selected['a-199'],true);
 h.click('idx-none');assert.deepEqual(Object.keys(h.ctx.S.selected),['a-001','a-026']);
 h.ctx.renderMigration();h.ctx.setIndexFilter('target','^b-1');h.click('mig-all-tgt');assert.equal(h.ctx.S.selected['b-199'],true);assert.equal(Object.keys(h.ctx.S.selected).length,102);
 h.click('mig-none-tgt');assert.deepEqual(Object.keys(h.ctx.S.selected),['a-001','a-026']);
});
test('refresh preserves pagination and clamps pages when index counts shrink',()=>{
 const h=paginationHarness();h.ctx.renderIndices();h.ctx.setIndexPage('indices',8);h.ctx.S.source=h.ctx.S.source.map(i=>({...i,status:'close'}));h.ctx.renderIndices();assert.equal(h.ctx.S.pages.indices.page,8);
 h.ctx.S.source=h.ctx.S.source.slice(0,30);h.ctx.renderIndices();assert.equal(h.ctx.S.pages.indices.page,2);assert.equal(h.visible().length,5);
 h.ctx.setIndexPageSize('indices',10);assert.equal(h.ctx.S.pages.indices.page,1);assert.equal(h.visible().length,10);
 h.ctx.setIndexPage('indices',999);assert.equal(h.ctx.S.pages.indices.page,3);h.ctx.setIndexPage('indices',-5);assert.equal(h.ctx.S.pages.indices.page,1);
 h.ctx.setIndexPage('indices',2);h.ctx.S.srcInfo.cluster_uuid='new-cluster';h.ctx.renderIndices();assert.equal(h.ctx.S.pages.indices.page,1);
});
test('bulk action keeps filter and current page',async()=>{
 const h=paginationHarness();h.ctx.renderIndices();h.ctx.setIndexFilter('indices','^a-1');h.ctx.setIndexPage('indices',3);h.ctx.S.selected={'a-151':true};
 h.ctx.POST=async()=>({results:[{index:'a-151',ok:true}]});h.ctx.loadAll=async()=>{};
 await h.ctx.runBulk('close');assert.equal(h.ctx.S.indicesFilter,'^a-1');assert.equal(h.ctx.S.pages.indices.page,3);assert.equal(h.visible()[0],'a-150');
});

test('sorting uses all filtered rows before paging and preserves source order and selection',()=>{
 const h=paginationHarness();
 h.ctx.S.source.forEach((row,i)=>{row.created_at=1700000000000+i*1000;row.size_bytes=i*1024;row.status=i%2?'close':'open'});
 h.ctx.renderIndices();h.click('idx','a-151');h.ctx.setIndexFilter('indices','^a-1');h.ctx.setIndexPage('indices',3);
 h.ctx.setIndexSort('indices','created');assert.equal(h.visible()[0],'a-199');assert.equal(h.ctx.S.pages.indices.page,1);
 h.ctx.setIndexPage('indices',2);assert.equal(h.visible()[0],'a-174');
 h.ctx.setIndexSortDirection('indices','asc');assert.equal(h.visible()[0],'a-100');
 h.ctx.setIndexSort('indices','size');assert.equal(h.visible()[0],'a-199');
 h.ctx.setIndexSortDirection('indices','asc');assert.equal(h.visible()[0],'a-100');
 h.ctx.setIndexSort('indices','status');assert.deepEqual(h.visible().slice(0,3),['a-100','a-102','a-104']);
 h.ctx.setIndexSortDirection('indices','desc');assert.deepEqual(h.visible().slice(0,3),['a-101','a-103','a-105']);
 h.ctx.setIndexSort('indices','name');h.ctx.setIndexSortDirection('indices','desc');assert.equal(h.visible()[0],'a-199');
 assert.equal(h.ctx.S.source[0].name,'a-000');assert.equal(h.ctx.S.source[236].name,'a-236');assert.equal(h.ctx.S.indicesFilter,'^a-1');assert.equal(h.ctx.S.selected['a-151'],true);
});

test('sorts numeric metrics with stable name ties and unknown values last in both directions',()=>{
 const h=paginationHarness();h.ctx.S.source=[
  {name:'index-10',size_bytes:10,created_at:10,status:'close'},
  {name:'index-2',size_bytes:10,created_at:10,status:'close'},
  {name:'zero',size_bytes:0,created_at:0,status:'open'},
  {name:'hundred',size_bytes:100,created_at:100,status:'open'},
  {name:'missing',size_bytes:null,created_at:null,status:''},
  {name:'unavailable'}
 ];h.ctx.renderIndices();
 for(const field of ['created','size']){
  h.ctx.setIndexSort('indices',field);assert.deepEqual(h.visible(),['hundred','index-2','index-10','zero','missing','unavailable']);
  h.ctx.setIndexSortDirection('indices','asc');assert.deepEqual(h.visible(),['zero','index-2','index-10','hundred','missing','unavailable']);
 }
 assert.match(h.main.innerHTML,/Created \(UTC\)/);assert.match(h.main.innerHTML,/1970-01-01 00:00/);assert.doesNotMatch(h.main.innerHTML,/Invalid Date/);
 h.ctx.setIndexSort('indices','status');assert.deepEqual(h.visible(),['hundred','zero','index-2','index-10','missing','unavailable']);
 h.ctx.setIndexSortDirection('indices','desc');assert.deepEqual(h.visible(),['index-2','index-10','hundred','zero','missing','unavailable']);
});

test('panels keep independent sort choices across refreshes, filtering and bulk actions',async()=>{
 const h=paginationHarness();h.ctx.renderIndices();h.ctx.setIndexSort('indices','name');h.ctx.setIndexSortDirection('indices','desc');
 h.ctx.renderMigration();h.ctx.setIndexSort('source','name');h.ctx.setIndexSortDirection('source','desc');
 h.ctx.S.target.forEach((row,i)=>{row.size_bytes=i});h.ctx.setIndexSort('target','size');
 assert.equal(h.visible()[0],'a-236');assert.equal(h.visible()[25],'b-236');
 h.ctx.setIndexFilter('source','^a-1');assert.equal(h.visible()[0],'a-199');assert.equal(h.ctx.S.pages.target.sort,'size');
 h.ctx.renderIndices();assert.equal(h.visible()[0],'a-236');h.ctx.setIndexPage('indices',3);h.ctx.S.selected={'a-151':true};
 h.ctx.POST=async()=>({results:[{index:'a-151',ok:true}]});h.ctx.loadAll=async()=>{};
 await h.ctx.runBulk('close');assert.equal(h.ctx.S.pages.indices.direction,'desc');assert.equal(h.ctx.S.pages.indices.page,3);assert.equal(h.visible()[0],'a-186');
 assert.match(h.main.innerHTML,/Indices sort by/);assert.match(h.main.innerHTML,/Indices sort order/);
});

test('live copy consent is explicit and cannot authorize destination overwrite',async()=>{
 const strict=harness('rename');strict.ctx.confirmMigration(['test'],'');await strict.click();assert.equal(strict.sent().allow_live_source,false);
 const live=harness('rename',true,false,true);live.ctx.confirmMigration(['test'],'');assert.match(live.modal(),/no write block/);assert.match(live.modal(),/Closed sources are temporarily opened/);assert.match(live.modal(),/forced shutdown/);assert.match(live.modal(),/later additions, updates and deletions are not synchronized/);await live.click();assert.equal(live.sent().allow_live_source,true);
 const overwrite=harness('overwrite',true,false,true);overwrite.ctx.confirmMigration(['test'],'');await overwrite.click();assert.equal(overwrite.sent(),undefined);assert.match(overwrite.warning(),/cannot overwrite/);
});

test('mapping live copy preserves consent through review and forces original retention',async()=>{
 const h=remapHarness();h.ctx.startRemap();h.input('remap-json-0','{}');h.input('remap-action-0','backup','change');
 const live=h.el('remap-live-source');live.checked=true;live.handlers.change.call(live);
 assert.equal(h.el('remap-action-0').disabled,true);assert.equal(h.el('remap-action-0').value,'keep');
 await h.click('remap-review');assert.match(h.html(),/captured documents only/);assert.match(h.html(),/fingerprint and count/);assert.match(h.html(),/temporarily open it if closed/);assert.match(h.html(),/restore its initial open\/closed state/);assert.doesNotMatch(h.html(),/permanently remove source index/);
 await h.click('remap-back');assert.match(h.html(),/id="remap-live-source"[^>]* checked/);assert.equal(h.el('remap-action-0').disabled,true);
 await h.click('remap-review');await h.click('remap-start');assert.equal(h.sent().allow_live_source,true);assert.equal(h.sent().remaps.test.original_action,'keep');assert.equal(h.sent().remaps.test.backup_index,'');
});

test('Continue sends checked live consent and completed live jobs explain verification scope',async()=>{
 const h=paginationHarness();h.ctx.S.state={active:{started_at:'2026-10-13',source:{host:'source'},dest:{host:'target'},indices:[{name:'test',status:'failed'}]}};
 h.ctx.renderMigration();assert.match(h.main.innerHTML,/continue-live-source/);
 const get=h.ctx.document.getElementById;h.ctx.document.getElementById=id=>id==='continue-live-source'?{checked:true}:get(id);
 let request;h.ctx.POST=async(url,body)=>{request={url,body}};await h.ctx.doContinue();assert.equal(request.url,'/api/migrate/continue');assert.equal(request.body.allow_live_source,true);
 h.ctx.S.state={history:[{allow_live_source:true,source:{host:'source'},dest:{host:'target'},indices:[{name:'test',status:'copied_live',allow_live_source:true}]}]};
 h.ctx.S.jobs=[{source_index:'test',target_index:'test',allow_live_source:true,status:'copied_live'}];h.ctx.renderMigration();assert.match(h.main.innerHTML,/Live copy/);assert.match(h.main.innerHTML,/Current-source equality is not verified/);assert.match(h.main.innerHTML,/pg-dot live/);
});

 test('live result explains restored destination write state',()=>{
  const h=paginationHarness();
  assert.match(h.ctx.liveCopySummary({allow_live_source:true,status:'copied_live',source_write_blocked:false}),/temporary write block removed/);
  assert.match(h.ctx.liveCopySummary({allow_live_source:true,status:'copied_live',source_write_blocked:true}),/writes blocked/);
 });
