(function(){
'use strict';

const DB_NAME='424_offline_database_v1';
const DB_VERSION=1;
const TABLE_STORE='tables';
const QUEUE_STORE='queue';
const META_STORE='meta';
const UUID_TABLES=new Set([
  'tenant_profiles','tenant_questions','tenant_answers','tenant_attachments',
  'tenant_other_information','tenant_units','tenant_tags','tenant_custom_sections',
  'tenant_room_transfers','tenant_emergency_contacts','elec_meter_reading_photos'
]);
const UPSERT_KEYS={
  elec_app_settings:['setting_key'],
  elec_month_locks:['month'],
  tenant_answers:['tenant_id','question_id']
};

let idbPromise=null;
let realClient=null;
let wrappedClient=null;
let syncingPromise=null;
let statusMounted=false;
let lastError='';

function nowIso(){return new Date().toISOString();}
function uuid(){
  if(globalThis.crypto && crypto.randomUUID) return crypto.randomUUID();
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,c=>{
    const r=Math.random()*16|0,v=c==='x'?r:(r&0x3|0x8);return v.toString(16);
  });
}
function clone(v){
  if(v===undefined) return undefined;
  try{return structuredClone(v);}catch{return JSON.parse(JSON.stringify(v));}
}
function networkError(error){
  const text=String(error?.message||error||'').toLowerCase();
  return text.includes('failed to fetch') || text.includes('network') || text.includes('load failed') || text.includes('fetch');
}
function openDB(){
  if(idbPromise) return idbPromise;
  idbPromise=new Promise((resolve,reject)=>{
    const req=indexedDB.open(DB_NAME,DB_VERSION);
    req.onupgradeneeded=()=>{
      const db=req.result;
      if(!db.objectStoreNames.contains(TABLE_STORE)) db.createObjectStore(TABLE_STORE,{keyPath:'name'});
      if(!db.objectStoreNames.contains(QUEUE_STORE)) db.createObjectStore(QUEUE_STORE,{keyPath:'id',autoIncrement:true});
      if(!db.objectStoreNames.contains(META_STORE)) db.createObjectStore(META_STORE,{keyPath:'key'});
    };
    req.onsuccess=()=>resolve(req.result);
    req.onerror=()=>reject(req.error);
  });
  return idbPromise;
}
async function storeGet(store,key){
  const db=await openDB();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(store,'readonly');
    const req=tx.objectStore(store).get(key);
    req.onsuccess=()=>resolve(req.result);
    req.onerror=()=>reject(req.error);
  });
}
async function storePut(store,value){
  const db=await openDB();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(store,'readwrite');
    tx.oncomplete=()=>resolve(value);
    tx.onerror=()=>reject(tx.error);
    tx.objectStore(store).put(value);
  });
}
async function storeAdd(store,value){
  const db=await openDB();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(store,'readwrite');
    const req=tx.objectStore(store).add(value);
    req.onsuccess=()=>resolve(req.result);
    req.onerror=()=>reject(req.error);
  });
}
async function storeDelete(store,key){
  const db=await openDB();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(store,'readwrite');
    tx.oncomplete=()=>resolve();
    tx.onerror=()=>reject(tx.error);
    tx.objectStore(store).delete(key);
  });
}
async function storeAll(store){
  const db=await openDB();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(store,'readonly');
    const req=tx.objectStore(store).getAll();
    req.onsuccess=()=>resolve(req.result||[]);
    req.onerror=()=>reject(req.error);
  });
}
async function setMeta(key,value){return storePut(META_STORE,{key,value});}
async function getMeta(key){return (await storeGet(META_STORE,key))?.value;}
async function getTable(name){return clone((await storeGet(TABLE_STORE,name))?.rows||[]);}
async function putTable(name,rows){await storePut(TABLE_STORE,{name,rows:clone(rows||[]),updated_at:nowIso()});}
async function pendingCount(){return (await storeAll(QUEUE_STORE)).length;}

function matchesFilter(row,f){
  const value=row?.[f.field];
  if(f.op==='eq') return String(value??'')===String(f.value??'');
  if(f.op==='gt') return Number(value)>Number(f.value);
  return true;
}
function parseOrTerm(term){
  const parts=term.split('.');
  if(parts.length<3) return null;
  return {field:parts[0],op:parts[1],value:parts.slice(2).join('.')};
}
function applyOr(rows,expr){
  if(!expr) return rows;
  const terms=String(expr).split(',').map(parseOrTerm).filter(Boolean);
  if(!terms.length) return rows;
  return rows.filter(row=>terms.some(t=>matchesFilter(row,t)));
}
function applyFilters(rows,filters){return rows.filter(row=>filters.every(f=>matchesFilter(row,f)));}
function applyOrders(rows,orders){
  const out=[...rows];
  for(let i=orders.length-1;i>=0;i--){
    const o=orders[i];
    out.sort((a,b)=>{
      const av=a?.[o.field],bv=b?.[o.field];
      if(av===bv) return 0;
      if(av===null||av===undefined) return o.ascending?1:-1;
      if(bv===null||bv===undefined) return o.ascending?-1:1;
      const c=String(av).localeCompare(String(bv),undefined,{numeric:true,sensitivity:'base'});
      return o.ascending?c:-c;
    });
  }
  return out;
}
function projectColumns(rows,columns){
  if(!columns || columns==='*') return clone(rows);
  const names=String(columns).split(',').map(x=>x.trim()).filter(Boolean);
  return rows.map(row=>{
    const o={};
    for(const n of names){ if(Object.prototype.hasOwnProperty.call(row,n)) o[n]=row[n]; }
    return o;
  });
}
function sameKey(a,b,keys){return keys.every(k=>String(a?.[k]??'')===String(b?.[k]??''));}
function inferKeys(table,row){
  if(UPSERT_KEYS[table]) return UPSERT_KEYS[table];
  if(row?.id!==undefined && row?.id!==null) return ['id'];
  if(table==='elec_tenant_bills' && row?.month!==undefined && row?.tenant_id) return ['month','tenant_id'];
  return [];
}
function prepareRow(table,row){
  const out=clone(row||{});
  if(UUID_TABLES.has(table) && !out.id) out.id=uuid();
  if(UUID_TABLES.has(table) && !out.created_at) out.created_at=nowIso();
  if(UUID_TABLES.has(table) && !out.updated_at && !['tenant_tags'].includes(table)) out.updated_at=nowIso();
  if(table==='tenant_room_transfers' && !out.transferred_at) out.transferred_at=nowIso();
  return out;
}
async function mergeCache(table,rows,replace=false){
  if(!Array.isArray(rows)) return;
  if(replace){await putTable(table,rows);return;}
  const existing=await getTable(table);
  for(const row of rows){
    const keys=inferKeys(table,row);
    if(!keys.length){ existing.push(clone(row)); continue; }
    const idx=existing.findIndex(x=>sameKey(x,row,keys));
    if(idx>=0) existing[idx]={...existing[idx],...clone(row)};
    else existing.push(clone(row));
  }
  await putTable(table,existing);
}
async function applyLocalMutation(table,op,payload,filters,upsertOptions){
  let rows=await getTable(table);
  let affected=[];
  if(op==='insert'){
    const list=(Array.isArray(payload)?payload:[payload]).map(x=>prepareRow(table,x));
    rows.push(...list); affected=list;
  }else if(op==='update'){
    rows=rows.map(row=>{
      if(filters.every(f=>matchesFilter(row,f))){
        const updated={...row,...clone(payload)};
        if(UUID_TABLES.has(table) && !['tenant_tags'].includes(table)) updated.updated_at=nowIso();
        affected.push(updated);return updated;
      }
      return row;
    });
  }else if(op==='delete'){
    const keep=[];
    for(const row of rows){
      if(filters.every(f=>matchesFilter(row,f))) affected.push(row); else keep.push(row);
    }
    rows=keep;
  }else if(op==='upsert'){
    const list=(Array.isArray(payload)?payload:[payload]).map(x=>prepareRow(table,x));
    for(const item of list){
      let keys=[];
      if(upsertOptions?.onConflict) keys=String(upsertOptions.onConflict).split(',').map(x=>x.trim());
      if(!keys.length) keys=inferKeys(table,item);
      const idx=keys.length?rows.findIndex(x=>sameKey(x,item,keys)):-1;
      if(idx>=0){rows[idx]={...rows[idx],...item};affected.push(rows[idx]);}
      else{rows.push(item);affected.push(item);}
    }
  }
  await putTable(table,rows);
  return affected;
}
async function enqueue(task){
  const id=await storeAdd(QUEUE_STORE,{...task,created_at:nowIso(),tries:0});
  window.dispatchEvent(new CustomEvent('offline424queuechange'));
  updateStatus();
  return id;
}

async function replayTask(task){
  if(task.kind==='db'){
    let q=realClient.from(task.table);
    if(task.op==='insert') q=q.insert(task.payload);
    else if(task.op==='update') q=q.update(task.payload);
    else if(task.op==='delete') q=q.delete();
    else if(task.op==='upsert') q=q.upsert(task.payload,task.upsertOptions||undefined);
    for(const f of task.filters||[]){
      if(f.op==='eq') q=q.eq(f.field,f.value);
      else if(f.op==='gt') q=q.gt(f.field,f.value);
    }
    const result=await q;
    if(result?.error) throw result.error;
    return;
  }
  if(task.kind==='storageUpload'){
    const result=await realClient.storage.from(task.bucket).upload(task.path,task.file,task.options||{});
    if(result?.error) throw result.error;
    return;
  }
  if(task.kind==='storageRemove'){
    const result=await realClient.storage.from(task.bucket).remove(task.paths||[]);
    if(result?.error) throw result.error;
  }
}
async function syncQueue(){
  if(syncingPromise) return syncingPromise;
  syncingPromise=(async()=>{
    if(!realClient || !navigator.onLine){updateStatus();return {ok:false,offline:true};}
    const tasks=(await storeAll(QUEUE_STORE)).sort((a,b)=>a.id-b.id);
    for(const task of tasks){
      try{
        await replayTask(task);
        await storeDelete(QUEUE_STORE,task.id);
        lastError='';
      }catch(err){
        lastError=String(err?.message||err||'Sync failed');
        console.error('Offline sync stopped:',err,task);
        updateStatus();
        return {ok:false,error:err};
      }
    }
    await setMeta('last_sync',nowIso());
    window.dispatchEvent(new CustomEvent('offline424synced'));
    updateStatus();
    return {ok:true};
  })().finally(()=>{syncingPromise=null;});
  return syncingPromise;
}

class OfflineQuery{
  constructor(table){
    this.table=table;this.op='select';this.columns='*';this.payload=null;this.filters=[];this.orders=[];this.orExpr='';this.singleFlag=false;this.returning=false;this.upsertOptions=null;
  }
  select(columns='*'){this.columns=columns;if(this.op!=='select')this.returning=true;return this;}
  insert(payload){this.op='insert';this.payload=payload;return this;}
  update(payload){this.op='update';this.payload=payload;return this;}
  delete(){this.op='delete';return this;}
  upsert(payload,options){this.op='upsert';this.payload=payload;this.upsertOptions=options||null;return this;}
  eq(field,value){this.filters.push({op:'eq',field,value});return this;}
  gt(field,value){this.filters.push({op:'gt',field,value});return this;}
  or(expr){this.orExpr=expr;return this;}
  order(field,opts={}){this.orders.push({field,ascending:opts.ascending!==false});return this;}
  single(){this.singleFlag=true;return this;}
  then(resolve,reject){return this.execute().then(resolve,reject);}
  async executeRemote(){
    let q=realClient.from(this.table);
    if(this.op==='select') q=q.select(this.columns||'*');
    else if(this.op==='insert') q=q.insert(this.payload);
    else if(this.op==='update') q=q.update(this.payload);
    else if(this.op==='delete') q=q.delete();
    else if(this.op==='upsert') q=q.upsert(this.payload,this.upsertOptions||undefined);
    if(this.op!=='select' && this.returning) q=q.select(this.columns||'*');
    for(const f of this.filters){
      if(f.op==='eq') q=q.eq(f.field,f.value);
      else if(f.op==='gt') q=q.gt(f.field,f.value);
    }
    if(this.orExpr) q=q.or(this.orExpr);
    for(const o of this.orders) q=q.order(o.field,{ascending:o.ascending});
    if(this.singleFlag) q=q.single();
    return await q;
  }
  async executeLocal(){
    if(this.op==='select'){
      let rows=await getTable(this.table);
      rows=applyFilters(rows,this.filters);
      rows=applyOr(rows,this.orExpr);
      rows=applyOrders(rows,this.orders);
      rows=projectColumns(rows,this.columns);
      if(this.singleFlag){
        if(rows.length) return {data:rows[0],error:null,offline:true};
        return {data:null,error:{code:'PGRST116',message:'No cached row found while offline.'},offline:true};
      }
      return {data:rows,error:null,offline:true};
    }
    const preparedPayload=(this.op==='insert'||this.op==='upsert')
      ? (Array.isArray(this.payload)?this.payload.map(x=>prepareRow(this.table,x)):prepareRow(this.table,this.payload))
      : clone(this.payload);
    this.payload=preparedPayload;
    const affected=await applyLocalMutation(this.table,this.op,preparedPayload,this.filters,this.upsertOptions);
    await enqueue({kind:'db',table:this.table,op:this.op,payload:preparedPayload,filters:clone(this.filters),upsertOptions:this.upsertOptions});
    let data=null;
    if(this.returning) data=this.singleFlag?(affected[0]||null):affected;
    return {data,error:null,offline:true,queued:true};
  }
  async execute(){
    await openDB();
    if(this.op==='insert'||this.op==='upsert'){
      this.payload=Array.isArray(this.payload)
        ? this.payload.map(x=>prepareRow(this.table,x))
        : prepareRow(this.table,this.payload);
    }
    if(navigator.onLine && realClient){
      await syncQueue();
      try{
        const result=await this.executeRemote();
        if(result?.error && networkError(result.error)){
          console.warn('Network unavailable; using offline copy.',result.error);
          return this.executeLocal();
        }
        if(!result?.error){
          if(this.op==='select'){
            const raw=Array.isArray(result.data)?result.data:(result.data?[result.data]:[]);
            const fullSelect=(this.columns==='*'||!this.columns) && this.filters.length===0 && !this.orExpr;
            if(raw.length || fullSelect) await mergeCache(this.table,raw,fullSelect);
          }else{
            const prepared=(this.op==='insert'||this.op==='upsert')
              ? (Array.isArray(this.payload)?this.payload.map(x=>prepareRow(this.table,x)):prepareRow(this.table,this.payload))
              : this.payload;
            const localAffected=await applyLocalMutation(this.table,this.op,prepared,this.filters,this.upsertOptions);
            if(result.data){
              const remoteRows=Array.isArray(result.data)?result.data:[result.data];
              await mergeCache(this.table,remoteRows,false);
            }else if(this.op==='insert' && localAffected.length===0){
              // no-op
            }
          }
          await setMeta('last_sync',nowIso());
          updateStatus();
        }
        return result;
      }catch(err){
        if(!networkError(err)) throw err;
        console.warn('Network unavailable; using offline copy.',err);
      }
    }
    return this.executeLocal();
  }
}

function wrapStorage(client){
  return {
    from(bucket){
      const realBucket=client.storage.from(bucket);
      return {
        async upload(path,file,options={}){
          if(navigator.onLine){
            try{
              const result=await realBucket.upload(path,file,options);
              if(!result?.error){await setMeta('last_sync',nowIso());updateStatus();return result;}
              if(!networkError(result.error)) return result;
            }catch(err){if(!networkError(err)) throw err;}
          }
          await enqueue({kind:'storageUpload',bucket,path,file,options});
          return {data:{path},error:null,offline:true,queued:true};
        },
        async remove(paths){
          if(navigator.onLine){
            try{
              const result=await realBucket.remove(paths);
              if(!result?.error){await setMeta('last_sync',nowIso());updateStatus();return result;}
              if(!networkError(result.error)) return result;
            }catch(err){if(!networkError(err)) throw err;}
          }
          await enqueue({kind:'storageRemove',bucket,paths:clone(paths)});
          return {data:paths,error:null,offline:true,queued:true};
        },
        getPublicUrl(path){return realBucket.getPublicUrl(path);}
      };
    }
  };
}

function wrapSupabase(client){
  realClient=client;
  if(wrappedClient) return wrappedClient;
  wrappedClient={
    from(table){return new OfflineQuery(table);},
    storage:wrapStorage(client),
    _onlineClient:client
  };
  openDB().then(()=>{if(navigator.onLine)syncQueue();updateStatus();}).catch(console.error);
  return wrappedClient;
}

function formatSyncTime(value){
  if(!value) return 'Never';
  try{return new Date(value).toLocaleString();}catch{return value;}
}
async function updateStatus(){
  const box=document.getElementById('offline424Status');
  if(!box) return;
  const count=await pendingCount().catch(()=>0);
  const last=await getMeta('last_sync').catch(()=>null);
  const online=navigator.onLine;
  const main=box.querySelector('[data-offline-main]');
  const detail=box.querySelector('[data-offline-detail]');
  const dot=box.querySelector('[data-offline-dot]');
  if(dot) dot.className='offline424-dot '+(online?(count?'waiting':'online'):'offline');
  if(main){
    if(!online) main.textContent=`OFFLINE — ${count} change${count===1?'':'s'} waiting to sync`;
    else if(count) main.textContent=`ONLINE — ${count} change${count===1?'':'s'} waiting to sync`;
    else main.textContent='ONLINE — Synced';
  }
  if(detail) detail.textContent=`Last cloud sync: ${formatSyncTime(last)}${lastError?' • '+lastError:''}`;
  const btn=box.querySelector('button');
  if(btn) btn.disabled=!online || count===0;
}
function injectStyles(){
  if(document.getElementById('offline424Styles')) return;
  const s=document.createElement('style');s.id='offline424Styles';s.textContent=`
#offline424Status{display:flex;align-items:center;gap:9px;flex-wrap:wrap;margin:8px 0 12px;padding:8px 11px;border:1px solid rgba(255,255,255,.14);border-radius:11px;background:rgba(15,23,42,.78);color:#fff;font:600 12px Arial,sans-serif;box-shadow:0 4px 18px rgba(0,0,0,.12)}
#offline424Status .offline424-dot{width:9px;height:9px;border-radius:50%;display:inline-block;box-shadow:0 0 0 3px rgba(255,255,255,.06)}
#offline424Status .offline424-dot.online{background:#22c55e}#offline424Status .offline424-dot.waiting{background:#f59e0b}#offline424Status .offline424-dot.offline{background:#ef4444}
#offline424Status .offline424-main{font-weight:800}#offline424Status .offline424-detail{color:#cbd5e1;font-weight:500;font-size:11px;flex:1 1 240px}
#offline424Status button{padding:6px 9px;border:0;border-radius:8px;background:#475569;color:#fff;font-weight:700;cursor:pointer;font-size:11px}#offline424Status button:disabled{opacity:.45;cursor:not-allowed}
@media(max-width:650px){#offline424Status{align-items:flex-start}#offline424Status .offline424-detail{width:100%;flex-basis:100%}}
`;
  document.head.appendChild(s);
}
function mountStatus(){
  if(statusMounted || document.getElementById('offline424Status')) return;
  statusMounted=true;injectStyles();
  const box=document.createElement('div');box.id='offline424Status';box.setAttribute('role','status');box.setAttribute('aria-live','polite');
  box.innerHTML=`<span data-offline-dot class="offline424-dot online"></span><span data-offline-main class="offline424-main">Checking connection…</span><span data-offline-detail class="offline424-detail">Last cloud sync: —</span><button type="button">↻ Sync Now</button>`;
  box.querySelector('button').addEventListener('click',async()=>{lastError='';await syncQueue();updateStatus();});
  const nav=document.querySelector('.site-nav');
  const h1=document.querySelector('h1');
  if(nav && nav.parentNode) nav.insertAdjacentElement('afterend',box);
  else if(h1 && h1.parentNode) h1.insertAdjacentElement('beforebegin',box);
  else document.body.prepend(box);
  updateStatus();
}
async function registerServiceWorker(){
  if(!('serviceWorker' in navigator)) return;
  try{await navigator.serviceWorker.register('sw.js');}catch(err){console.warn('Service worker registration failed:',err);}
}
window.addEventListener('online',async()=>{lastError='';await syncQueue();updateStatus();});
window.addEventListener('offline',()=>updateStatus());
window.addEventListener('offline424queuechange',()=>updateStatus());
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible'&&navigator.onLine)syncQueue();});
window.addEventListener('pageshow',()=>{if(navigator.onLine)syncQueue();updateStatus();});

window.Offline424={
  wrapSupabase,
  syncNow:syncQueue,
  pendingCount,
  getCachedTable:getTable,
  putCachedTable:putTable,
  mountStatus,
  registerServiceWorker,
  isOnline:()=>navigator.onLine
};

document.addEventListener('DOMContentLoaded',()=>{mountStatus();registerServiceWorker();});
})();
