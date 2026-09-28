const CACHE='424-systems-offline-v2';
const CORE=[
  './','./index.html','./electricity.html','./tenants.html','./tindahan.html',
  './offline-sync.js','./manifest.webmanifest'
];
const EXTERNAL=[
  'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2',
  'https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js',
  'https://cdn.jsdelivr.net/npm/jspdf-autotable@3.8.4/dist/jspdf.plugin.autotable.min.js'
];

self.addEventListener('install',event=>{
  event.waitUntil((async()=>{
    const cache=await caches.open(CACHE);
    for(const url of CORE){
      try{await cache.add(url);}catch(e){/* file may not exist yet; runtime caching will handle it */}
    }
    for(const url of EXTERNAL){
      try{
        const response=await fetch(url,{mode:'no-cors'});
        await cache.put(url,response.clone());
      }catch(e){/* if first install is offline, runtime caching will fill this later */}
    }
    await self.skipWaiting();
  })());
});

self.addEventListener('activate',event=>{
  event.waitUntil((async()=>{
    const keys=await caches.keys();
    await Promise.all(keys.filter(k=>k!==CACHE && k.startsWith('424-systems-offline-')).map(k=>caches.delete(k)));
    await self.clients.claim();
  })());
});

function isPublicSupabaseStorage(url){return /\.supabase\.co$/i.test(url.hostname) && url.pathname.includes('/storage/v1/object/public/');}
function isSupabaseApi(url){return /\.supabase\.co$/i.test(url.hostname) && !isPublicSupabaseStorage(url);}
function isStaticCdn(url){return /cdn\.jsdelivr\.net$/i.test(url.hostname);}

self.addEventListener('fetch',event=>{
  const req=event.request;
  if(req.method!=='GET') return;
  const url=new URL(req.url);

  // Never cache Supabase API responses. IndexedDB is the app's offline data source.
  if(isSupabaseApi(url)) return;

  if(req.mode==='navigate'){
    event.respondWith((async()=>{
      try{
        const fresh=await fetch(req);
        const cache=await caches.open(CACHE);
        cache.put(req,fresh.clone()).catch(()=>{});
        return fresh;
      }catch{
        const cache=await caches.open(CACHE);
        return (await cache.match(req)) || (await cache.match('./index.html')) || new Response('Offline',{status:503,headers:{'Content-Type':'text/plain'}});
      }
    })());
    return;
  }

  if(url.origin===self.location.origin || isStaticCdn(url) || isPublicSupabaseStorage(url)){
    event.respondWith((async()=>{
      const cache=await caches.open(CACHE);
      const cached=await cache.match(req);
      if(cached){
        fetch(req).then(r=>{if(r&&r.ok)cache.put(req,r.clone()).catch(()=>{});}).catch(()=>{});
        return cached;
      }
      try{
        const fresh=await fetch(req);
        if(fresh && (fresh.ok || fresh.type==='opaque')) cache.put(req,fresh.clone()).catch(()=>{});
        return fresh;
      }catch{
        return new Response('',{status:503,statusText:'Offline'});
      }
    })());
  }
});
