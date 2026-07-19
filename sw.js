// ── Service Worker ──────────────────────────────────────────────
// アプリシェル（HTML/アイコン/CDN）をキャッシュし、オフラインでも起動可能にする。
// 株価・クラウド同期などの動的データは常にネットワークから取得（キャッシュしない）。
const CACHE = 'portfolio-v8';
const SHELL = ['./', './index.html', './favicon.svg', './manifest.json'];

// データ系リクエストはキャッシュせず常にネットワークへ（鮮度が命）
const BYPASS_HOSTS = [
  'onrender.com',      // Render バックエンド (/api/*)
  'supabase.co',       // クラウド同期
  'yahoo.com',         // Yahoo Finance US
  'yahoo.co.jp',       // Yahoo Finance Japan
  'allorigins.win',    // CORS プロキシ
  'codetabs.com',      // CORS プロキシ
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE)
      .then(c => c.addAll(SHELL))
      .then(() => self.skipWaiting())
      .catch(() => self.skipWaiting())  // シェル取得失敗時も登録は継続
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(async keys => {
        const oldKeys = keys.filter(k => k !== CACHE);
        await Promise.all(oldKeys.map(k => caches.delete(k)));
        await self.clients.claim();
        if (oldKeys.length) {
          const windows = await self.clients.matchAll({ type: 'window' });
          windows.forEach(client => client.postMessage({ type: 'NEW_VERSION' }));
        }
      })
  );
});

self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;

  let url;
  try { url = new URL(req.url); } catch { return; }

  // 動的データ（API・株価・クラウド）はキャッシュせずネットワーク直結
  if (url.pathname.startsWith('/api/') || BYPASS_HOSTS.some(h => url.hostname.endsWith(h))) return;

  // ページ遷移（=アプリ起動）: network-first。
  // オンライン時は必ず最新版を表示し、通信失敗時だけキャッシュへフォールバックする。
  if (req.mode === 'navigate') {
    e.respondWith(
      fetch(req, { cache: 'no-store' })
        .then(async response => {
          if (response && response.ok) {
            const cache = await caches.open(CACHE);
            await Promise.all([
              cache.put(req, response.clone()),
              cache.put('./index.html', response.clone()),
            ]);
          }
          return response;
        })
        .catch(async () => (await caches.match(req)) || (await caches.match('./index.html')))
    );
    return;
  }

  // 静的資産・CDN: stale-while-revalidate（キャッシュ即返し＋裏で更新）
  e.respondWith(
    caches.match(req).then(cached => {
      const network = fetch(req)
        .then(r => {
          if (r && (r.ok || r.type === 'opaque')) {
            const copy = r.clone();
            caches.open(CACHE).then(c => c.put(req, copy));
          }
          return r;
        })
        .catch(() => cached);
      return cached || network;
    })
  );
});
