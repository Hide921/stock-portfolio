// ── Service Worker ──────────────────────────────────────────────
// アプリシェル（HTML/アイコン/CDN）をキャッシュし、オフラインでも起動可能にする。
// 株価・クラウド同期などの動的データは常にネットワークから取得（キャッシュしない）。
const CACHE = 'portfolio-v2';
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
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;

  let url;
  try { url = new URL(req.url); } catch { return; }

  // 動的データ（API・株価・クラウド）はキャッシュせずネットワーク直結
  if (url.pathname.startsWith('/api/') || BYPASS_HOSTS.some(h => url.hostname.endsWith(h))) return;

  // ページ遷移（=アプリ起動）: cache-first / stale-while-revalidate。
  // キャッシュ済みシェルを即返して一瞬で起動し、裏でネットワーク取得して
  // キャッシュを更新（最新は次回起動で反映）。これにより Render Free の
  // コールドスタートやスリープ復帰の往復で起動が待たされなくなる。
  if (req.mode === 'navigate') {
    e.respondWith(
      caches.match(req).then(cached => cached || caches.match('./index.html')).then(cached => {
        const network = fetch(req)
          .then(async r => {
            if (r && r.ok) {
              const cache = await caches.open(CACHE);
              cache.put(req, r.clone());
              cache.put('./index.html', r.clone());
              // 取得した新版がキャッシュ済みと中身が違えば、開いている画面に通知して
              // その場での更新（再読み込み）を促す。index.html だけのデプロイも検知できる。
              if (cached) {
                const [fresh, old] = await Promise.all([r.clone().text(), cached.clone().text()]);
                if (fresh !== old) {
                  const wins = await self.clients.matchAll({ type: 'window' });
                  wins.forEach(c => c.postMessage({ type: 'NEW_VERSION' }));
                }
              }
            }
            return r;
          })
          .catch(() => cached);
        // キャッシュがあれば即起動。なければ（初回など）ネットワークを待つ。
        return cached || network;
      })
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
