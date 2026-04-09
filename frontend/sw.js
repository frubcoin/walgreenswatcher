const CACHE_NAME = 'walgreens-watcher-v3';
const APP_SHELL = [
  '/',
  '/index.html',
  '/admin.html',
  '/admin.css',
  '/admin.js',
  '/privacy.html',
  '/terms.html',
  '/disclosures.html',
  '/legal.css',
  '/manifest.webmanifest',
  '/frubgreens.webp',
  '/favicon.ico',
  '/icons/icon-192.png',
  '/icons/icon-512.png',
  '/topography.svg',
  '/settings.webp',
  '/refresh.webp',
  '/install_desktop.svg',
  '/power.webp',
  '/no-power.webp',
  '/out-of-stock.webp',
  '/ready-stock.webp',
  '/delete.png'
];
const DOCUMENT_FALLBACKS = {
  '/': '/index.html',
  '/index.html': '/index.html',
  '/dashboard': '/index.html',
  '/map': '/index.html',
  '/settings': '/index.html',
  '/trending': '/index.html',
  '/history': '/index.html',
  '/admin': '/admin.html',
  '/admin.html': '/admin.html',
  '/privacy': '/privacy.html',
  '/privacy.html': '/privacy.html',
  '/terms': '/terms.html',
  '/terms.html': '/terms.html',
  '/disclosures': '/disclosures.html',
  '/disclosures.html': '/disclosures.html'
};

function normalizePathname(pathname = '/') {
  return String(pathname || '').replace(/\/+$/, '') || '/';
}

async function fetchAndCache(request) {
  const response = await fetch(request);
  if (response && response.status === 200) {
    const responseClone = response.clone();
    const cache = await caches.open(CACHE_NAME);
    await cache.put(request, responseClone);
  }
  return response;
}

async function resolveNavigationFallback(url) {
  const pathname = normalizePathname(url.pathname);
  const fallbackPath = DOCUMENT_FALLBACKS[pathname];
  if (!fallbackPath) {
    return new Response('Offline', {
      status: 503,
      statusText: 'Offline',
      headers: { 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }

  return (await caches.match(url.pathname))
    || (await caches.match(fallbackPath))
    || new Response('Offline', {
      status: 503,
      statusText: 'Offline',
      headers: { 'Content-Type': 'text/plain; charset=utf-8' }
    });
}

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(APP_SHELL))
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_NAME)
          .map(key => caches.delete(key))
      )
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  const { request } = event;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;

  // Avoid caching authenticated or user-specific API payloads in the shared service-worker cache.
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(fetch(request));
    return;
  }

  if (url.pathname === '/runtime-config.js') {
    event.respondWith(
      fetchAndCache(request).catch(() => caches.match(request))
    );
    return;
  }

  if (request.mode === 'navigate') {
    event.respondWith(
      fetchAndCache(request).catch(() => resolveNavigationFallback(url))
    );
    return;
  }

  event.respondWith(
    caches.match(request).then(cachedResponse => {
      const networkFetch = fetchAndCache(request).catch(() => cachedResponse);

      return cachedResponse || networkFetch;
    })
  );
});
