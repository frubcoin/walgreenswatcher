const CACHE_NAME = 'walgreens-watcher-v1';
const APP_SHELL = [
  '/',
  '/index.html',
  '/runtime-config.js',
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
  '/trash.webp'
];

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

  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request).catch(() => caches.match('/index.html'))
    );
    return;
  }

  event.respondWith(
    caches.match(request).then(cachedResponse => {
      const networkFetch = fetch(request)
        .then(response => {
          if (response && response.status === 200) {
            const responseClone = response.clone();
            caches.open(CACHE_NAME).then(cache => cache.put(request, responseClone));
          }
          return response;
        })
        .catch(() => cachedResponse);

      return cachedResponse || networkFetch;
    })
  );
});
