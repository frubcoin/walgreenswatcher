
    const AUTH_BOOT_LOADER_STORAGE_KEY = 'walgreens_auth_boot_loader';

    try {
      if (localStorage.getItem(AUTH_BOOT_LOADER_STORAGE_KEY) !== '1') {
        document.body.classList.remove('app-booting');
        document.getElementById('page-loader')?.remove();
      }
    } catch (error) {
      document.body.classList.remove('app-booting');
      document.getElementById('page-loader')?.remove();
    }

    const DEFAULT_POKEMON_BACKGROUND_THEME = 'gyra';
    const DEFAULT_POKEMON_BACKGROUND_ENABLED = false;
    const DEFAULT_POKEMON_BACKGROUND_TILE_SIZE = 645;
    const MIN_POKEMON_BACKGROUND_TILE_SIZE = 200;
    const MAX_POKEMON_BACKGROUND_TILE_SIZE = 1200;
    const DEFAULT_THEME_TOKENS = {
      accent: '#e62600',
      accentHover: '#c61f00',
      accentContrast: '#f4f7f9',
      accentRing: 'rgba(72,156,212,0.16)',
      accentSoft: 'rgba(72,156,212,0.12)',
      accentSoftBorder: 'rgba(72,156,212,0.45)',
      border: 'rgba(72,156,212,0.18)',
      borderGlow: 'rgba(230,38,0,0.35)',
      scrollbarTrack: 'rgba(255,255,255,0.035)',
      scrollbarThumbStart: 'rgba(230,38,0,0.86)',
      scrollbarThumbEnd: 'rgba(154,18,0,0.82)',
      scrollbarThumbHoverStart: 'rgba(243,117,32,0.92)',
      scrollbarThumbHoverEnd: 'rgba(198,31,0,0.9)',
      scrollbarThumbBorder: 'rgba(31,35,39,0.94)',
      footerText: 'rgba(152,164,174,0.96)',
      footerTextHover: '#f4f7f9',
      footerTextShadow: '0 1px 12px rgba(0, 0, 0, 0.65)'
    };
    const RESULTS_MAP_THEME_STORAGE_KEY = 'walgreens_results_map_theme';
    const MAP_ADDRESS_STORAGE_KEY = 'walgreens_saved_map_address';
    const MAP_ADDRESS_REFERENCE_STORAGE_KEY = 'walgreens_saved_map_address_reference';
    const RESULTS_MAP_THEME_ICONS = {
      dark: '<svg class="results-map-theme-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 -960 960 960" aria-hidden="true"><path d="M480-120q-150 0-255-105T120-480q0-150 105-255t255-105q14 0 27.5 1t26.5 3q-41 29-65.5 75.5T444-660q0 90 63 153t153 63q55 0 101-24.5t75-65.5q2 13 3 26.5t1 27.5q0 150-105 255T480-120Zm0-80q88 0 158-48.5T740-375q-20 5-40 8t-40 3q-123 0-209.5-86.5T364-660q0-20 3-40t8-40q-78 32-126.5 102T200-480q0 116 82 198t198 82Zm-10-270Z"/></svg>',
      light: '<svg class="results-map-theme-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 -960 960 960" aria-hidden="true"><path d="M565-395q35-35 35-85t-35-85q-35-35-85-35t-85 35q-35 35-35 85t35 85q35 35 85 35t85-35Zm-226.5 56.5Q280-397 280-480t58.5-141.5Q397-680 480-680t141.5 58.5Q680-563 680-480t-58.5 141.5Q563-280 480-280t-141.5-58.5ZM200-440H40v-80h160v80Zm720 0H760v-80h160v80ZM440-760v-160h80v160h-80Zm0 720v-160h80v160h-80ZM256-650l-101-97 57-59 96 100-52 56Zm492 496-97-101 53-55 101 97-57 59Zm-98-550 97-101 59 57-100 96-56-52ZM154-212l101-97 55 53-97 101-59-57Zm326-268Z"/></svg>',
      streets: '<span class="results-map-theme-icon results-map-theme-icon-road" aria-hidden="true"></span>'
    };
    const RESULTS_MAP_THEMES = {
      dark: {
        label: 'Dark',
        background: '#1f2327',
        tileUrl: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
      },
      light: {
        label: 'Light',
        background: '#dfe6ec',
        tileUrl: 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png'
      },
      streets: {
        label: 'Streets',
        background: '#edf2f6',
        tileUrl: 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png'
      }
    };

    function createGrainedBackgroundDataUrl({
      patternWidth = 160,
      patternHeight = 160,
      grainOpacity = 0.08,
      grainDensity = 1,
      grainWidth = 1,
      grainHeight = 1
    } = {}) {
      try {
        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d');
        if (!context) return '';

        canvas.width = patternWidth;
        canvas.height = patternHeight;

        for (let x = 0; x < patternWidth; x += grainDensity) {
          for (let y = 0; y < patternHeight; y += grainDensity) {
            const rgb = Math.floor(Math.random() * 256);
            context.fillStyle = `rgba(${rgb}, ${rgb}, ${rgb}, ${grainOpacity})`;
            context.fillRect(x, y, grainWidth, grainHeight);
          }
        }

        return canvas.toDataURL('image/png');
      } catch (error) {
        return '';
      }
    }

    const defaultGrainImage = createGrainedBackgroundDataUrl();
    if (defaultGrainImage) {
      document.documentElement.style.setProperty('--default-grain-image', `url("${defaultGrainImage}")`);
    }

    const POKEMON_BACKGROUNDS = [
      { value: 'gyra', label: 'Gyarados', file: 'gyra.webp', size: '645px auto', opacity: '1', accent: '#1c98ce' },
      { value: 'ancient', label: 'Ancient', file: 'ancient.webp', size: '645px auto', opacity: '1', accent: '#d67d39' },
      { value: 'bee', label: 'Bee', file: 'bee.webp', size: '645px auto', opacity: '1', accent: '#d6bf2c' },
      { value: 'bulba', label: 'Bulbasaur', file: 'bulba.webp', size: '645px auto', opacity: '1', accent: '#66b88b' },
      { value: 'charmander', label: 'Charmander', file: 'charmander.webp', size: '645px auto', opacity: '1', accent: '#ec9633' },
      { value: 'eevee', label: 'Eevee', file: 'eevee.webp', size: '645px auto', opacity: '1', accent: '#cf9140' },
      { value: 'graveler', label: 'Graveler', file: 'graveler.webp', size: '645px auto', opacity: '1', accent: '#87939b' },
      { value: 'gx5YNZ3', label: 'GX Pattern', file: 'gx5YNZ3.webp', size: '645px auto', opacity: '1', accent: '#f0d64c' },
      { value: 'karp', label: 'Magikarp', file: 'karp.webp', size: '645px auto', opacity: '1', accent: '#ee6a3e' },
      { value: 'pattern1', label: 'Pattern 1', file: 'pattern1.webp', size: '645px auto', opacity: '1', accent: '#f0c74a' },
      { value: 'pika', label: 'Pikachu', file: 'pika.webp', size: '645px auto', opacity: '1', accent: '#f1dc43' },
      { value: 'poli', label: 'Poliwhirl', file: 'poli.webp', size: '645px auto', opacity: '1', accent: '#56b2f5' },
      { value: 'ponyta', label: 'Ponyta', file: 'ponyta.webp', size: '645px auto', opacity: '1', accent: '#ff9508' },
      { value: 'puff', label: 'Jigglypuff', file: 'puff.webp', size: '645px auto', opacity: '1', accent: '#ff93a8' },
      { value: 'rat', label: 'Raticate', file: 'rat.webp', size: '645px auto', opacity: '1', accent: '#c98b47' },
      { value: 'slowpoke', label: 'Slowpoke', file: 'slowpoke.webp', size: '645px auto', opacity: '1', accent: '#e59486' },
      { value: 'slowpoke2', label: 'Slowbro', file: 'slowbro.webp', size: '645px auto', opacity: '1', accent: '#c18457' },
      { value: 'squirtle', label: 'Squirtle', file: 'squirtle.webp', size: '645px auto', opacity: '1', accent: '#68b5d3' },
      { value: 'vulpix', label: 'Vulpix', file: 'vulpix.webp', size: '645px auto', opacity: '1', accent: '#d3b088' },
      { value: 'western', label: 'Western', file: 'western.webp', size: '645px auto', opacity: '1', accent: '#9a8266' }
    ];

    function getPokemonBackground(theme = DEFAULT_POKEMON_BACKGROUND_THEME) {
      return POKEMON_BACKGROUNDS.find(background => background.value === theme) || POKEMON_BACKGROUNDS[0];
    }

    function getPokemonBackgroundUrl(filename) {
      return `/${encodeURIComponent('bg')}/${encodeURIComponent(filename)}`;
    }

    function clampChannel(value) {
      return Math.max(0, Math.min(255, Math.round(value)));
    }

    function hexToRgb(hex) {
      const normalized = hex.replace('#', '');
      if (normalized.length !== 6) return null;
      return {
        r: parseInt(normalized.slice(0, 2), 16),
        g: parseInt(normalized.slice(2, 4), 16),
        b: parseInt(normalized.slice(4, 6), 16)
      };
    }

    function rgbToHex({ r, g, b }) {
      return `#${[r, g, b].map(channel => clampChannel(channel).toString(16).padStart(2, '0')).join('')}`;
    }

    function mixRgb(source, target, amount) {
      return {
        r: source.r + ((target.r - source.r) * amount),
        g: source.g + ((target.g - source.g) * amount),
        b: source.b + ((target.b - source.b) * amount)
      };
    }

    function rgbaString(rgb, alpha) {
      return `rgba(${clampChannel(rgb.r)},${clampChannel(rgb.g)},${clampChannel(rgb.b)},${alpha})`;
    }

    function getContrastColor(rgb) {
      const luminance = ((0.2126 * rgb.r) + (0.7152 * rgb.g) + (0.0722 * rgb.b)) / 255;
      return luminance > 0.66 ? '#1f252b' : '#f4f7f9';
    }

    function createBackgroundThemeTokens(accentHex) {
      const accentRgb = hexToRgb(accentHex);
      if (!accentRgb) return DEFAULT_THEME_TOKENS;

      const thumbStartRgb = mixRgb(accentRgb, { r: 255, g: 255, b: 255 }, 0.18);
      const thumbEndRgb = mixRgb(accentRgb, { r: 18, g: 22, b: 27 }, 0.22);
      const thumbHoverStartRgb = mixRgb(accentRgb, { r: 255, g: 255, b: 255 }, 0.3);
      const thumbHoverEndRgb = mixRgb(accentRgb, { r: 243, g: 117, b: 32 }, 0.2);
      const trackRgb = mixRgb(accentRgb, { r: 255, g: 255, b: 255 }, 0.08);
      const luminance = ((0.2126 * accentRgb.r) + (0.7152 * accentRgb.g) + (0.0722 * accentRgb.b)) / 255;
      const useDarkFooterText = luminance > 0.62;

      return {
        accent: accentHex,
        accentHover: rgbToHex(mixRgb(accentRgb, { r: 18, g: 22, b: 27 }, 0.18)),
        accentContrast: getContrastColor(accentRgb),
        accentRing: rgbaString(accentRgb, 0.2),
        accentSoft: rgbaString(accentRgb, 0.12),
        accentSoftBorder: rgbaString(accentRgb, 0.42),
        border: rgbaString(accentRgb, 0.2),
        borderGlow: rgbaString(accentRgb, 0.35),
        scrollbarTrack: rgbaString(trackRgb, 0.1),
        scrollbarThumbStart: rgbaString(thumbStartRgb, 0.88),
        scrollbarThumbEnd: rgbaString(thumbEndRgb, 0.78),
        scrollbarThumbHoverStart: rgbaString(thumbHoverStartRgb, 0.94),
        scrollbarThumbHoverEnd: rgbaString(thumbHoverEndRgb, 0.86),
        scrollbarThumbBorder: rgbaString(mixRgb(accentRgb, { r: 18, g: 22, b: 27 }, 0.82), 0.92),
        footerText: useDarkFooterText ? 'rgba(20,24,29,0.9)' : 'rgba(236,241,245,0.92)',
        footerTextHover: useDarkFooterText ? '#05080b' : '#ffffff',
        footerTextShadow: useDarkFooterText ? '0 1px 10px rgba(255, 255, 255, 0.28)' : '0 1px 12px rgba(0, 0, 0, 0.65)'
      };
    }

    function applyAccentTheme(tokens = DEFAULT_THEME_TOKENS) {
      const root = document.documentElement;
      root.style.setProperty('--accent', tokens.accent);
      root.style.setProperty('--accent-hover', tokens.accentHover);
      root.style.setProperty('--accent-contrast', tokens.accentContrast);
      root.style.setProperty('--accent-ring', tokens.accentRing);
      root.style.setProperty('--accent-soft', tokens.accentSoft);
      root.style.setProperty('--accent-soft-border', tokens.accentSoftBorder);
      root.style.setProperty('--border', tokens.border);
      root.style.setProperty('--border-glow', tokens.borderGlow);
      root.style.setProperty('--scrollbar-track', tokens.scrollbarTrack);
      root.style.setProperty('--scrollbar-thumb-start', tokens.scrollbarThumbStart);
      root.style.setProperty('--scrollbar-thumb-end', tokens.scrollbarThumbEnd);
      root.style.setProperty('--scrollbar-thumb-hover-start', tokens.scrollbarThumbHoverStart);
      root.style.setProperty('--scrollbar-thumb-hover-end', tokens.scrollbarThumbHoverEnd);
      root.style.setProperty('--scrollbar-thumb-border', tokens.scrollbarThumbBorder);
      root.style.setProperty('--footer-text', tokens.footerText);
      root.style.setProperty('--footer-text-hover', tokens.footerTextHover);
      root.style.setProperty('--footer-text-shadow', tokens.footerTextShadow);
    }

    function setPokemonBackgroundSelectState() {
      const enabledInput = document.getElementById('cfg-pokemon-bg-enabled');
      const themeInput = document.getElementById('cfg-pokemon-bg-theme');
      const sizeInput = document.getElementById('cfg-pokemon-bg-size');
      if (!enabledInput || !themeInput || !sizeInput) return;
      const disabled = !enabledInput.checked;
      themeInput.disabled = disabled;
      sizeInput.disabled = disabled;
    }

    function normalizePokemonBackgroundTileSize(tileSize = DEFAULT_POKEMON_BACKGROUND_TILE_SIZE) {
      const parsed = Number(tileSize);
      if (!Number.isFinite(parsed)) return DEFAULT_POKEMON_BACKGROUND_TILE_SIZE;
      return Math.max(MIN_POKEMON_BACKGROUND_TILE_SIZE, Math.min(MAX_POKEMON_BACKGROUND_TILE_SIZE, Math.round(parsed)));
    }

    function updatePokemonBackgroundSizeLabel(tileSize = DEFAULT_POKEMON_BACKGROUND_TILE_SIZE) {
      const sizeLabel = document.getElementById('cfg-pokemon-bg-size-value');
      if (sizeLabel) {
        sizeLabel.textContent = `${normalizePokemonBackgroundTileSize(tileSize)}px`;
      }
    }

    function applyPokemonBackgroundSettings({
      enabled = DEFAULT_POKEMON_BACKGROUND_ENABLED,
      theme = DEFAULT_POKEMON_BACKGROUND_THEME,
      tileSize = DEFAULT_POKEMON_BACKGROUND_TILE_SIZE,
      syncControls = true
    } = {}) {
      const background = getPokemonBackground(theme);
      const enabledInput = document.getElementById('cfg-pokemon-bg-enabled');
      const themeInput = document.getElementById('cfg-pokemon-bg-theme');
      const sizeInput = document.getElementById('cfg-pokemon-bg-size');
      const normalizedTileSize = normalizePokemonBackgroundTileSize(tileSize);

      if (syncControls) {
        if (enabledInput) enabledInput.checked = Boolean(enabled);
        if (themeInput) themeInput.value = background.value;
        if (sizeInput) sizeInput.value = String(normalizedTileSize);
        updatePokemonBackgroundSizeLabel(normalizedTileSize);
        setPokemonBackgroundSelectState();
      }

      document.body.classList.toggle('pokemon-bg-enabled', Boolean(enabled));
      document.body.style.setProperty('--pokemon-bg-image', `url("${getPokemonBackgroundUrl(background.file)}")`);
      document.body.style.setProperty('--pokemon-bg-size', `${normalizedTileSize}px auto`);
      document.body.style.setProperty('--pokemon-bg-opacity', background.opacity || '1');
      applyAccentTheme(enabled ? createBackgroundThemeTokens(background.accent) : DEFAULT_THEME_TOKENS);

      try {
        localStorage.setItem('local_theme_prefs', JSON.stringify({
          enabled: Boolean(enabled),
          theme: background.value,
          tileSize: normalizedTileSize
        }));
      } catch (e) { }
    }
    // Instant Theme Loader
    try {
      let themeToApply = null;
      try {
        const localPrefs = JSON.parse(localStorage.getItem('local_theme_prefs'));
        if (localPrefs && typeof localPrefs.enabled !== 'undefined') {
          themeToApply = localPrefs;
        }
      } catch (e) { }

      if (!themeToApply) {
        const statusData = JSON.parse(localStorage.getItem('cached_api_status'));
        if (statusData && statusData.status) {
          themeToApply = {
            enabled: statusData.status.pokemon_background_enabled ?? DEFAULT_POKEMON_BACKGROUND_ENABLED,
            theme: statusData.status.pokemon_background_theme || DEFAULT_POKEMON_BACKGROUND_THEME,
            tileSize: statusData.status.pokemon_background_tile_size ?? DEFAULT_POKEMON_BACKGROUND_TILE_SIZE
          };
        }
      }

      if (themeToApply) {
        // Wait for body to be available before setting classes/images if this runs purely in head
        const tryApply = () => {
          if (document.body) {
            applyPokemonBackgroundSettings({
              ...themeToApply,
              syncControls: false
            });
          } else {
            requestAnimationFrame(tryApply);
          }
        };
        tryApply();
      }
    } catch (e) { console.error('Failed to pre-load theme cache', e); }

  

    let currentTab = 'results';
    let isStatusRefreshInFlight = false;
    let lastResultsSignature = '';
    let currentResultsView = 'cards';
    let latestRenderedResults = null;
    let resultsMap = null;
    let resultsMapBaseLayer = null;
    let resultsMapMarkers = [];
    let resultsMapMarkersByKey = new Map();
    let userLocationMarker = null;
    let activeResultsMapKey = null;
    let currentResultsMapTheme = loadStoredResultsMapTheme();
    let manualLocationFallbackVisible = false;
    let mapDistanceReference = loadStoredMapAddressReference();
    let manualLocationQuery = loadStoredMapAddress();
    let manualLocationSuggestions = [];
    let manualLocationHighlightedIndex = -1;
    let manualLocationAutocompleteBusy = false;
    let manualLocationAutocompleteTimer = 0;
    let manualLocationAutocompleteController = null;
    let manualLocationAutocompleteRequestId = 0;
    let settingsMapAddressSuggestions = [];
    let settingsMapAddressHighlightedIndex = -1;
    let settingsMapAddressAutocompleteBusy = false;
    let settingsMapAddressAutocompleteTimer = 0;
    let settingsMapAddressStatus = {
      message: 'Type at least 3 characters for address suggestions.',
      tone: 'muted'
    };
    let manualLocationStatus = {
      message: 'Enter an address, city, or ZIP to center the map.',
      tone: 'muted'
    };
    let trackedProductsCache = [];
    let editingProductId = null;
    let currentUser = null;
    let authState = null;
    let statusRefreshTimer = null;
    let authStateRefreshTimer = null;
    let authSessionRefreshInFlight = false;
    let googleButtonRendered = false;
    let latestStatusSnapshot = null;
    let onboardingStepIndex = 0;
    let onboardingAutoPrompted = false;
    let onboardingDraft = null;
    let onboardingFeedback = { message: '', type: 'info' };
    let onboardingBusy = false;
    let deferredInstallPrompt = null;
    let installDisplayModeQueries = [];
    let shellScrollbarFrame = 0;
    let shellScrollbarDrag = null;
    let shellScrollbarMutationObserver = null;
    let shellScrollbarResizeObserver = null;
    let hasWindowLoaded = document.readyState === 'complete';
    let hasInitialSessionResolved = false;
    let hasInitialFontsResolved = !document.fonts || document.fonts.status === 'loaded';
    let googleButtonRevealTimer = 0;

    const runtimeConfig = window.WATCHER_RUNTIME_CONFIG || {};
    const API_BASE = normalizeApiBase(runtimeConfig.apiBaseUrl || '');

    function normalizeApiBase(value) {
      const normalized = String(value || '').trim();
      if (!normalized) return '';
      return normalized.replace(/\/+$/, '');
    }

    function setBootLoaderPreference(shouldUseLoader) {
      try {
        if (shouldUseLoader) {
          localStorage.setItem(AUTH_BOOT_LOADER_STORAGE_KEY, '1');
        } else {
          localStorage.removeItem(AUTH_BOOT_LOADER_STORAGE_KEY);
        }
      } catch (error) {
      }
    }

    function apiUrl(path) {
      const normalizedPath = path.startsWith('/') ? path : `/${path}`;
      return `${API_BASE}${normalizedPath}`;
    }

    function formatCompactNumber(value) {
      return new Intl.NumberFormat().format(Number(value) || 0);
    }

    function renderPublicStats(stats = null) {
      const checksNode = document.getElementById('auth-global-checks');
      const successesNode = document.getElementById('auth-global-successes');
      const uptimeNode = document.getElementById('auth-service-uptime');
      if (!checksNode || !successesNode || !uptimeNode) return;

      const totalChecks = Number(stats?.total_checks || 0);
      const successfulChecks = Number(stats?.successful_checks || 0);
      const uptimeLabel = String(stats?.service_uptime?.label || 'Loading uptime...');
      checksNode.textContent = formatCompactNumber(totalChecks);
      successesNode.textContent = `${formatCompactNumber(successfulChecks)} successful stock checks recorded`;
      uptimeNode.textContent = uptimeLabel;
    }

    async function loadPublicStats() {
      try {
        const response = await fetch(apiUrl('/api/public-stats'), { credentials: 'include' });
        if (!response.ok) return null;
        const stats = await response.json();
        renderPublicStats(stats);
        return stats;
      } catch (error) {
        return null;
      }
    }

    function refreshPublicStatsForLogin() {
      if (!currentUser) {
        loadPublicStats();
      }
    }

    function getShellScrollbarElements() {
      return {
        root: document.getElementById('shell-scrollbar'),
        track: document.querySelector('#shell-scrollbar .shell-scrollbar-track'),
        thumb: document.querySelector('#shell-scrollbar .shell-scrollbar-thumb')
      };
    }

    function getActiveShellScroller() {
      const appShell = document.getElementById('app-shell');
      if (appShell && !appShell.hidden) return appShell;

      const authGate = document.getElementById('auth-gate');
      if (authGate && !authGate.hidden) return authGate;

      return null;
    }

    function hideShellScrollbar() {
      const { root } = getShellScrollbarElements();
      if (!root) return;
      root.classList.add('is-hidden');
      root.classList.remove('is-visible', 'is-dragging');
    }

    function waitForInitialFonts(maxMs = 1200) {
      if (!document.fonts?.ready) {
        hasInitialFontsResolved = true;
        return Promise.resolve();
      }

      return Promise.race([
        document.fonts.ready.catch(() => undefined),
        new Promise(resolve => window.setTimeout(resolve, maxMs))
      ]).finally(() => {
        hasInitialFontsResolved = true;
        finalizeInitialReveal();
      });
    }

    function finalizeInitialReveal() {
      const loader = document.getElementById('page-loader');
      if (!hasWindowLoaded || !hasInitialSessionResolved || !hasInitialFontsResolved) return;

      if (!loader) {
        document.body.classList.remove('app-booting');
        return;
      }

      if (loader.dataset.dismissed === 'true') return;
      loader.dataset.dismissed = 'true';
      loader.remove();
      document.body.classList.remove('app-booting');
    }

    function queueShellScrollbarUpdate() {
      if (shellScrollbarFrame) return;
      shellScrollbarFrame = window.requestAnimationFrame(() => {
        shellScrollbarFrame = 0;
        updateShellScrollbar();
      });
    }

    function updateShellScrollbar() {
      const { root, thumb } = getShellScrollbarElements();
      const scroller = getActiveShellScroller();
      if (!root || !thumb || !scroller) {
        hideShellScrollbar();
        return;
      }

      const rect = scroller.getBoundingClientRect();
      const maxScroll = scroller.scrollHeight - scroller.clientHeight;
      if (rect.height <= 0 || rect.width <= 0 || maxScroll <= 1) {
        hideShellScrollbar();
        return;
      }

      const computedStyle = window.getComputedStyle(scroller);
      const topRightRadius = parseFloat(computedStyle.borderTopRightRadius) || 18;
      const bottomRightRadius = parseFloat(computedStyle.borderBottomRightRadius) || topRightRadius;
      const verticalInsetTop = Math.max(12, topRightRadius - 2);
      const verticalInsetBottom = Math.max(12, bottomRightRadius - 2);
      const horizontalInset = Math.max(8, Math.round(topRightRadius * 0.45));
      const barHeight = Math.max(48, rect.height - verticalInsetTop - verticalInsetBottom);
      const barTop = rect.top + verticalInsetTop;
      const barLeft = rect.right - horizontalInset - 8;
      const thumbHeight = Math.max(36, Math.min(barHeight, (scroller.clientHeight / scroller.scrollHeight) * barHeight));
      const thumbTravel = Math.max(0, barHeight - thumbHeight);
      const scrollRatio = maxScroll > 0 ? (scroller.scrollTop / maxScroll) : 0;
      const thumbTop = thumbTravel * scrollRatio;

      root.style.top = `${Math.round(barTop)}px`;
      root.style.left = `${Math.round(barLeft)}px`;
      root.style.height = `${Math.round(barHeight)}px`;

      thumb.style.height = `${Math.round(thumbHeight)}px`;
      thumb.style.transform = `translateY(${Math.round(thumbTop)}px)`;

      root.classList.remove('is-hidden');
      root.classList.add('is-visible');
    }

    function endShellScrollbarDrag() {
      if (!shellScrollbarDrag) return;
      const { root } = getShellScrollbarElements();
      if (root) root.classList.remove('is-dragging');
      shellScrollbarDrag = null;
      window.removeEventListener('pointermove', handleShellScrollbarDrag);
      window.removeEventListener('pointerup', endShellScrollbarDrag);
      window.removeEventListener('pointercancel', endShellScrollbarDrag);
    }

    function handleShellScrollbarDrag(event) {
      if (!shellScrollbarDrag) return;
      const { scroller, startY, startScrollTop, maxScroll, thumbTravel } = shellScrollbarDrag;
      if (!scroller || thumbTravel <= 0 || maxScroll <= 0) return;

      const deltaY = event.clientY - startY;
      const scrollDelta = (deltaY / thumbTravel) * maxScroll;
      scroller.scrollTop = startScrollTop + scrollDelta;
    }

    function beginShellScrollbarDrag(event) {
      const { root, thumb } = getShellScrollbarElements();
      const scroller = getActiveShellScroller();
      if (!root || !thumb || !scroller) return;

      const rootRect = root.getBoundingClientRect();
      const thumbRect = thumb.getBoundingClientRect();
      const maxScroll = scroller.scrollHeight - scroller.clientHeight;
      const thumbTravel = Math.max(0, rootRect.height - thumbRect.height);
      if (maxScroll <= 0 || thumbTravel <= 0) return;

      event.preventDefault();
      shellScrollbarDrag = {
        scroller,
        startY: event.clientY,
        startScrollTop: scroller.scrollTop,
        maxScroll,
        thumbTravel
      };

      root.classList.add('is-dragging');
      window.addEventListener('pointermove', handleShellScrollbarDrag);
      window.addEventListener('pointerup', endShellScrollbarDrag);
      window.addEventListener('pointercancel', endShellScrollbarDrag);
    }

    function handleShellScrollbarTrackPointerDown(event) {
      const { root, thumb } = getShellScrollbarElements();
      const scroller = getActiveShellScroller();
      if (!root || !thumb || !scroller) return;
      if (event.target === thumb) return;

      const rootRect = root.getBoundingClientRect();
      const thumbRect = thumb.getBoundingClientRect();
      const maxScroll = scroller.scrollHeight - scroller.clientHeight;
      const thumbTravel = Math.max(0, rootRect.height - thumbRect.height);
      if (maxScroll <= 0 || thumbTravel <= 0) return;

      event.preventDefault();
      const nextThumbTop = Math.max(0, Math.min(thumbTravel, event.clientY - rootRect.top - (thumbRect.height / 2)));
      scroller.scrollTop = (nextThumbTop / thumbTravel) * maxScroll;
      queueShellScrollbarUpdate();
    }

    function initializeShellScrollbar() {
      const { root, track, thumb } = getShellScrollbarElements();
      const authGate = document.getElementById('auth-gate');
      const appShell = document.getElementById('app-shell');
      if (!root || !track || !thumb || !authGate || !appShell) return;

      thumb.addEventListener('pointerdown', beginShellScrollbarDrag);
      track.addEventListener('pointerdown', handleShellScrollbarTrackPointerDown);
      authGate.addEventListener('scroll', queueShellScrollbarUpdate, { passive: true });
      appShell.addEventListener('scroll', queueShellScrollbarUpdate, { passive: true });
      window.addEventListener('resize', queueShellScrollbarUpdate, { passive: true });

      if (typeof MutationObserver === 'function') {
        shellScrollbarMutationObserver = new MutationObserver(() => queueShellScrollbarUpdate());
        shellScrollbarMutationObserver.observe(authGate, { attributes: true, childList: true, subtree: true, characterData: true });
        shellScrollbarMutationObserver.observe(appShell, { attributes: true, childList: true, subtree: true, characterData: true });
      }

      if (typeof ResizeObserver === 'function') {
        shellScrollbarResizeObserver = new ResizeObserver(() => queueShellScrollbarUpdate());
        shellScrollbarResizeObserver.observe(authGate);
        shellScrollbarResizeObserver.observe(appShell);
      }

      queueShellScrollbarUpdate();
    }

    function normalizeResultsMapTheme(theme) {
      return ['dark', 'light', 'streets'].includes(theme) ? theme : 'dark';
    }

    function loadStoredResultsMapTheme() {
      try {
        return normalizeResultsMapTheme(localStorage.getItem(RESULTS_MAP_THEME_STORAGE_KEY) || 'dark');
      } catch (error) {
        return 'dark';
      }
    }

    function loadStoredMapAddress() {
      try {
        return String(localStorage.getItem(MAP_ADDRESS_STORAGE_KEY) || '').trim();
      } catch (error) {
        return '';
      }
    }

    function normalizeMapAddressReference(reference) {
      if (!reference || typeof reference !== 'object') return null;
      const latitude = normalizeCoordinate(reference.latitude);
      const longitude = normalizeCoordinate(reference.longitude);
      if (latitude === null || longitude === null) return null;
      return {
        label: String(reference.label || '').trim(),
        latitude,
        longitude
      };
    }

    function loadStoredMapAddressReference() {
      try {
        const rawValue = localStorage.getItem(MAP_ADDRESS_REFERENCE_STORAGE_KEY);
        if (!rawValue) return null;
        return normalizeMapAddressReference(JSON.parse(rawValue));
      } catch (error) {
        return null;
      }
    }

    function storeMapAddress(address) {
      const normalizedAddress = String(address || '').trim();
      try {
        if (normalizedAddress) {
          localStorage.setItem(MAP_ADDRESS_STORAGE_KEY, normalizedAddress);
        } else {
          localStorage.removeItem(MAP_ADDRESS_STORAGE_KEY);
        }
      } catch (error) {
      }
      return normalizedAddress;
    }

    function setMapDistanceReference(reference, { persist = false } = {}) {
      const normalizedReference = normalizeMapAddressReference(reference);
      mapDistanceReference = normalizedReference;
      if (persist) {
        try {
          if (normalizedReference) {
            localStorage.setItem(MAP_ADDRESS_REFERENCE_STORAGE_KEY, JSON.stringify(normalizedReference));
          } else {
            localStorage.removeItem(MAP_ADDRESS_REFERENCE_STORAGE_KEY);
          }
        } catch (error) {
        }
      }
      return normalizedReference;
    }

    function persistResultsMapTheme(theme) {
      try {
        localStorage.setItem(RESULTS_MAP_THEME_STORAGE_KEY, normalizeResultsMapTheme(theme));
      } catch (error) {
      }
    }

    function getResultsMapTileLayer(theme = currentResultsMapTheme) {
      const themeConfig = RESULTS_MAP_THEMES[normalizeResultsMapTheme(theme)] || RESULTS_MAP_THEMES.dark;
      return L.tileLayer(themeConfig.tileUrl, {
        subdomains: 'abcd',
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/">CARTO</a>',
        maxZoom: 19
      });
    }

    function updateResultsMapThemeControlState(theme = currentResultsMapTheme) {
      const normalizedTheme = normalizeResultsMapTheme(theme);
      document.querySelectorAll('.results-map-theme-btn').forEach(button => {
        const isActive = button.dataset.mapTheme === normalizedTheme;
        button.classList.toggle('is-active', isActive);
        button.setAttribute('aria-pressed', isActive ? 'true' : 'false');
      });
    }

    function applyResultsMapTheme(theme, { persist = true } = {}) {
      const normalizedTheme = normalizeResultsMapTheme(theme);
      const mapCard = document.querySelector('.results-map-card');
      const mapElement = document.getElementById('results-map');
      const themeConfig = RESULTS_MAP_THEMES[normalizedTheme] || RESULTS_MAP_THEMES.dark;

      currentResultsMapTheme = normalizedTheme;

      if (mapCard) {
        mapCard.dataset.mapTheme = normalizedTheme;
      }

      if (mapElement) {
        mapElement.dataset.mapTheme = normalizedTheme;
        mapElement.style.background = themeConfig.background;
      }

      if (resultsMap) {
        if (resultsMapBaseLayer) {
          resultsMap.removeLayer(resultsMapBaseLayer);
        }
        resultsMapBaseLayer = getResultsMapTileLayer(normalizedTheme).addTo(resultsMap);
      }

      updateResultsMapThemeControlState(normalizedTheme);

      if (persist) {
        persistResultsMapTheme(normalizedTheme);
      }
    }

    function createResultsMapThemeControl() {
      const control = L.control({ position: 'topright' });
      control.onAdd = function onAdd() {
        const container = L.DomUtil.create('div', 'results-map-theme-control leaflet-bar');
        container.setAttribute('role', 'group');
        container.setAttribute('aria-label', 'Map theme');
        container.innerHTML = Object.entries(RESULTS_MAP_THEMES).map(([theme, config]) => `
            <button class="results-map-theme-btn ${currentResultsMapTheme === theme ? 'is-active' : ''}" type="button" data-map-theme="${theme}" aria-label="${config.label} map theme" title="${config.label}" aria-pressed="${currentResultsMapTheme === theme ? 'true' : 'false'}">${RESULTS_MAP_THEME_ICONS[theme] || config.label}</button>
          `).join('');

        L.DomEvent.disableClickPropagation(container);
        L.DomEvent.disableScrollPropagation(container);

        container.querySelectorAll('.results-map-theme-btn').forEach(button => {
          L.DomEvent.on(button, 'click', event => {
            L.DomEvent.stop(event);
            applyResultsMapTheme(button.dataset.mapTheme || 'dark');
          });
        });

        return container;
      };

      return control;
    }

    function stopStatusPolling() {
      if (statusRefreshTimer) {
        clearInterval(statusRefreshTimer);
        statusRefreshTimer = null;
      }
    }

    function stopAuthStatePolling() {
      if (authStateRefreshTimer) {
        clearInterval(authStateRefreshTimer);
        authStateRefreshTimer = null;
      }
    }

    async function refreshWaitlistAuthState() {
      if (authSessionRefreshInFlight) return;
      if (currentUser || authState?.access_state !== 'waitlisted') {
        stopAuthStatePolling();
        return;
      }

      authSessionRefreshInFlight = true;
      try {
        const sessionPayload = await apiCall('/api/auth/session', 'GET', null, {
          allowUnauthorized: true,
          suppressAuthRedirect: true,
          silent: true
        });
        if (!sessionPayload) return;

        const wasWaitlisted = authState?.access_state === 'waitlisted';
        const isApprovedNow = Boolean(sessionPayload?.authenticated && sessionPayload?.user);
        await hydrateAuthenticatedSession(sessionPayload);
        if (wasWaitlisted && isApprovedNow) {
          showToast('Approval received. Welcome in.', 'success');
        }
      } finally {
        authSessionRefreshInFlight = false;
      }
    }

    function startAuthStatePolling() {
      stopAuthStatePolling();
      authStateRefreshTimer = setInterval(() => {
        refreshWaitlistAuthState();
      }, 5000);
    }

    function startStatusPolling() {
      stopStatusPolling();
      statusRefreshTimer = setInterval(() => {
        if (currentUser) {
          loadStatus();
        }
      }, 5000);
    }

    function updateAuthStatus(message = '') {
      const statusNode = document.getElementById('auth-status-text');
      if (statusNode) {
        statusNode.textContent = message;
      }
    }

    function setAuthGateMode(mode = 'signin', payload = null) {
      const signInContent = document.getElementById('auth-signin-content');
      const waitlistCard = document.getElementById('auth-waitlist-card');
      const waitlistCopy = document.getElementById('auth-waitlist-copy');
      const waitlistedUser = payload?.waitlisted_user || null;
      const waitlistMessage = payload?.access_denied_reason
        || (waitlistedUser?.email
          ? `${waitlistedUser.email} is waiting for admin approval.`
          : 'Your sign-in has been received and is waiting on admin review.');

      if (signInContent) {
        signInContent.hidden = mode === 'waitlist';
      }
      if (waitlistCard) {
        waitlistCard.hidden = mode !== 'waitlist';
      }
      if (waitlistCopy) {
        waitlistCopy.textContent = waitlistMessage;
      }
    }

    function setInstallButtonsVisible(visible) {
      document.querySelectorAll('.install-app-btn').forEach(button => {
        button.hidden = !visible;
        button.classList.remove('is-visible');
        if (visible) {
          window.requestAnimationFrame(() => {
            window.requestAnimationFrame(() => {
              if (!button.hidden) {
                button.classList.add('is-visible');
              }
            });
          });
        }
      });
      document.querySelectorAll('.install-app-copy').forEach(copy => {
        copy.hidden = !visible;
        copy.classList.remove('is-visible');
        if (visible) {
          window.requestAnimationFrame(() => {
            window.requestAnimationFrame(() => {
              if (!copy.hidden) {
                copy.classList.add('is-visible');
              }
            });
          });
        }
      });
    }

    function matchesDisplayMode(mode) {
      return Boolean(window.matchMedia && window.matchMedia(`(display-mode: ${mode})`).matches);
    }

    function isWindowsPlatform() {
      const platformValue =
        (navigator.userAgentData && navigator.userAgentData.platform)
        || navigator.platform
        || navigator.userAgent
        || '';
      return /windows/i.test(String(platformValue));
    }

    function isAppInstalled() {
      const isInstalledDisplayMode =
        matchesDisplayMode('standalone')
        || matchesDisplayMode('window-controls-overlay')
        || matchesDisplayMode('minimal-ui')
        || matchesDisplayMode('fullscreen');
      const isIosStandalone = window.navigator && window.navigator.standalone === true;
      const isAndroidAppReferrer = document.referrer && document.referrer.startsWith('android-app://');
      return isInstalledDisplayMode || isIosStandalone || isAndroidAppReferrer;
    }

    function refreshInstallButtons() {
      const installed = isAppInstalled();
      const shouldReserveInstallSlot = !installed && isWindowsPlatform();
      document.body.classList.toggle('app-installed', installed);
      document.body.classList.toggle('install-slot-active', shouldReserveInstallSlot);
      setInstallButtonsVisible(Boolean(deferredInstallPrompt) && !installed && isWindowsPlatform());
    }

    async function promptInstallApp() {
      if (!deferredInstallPrompt || !isWindowsPlatform()) return;
      deferredInstallPrompt.prompt();
      try {
        await deferredInstallPrompt.userChoice;
      } catch (e) {
      }
      deferredInstallPrompt = null;
      refreshInstallButtons();
    }

    async function registerServiceWorker() {
      if (!('serviceWorker' in navigator)) return;
      try {
        await navigator.serviceWorker.register('/sw.js', { scope: '/' });
      } catch (error) {
        console.error('Service worker registration failed:', error);
      }
    }

    function getOnboardingStorageKey() {
      if (!currentUser?.id) return '';
      return `walgreens_onboarding_completed_${currentUser.id}`;
    }

    function hasCompletedOnboarding() {
      const key = getOnboardingStorageKey();
      return key ? localStorage.getItem(key) === 'done' : false;
    }

    function markOnboardingCompleted() {
      const key = getOnboardingStorageKey();
      if (key) {
        localStorage.setItem(key, 'done');
      }
    }

    function resetOnboardingSessionState() {
      onboardingStepIndex = 0;
      onboardingAutoPrompted = false;
      onboardingDraft = null;
      onboardingFeedback = { message: '', type: 'info' };
      onboardingBusy = false;
    }

    function buildOnboardingDraft() {
      const status = latestStatusSnapshot?.status || {};
      const stats = latestStatusSnapshot?.statistics || {};
      const primaryDestination = (status.discord_destinations || [])[0] || {};

      return {
        zipcode: document.getElementById('cfg-zip')?.value.trim() || status.current_zipcode || '',
        interval: String(document.getElementById('cfg-interval')?.value || status.check_interval_minutes || 60),
        mapAddress: document.getElementById('cfg-map-address')?.value.trim() || loadStoredMapAddress(),
        webhookUrl: primaryDestination.url || '',
        webhookRoleId: primaryDestination.role_id || '',
        productUrl: '',
        productName: '',
        runManualCheck: (stats.total_checks || 0) === 0,
        startScheduler: Boolean(status.is_running)
      };
    }

    function ensureOnboardingDraft() {
      if (!onboardingDraft) {
        onboardingDraft = buildOnboardingDraft();
      }
      return onboardingDraft;
    }

    function setOnboardingFeedback(message = '', type = 'info') {
      onboardingFeedback = { message, type };
      const node = document.getElementById('onboarding-inline-status');
      if (!node) return;
      node.textContent = message;
      node.className = `onboarding-inline-status${message ? ` is-${type}` : ''}`;
    }

    function syncOnboardingDraftFromCurrentStep() {
      const draft = ensureOnboardingDraft();
      const zipInput = document.getElementById('onboarding-zip');
      const intervalInput = document.getElementById('onboarding-interval');
      const mapAddressInput = document.getElementById('onboarding-map-address');
      const webhookInput = document.getElementById('onboarding-webhook-url');
      const webhookRoleInput = document.getElementById('onboarding-webhook-role');
      const productUrlInput = document.getElementById('onboarding-product-url');
      const productNameInput = document.getElementById('onboarding-product-name');
      const runCheckInput = document.getElementById('onboarding-run-check');
      const startSchedulerInput = document.getElementById('onboarding-start-scheduler');

      if (zipInput) draft.zipcode = zipInput.value.trim();
      if (intervalInput) draft.interval = intervalInput.value.trim();
      if (mapAddressInput) draft.mapAddress = mapAddressInput.value.trim();
      if (webhookInput) draft.webhookUrl = webhookInput.value.trim();
      if (webhookRoleInput) draft.webhookRoleId = webhookRoleInput.value.replace(/[^\d]/g, '');
      if (productUrlInput) draft.productUrl = productUrlInput.value.trim();
      if (productNameInput) draft.productName = productNameInput.value.trim();
      if (runCheckInput) draft.runManualCheck = runCheckInput.checked;
      if (startSchedulerInput) draft.startScheduler = startSchedulerInput.checked;
    }

    function getOnboardingSteps() {
      const draft = ensureOnboardingDraft();
      const configuredZip = draft.zipcode || latestStatusSnapshot?.status?.current_zipcode || 'your ZIP code';
      const trackedCount = trackedProductsCache.length || (latestStatusSnapshot?.status?.tracked_products || []).length || 0;

      return [
        {
          tag: 'Welcome',
          title: 'Here is the fast path.',
          copy: 'This hosted version keeps your watcher state tied to your Google account, so you only need to set things up once before the server can keep scanning for you.',
          calloutTitle: 'You are about to configure',
          bullets: [
            'Your search area and scan cadence.',
            'A Discord destination for alerts, if you want push notifications.',
            'The Walgreens product links you actually want monitored.'
          ],
          actionLabel: 'Open dashboard',
          action: () => openTab('results'),
          nextLabel: 'Start setup',
          renderForm: () => `
              <div class="onboarding-form-card">
                <div class="onboarding-form-card-title">What happens next</div>
                <div class="onboarding-form-card-copy">The next two steps save your live settings directly from this modal, so you do not have to bounce between tabs before your first scan.</div>
              </div>
            `
        },
        {
          tag: 'Settings',
          title: 'Enter the settings you want the server to use.',
          copy: 'Save your ZIP code, scan interval, and first Discord destination right here. You can add more webhook destinations later from the full Settings page.',
          calloutTitle: 'Save these live values',
          bullets: [
            'Use a real ZIP code instead of the placeholder.',
            'Pick an interval that matches how often you want checks to run.',
            'The saved map address is only used by map view for distance and recentering.',
            'That map address stays in this browser only and is not stored on our servers.',
            'Discord is optional, but helpful if you want alerts outside the browser.'
          ],
          actionLabel: 'Use full settings page',
          action: () => {
            openTab('settings');
            document.getElementById('cfg-zip')?.focus();
            document.getElementById('cfg-zip')?.select?.();
          },
          nextLabel: 'Save & continue',
          renderForm: () => `
              <div class="onboarding-form-grid">
                <div class="form-group">
                  <label class="form-label" for="onboarding-zip">ZIP Code</label>
                  <input type="text" class="form-input" id="onboarding-zip" maxlength="10" placeholder="Enter ZIP code" value="${escapeHtml(draft.zipcode)}">
                  <span class="form-hint">Walgreens store search area.</span>
                </div>
                <div class="form-group">
                  <label class="form-label" for="onboarding-interval">Check Interval</label>
                  <input type="number" class="form-input" id="onboarding-interval" min="1" max="1440" step="1" placeholder="60" value="${escapeHtml(draft.interval)}">
                  <span class="form-hint">Minutes between automatic checks.</span>
                </div>
              </div>
              <div class="onboarding-form-card">
                <div class="onboarding-form-card-title">Saved map address</div>
                <div class="onboarding-form-card-copy">Optional. This is only used in map view to recenter the map and calculate distances from a saved place when you want something other than live browser location.</div>
                <div class="form-group">
                  <label class="form-label" for="onboarding-map-address">Map View Address</label>
                  <input type="text" class="form-input" id="onboarding-map-address" maxlength="180" placeholder="Enter address, city, or ZIP" autocomplete="street-address" spellcheck="false" value="${escapeHtml(draft.mapAddress || '')}">
                  <span class="form-hint">Stored locally in this browser only. It is not saved to our servers.</span>
                </div>
              </div>
              <div class="onboarding-form-card">
                <div class="onboarding-form-card-title">Primary Discord destination</div>
                <div class="onboarding-form-card-copy">Optional for onboarding. If you leave this blank, alerts will stay in the dashboard until you add a webhook later.</div>
                <div class="onboarding-inline-help">
                  <span class="tooltip-container">
                    <span>Need help?</span>
                    <div class="tooltip-content">
                      <div style="margin-bottom:8px;"><strong>Webhook URL:</strong> In your Discord server, go to <em>Server
                          Settings > Integrations > Webhooks</em> to create a new webhook and copy its URL.</div>
                      <div><strong>Role ID:</strong> Type <code>\@RoleName</code> in any Discord channel and press enter. It
                        will output a numeric ID like <code>&lt;@&amp;123456789&gt;</code>. Paste the numbers here.</div>
                    </div>
                  </span>
                </div>
                <div class="form-group">
                  <label class="form-label" for="onboarding-webhook-url">Webhook URL</label>
                  <input type="url" class="form-input" id="onboarding-webhook-url" placeholder="https://discord.com/api/webhooks/..." value="${escapeHtml(draft.webhookUrl)}">
                </div>
                <div class="form-group">
                  <label class="form-label" for="onboarding-webhook-role">Role ID</label>
                  <input type="text" class="form-input" id="onboarding-webhook-role" inputmode="numeric" placeholder="Optional role id" value="${escapeHtml(draft.webhookRoleId)}">
                  <span class="form-hint">Optional. Mention a role only for this webhook.</span>
                </div>
              </div>
            `,
          onNext: saveOnboardingSettings
        },
        {
          tag: 'Products',
          title: 'Add the first product you want watched.',
          copy: 'Paste one Walgreens product link and the app will resolve the product metadata for you. This gets your account out of demo mode immediately.',
          calloutTitle: 'Add at least one tracked item',
          bullets: [
            'Paste a Walgreens product page URL.',
            'Use a custom name if you want cleaner Discord labels.',
            `You currently have ${trackedCount} tracked product${trackedCount === 1 ? '' : 's'} saved.`
          ],
          actionLabel: 'Use full settings page',
          action: () => {
            openTab('settings');
            document.getElementById('new-product-url')?.focus();
          },
          nextLabel: 'Add product',
          renderForm: () => `
              <div class="onboarding-form-card">
                <div class="onboarding-form-card-title">First tracked product</div>
                <div class="form-group">
                  <label class="form-label" for="onboarding-product-url">Product URL</label>
                  <input type="url" class="form-input" id="onboarding-product-url" placeholder="Paste a Walgreens product link" value="${escapeHtml(draft.productUrl)}">
                </div>
                <div class="form-group">
                  <label class="form-label" for="onboarding-product-name">Custom Name</label>
                  <input type="text" class="form-input" id="onboarding-product-name" maxlength="160" placeholder="Optional custom name" value="${escapeHtml(draft.productName)}">
                  <span class="form-hint">Optional, but useful if the Walgreens title is messy.</span>
                </div>
              </div>
            `,
          onNext: saveOnboardingProduct
        },
        {
          tag: 'Launch',
          title: 'Choose how you want to finish setup.',
          copy: `You are ready to monitor ${configuredZip}. You can kick off a manual check now, start the background scheduler, or do both from this last step.`,
          calloutTitle: 'Finish strong',
          bullets: [
            'A manual check confirms your ZIP code and product link behave the way you expect.',
            'Starting the scheduler keeps the server scanning even when your browser is closed.',
            'You can always reopen this guide later from the header.'
          ],
          actionLabel: 'Open results',
          action: () => {
            openTab('results');
            document.getElementById('check-btn')?.focus();
          },
          nextLabel: 'Finish setup',
          renderForm: () => `
              <div class="onboarding-summary">
                <div class="onboarding-summary-row"><span>ZIP code</span><strong>${escapeHtml(configuredZip)}</strong></div>
                <div class="onboarding-summary-row"><span>Check interval</span><strong>${escapeHtml(draft.interval || '60')} min</strong></div>
                <div class="onboarding-summary-row"><span>Map view address</span><strong>${draft.mapAddress ? 'Saved locally' : 'Add later'}</strong></div>
                <div class="onboarding-summary-row"><span>Discord</span><strong>${draft.webhookUrl ? 'Configured' : 'Add later'}</strong></div>
                <div class="onboarding-summary-row"><span>Tracked products</span><strong>${trackedCount}</strong></div>
              </div>
              <div class="onboarding-choice-list">
                <label class="onboarding-choice">
                  <input type="checkbox" id="onboarding-run-check" ${draft.runManualCheck ? 'checked' : ''}>
                  <span>
                    <strong>Run a manual check now</strong>
                    Use the live hosted workflow immediately and land on the Results tab when setup finishes.
                  </span>
                </label>
                <label class="onboarding-choice">
                  <input type="checkbox" id="onboarding-start-scheduler" ${draft.startScheduler ? 'checked' : ''}>
                  <span>
                    <strong>Start the background scheduler</strong>
                    Let the server continue checking in the background after onboarding closes.
                  </span>
                </label>
              </div>
            `,
          onNext: finishOnboardingLaunch
        }
      ];
    }

    function renderOnboarding() {
      const steps = getOnboardingSteps();
      const step = steps[onboardingStepIndex] || steps[0];
      const progressNode = document.getElementById('onboarding-progress');
      const counterNode = document.getElementById('onboarding-step-counter');
      const tagNode = document.getElementById('onboarding-step-tag');
      const titleNode = document.getElementById('onboarding-step-title');
      const copyNode = document.getElementById('onboarding-step-copy');
      const calloutTitleNode = document.getElementById('onboarding-callout-title');
      const listNode = document.getElementById('onboarding-step-list');
      const formNode = document.getElementById('onboarding-step-form');
      const backButton = document.getElementById('onboarding-back-btn');
      const nextButton = document.getElementById('onboarding-next-btn');
      const jumpButton = document.getElementById('onboarding-jump-btn');
      const skipButton = document.getElementById('onboarding-skip-btn');

      if (progressNode) {
        progressNode.innerHTML = steps.map((item, index) => `
            <div class="onboarding-progress-item ${index === onboardingStepIndex ? 'is-active' : ''}">
              <span class="onboarding-progress-index">${index + 1}</span>
              <span class="onboarding-progress-text">${item.tag}</span>
            </div>
          `).join('');
      }

      if (counterNode) counterNode.textContent = `Step ${onboardingStepIndex + 1} of ${steps.length}`;
      if (tagNode) tagNode.textContent = step.tag;
      if (titleNode) titleNode.textContent = step.title;
      if (copyNode) copyNode.textContent = step.copy;
      if (calloutTitleNode) calloutTitleNode.textContent = step.calloutTitle;
      if (listNode) {
        listNode.innerHTML = step.bullets.map(item => `<li>${escapeHtml(item)}</li>`).join('');
      }
      if (formNode) {
        const formHtml = step.renderForm ? step.renderForm(ensureOnboardingDraft()) : '';
        formNode.innerHTML = formHtml;
        formNode.hidden = !formHtml;
      }
      if (backButton) backButton.disabled = onboardingBusy || onboardingStepIndex === 0;
      if (nextButton) {
        nextButton.disabled = onboardingBusy;
        nextButton.textContent = onboardingBusy ? 'Saving...' : (step.nextLabel || (onboardingStepIndex === steps.length - 1 ? 'Finish' : 'Next step'));
      }
      if (jumpButton) {
        jumpButton.hidden = !step.action;
        jumpButton.disabled = onboardingBusy;
        jumpButton.textContent = step.actionLabel || 'Show me';
      }
      if (skipButton) {
        skipButton.disabled = onboardingBusy;
        skipButton.textContent = onboardingStepIndex === steps.length - 1 ? 'Close' : 'Skip guide';
      }
      setOnboardingFeedback(onboardingFeedback.message, onboardingFeedback.type);
    }

    function syncModalBodyState() {
      const onboardingModal = document.getElementById('onboarding-modal');
      const techStackModal = document.getElementById('tech-stack-modal');
      const hasOpenModal = Boolean(
        (onboardingModal && !onboardingModal.hidden) ||
        (techStackModal && !techStackModal.hidden)
      );
      document.body.classList.toggle('onboarding-open', hasOpenModal);
    }

    function openOnboarding(reset = false) {
      if (!currentUser) return;
      if (reset) {
        onboardingStepIndex = 0;
        onboardingDraft = buildOnboardingDraft();
        onboardingFeedback = { message: '', type: 'info' };
      } else {
        ensureOnboardingDraft();
      }
      onboardingAutoPrompted = true;
      renderOnboarding();
      const modal = ensureOnboardingModalRoot();
      if (modal) {
        modal.hidden = false;
        syncModalBodyState();
      }
    }

    function closeOnboarding(markComplete = false) {
      const modal = document.getElementById('onboarding-modal');
      if (modal) {
        modal.hidden = true;
      }
      syncModalBodyState();
      onboardingBusy = false;
      onboardingFeedback = { message: '', type: 'info' };
      if (markComplete) {
        markOnboardingCompleted();
      }
    }

    function ensureOnboardingModalRoot() {
      const modal = document.getElementById('onboarding-modal');
      if (modal && modal.parentElement !== document.body) {
        document.body.appendChild(modal);
      }
      return modal;
    }

    function openTechStackModal() {
      const modal = ensureTechStackModalRoot();
      if (modal) {
        modal.hidden = false;
        syncModalBodyState();
      }
    }

    function closeTechStackModal() {
      const modal = document.getElementById('tech-stack-modal');
      if (modal) {
        modal.hidden = true;
      }
      syncModalBodyState();
    }

    function ensureTechStackModalRoot() {
      const modal = document.getElementById('tech-stack-modal');
      if (modal && modal.parentElement !== document.body) {
        document.body.appendChild(modal);
      }
      return modal;
    }

    function goToOnboardingStep(direction) {
      syncOnboardingDraftFromCurrentStep();
      onboardingFeedback = { message: '', type: 'info' };
      const steps = getOnboardingSteps();
      const nextIndex = onboardingStepIndex + direction;
      if (nextIndex < 0) {
        onboardingStepIndex = 0;
        renderOnboarding();
        return;
      }
      if (nextIndex >= steps.length) {
        closeOnboarding(true);
        showToast('You can reopen the guide anytime from the header.', 'success');
        return;
      }
      onboardingStepIndex = nextIndex;
      renderOnboarding();
    }

    async function handleOnboardingNext() {
      syncOnboardingDraftFromCurrentStep();
      onboardingFeedback = { message: '', type: 'info' };
      const steps = getOnboardingSteps();
      const step = steps[onboardingStepIndex];
      if (!step) return;

      if (!step.onNext) {
        goToOnboardingStep(1);
        return;
      }

      onboardingBusy = true;
      renderOnboarding();
      let result = false;
      try {
        result = await step.onNext();
      } finally {
        onboardingBusy = false;
      }

      if (result === false) {
        renderOnboarding();
        return;
      }
      if (result === 'complete') {
        return;
      }
      goToOnboardingStep(1);
    }

    function handleOnboardingAction() {
      syncOnboardingDraftFromCurrentStep();
      const steps = getOnboardingSteps();
      const step = steps[onboardingStepIndex];
      if (step?.action) {
        closeOnboarding(false);
        step.action();
      }
    }

    async function saveOnboardingSettings() {
      const draft = ensureOnboardingDraft();
      const zip = draft.zipcode.trim();
      const interval = Number(draft.interval);
      const mapAddress = storeMapAddress(draft.mapAddress || '');
      const destinations = [];
      let mapAddressResolved = true;

      if (!/^\d{5}(?:-\d{4})?$/.test(zip)) {
        setOnboardingFeedback('Enter a valid 5-digit ZIP code before continuing.', 'error');
        return false;
      }

      if (!Number.isFinite(interval) || interval < 1 || interval > 1440) {
        setOnboardingFeedback('Choose an interval between 1 and 1440 minutes.', 'error');
        return false;
      }

      if (draft.webhookUrl) {
        try {
          const parsed = new URL(draft.webhookUrl);
          if (!/^https?:$/.test(parsed.protocol)) throw new Error('invalid');
        } catch (error) {
          setOnboardingFeedback('Use a valid Discord webhook URL or leave it blank for now.', 'error');
          return false;
        }
        const destination = { url: draft.webhookUrl };
        if (draft.webhookRoleId) {
          destination.role_id = draft.webhookRoleId.replace(/[^\d]/g, '');
        }
        destinations.push(destination);
      }

      try {
        const resolvedReference = await resolveMapAddressReference(mapAddress);
        mapAddressResolved = !mapAddress || Boolean(resolvedReference);
      } catch (error) {
        mapAddressResolved = false;
      }

      const result = await apiCall('/api/configure', 'POST', {
        zipcode: zip,
        check_interval_minutes: interval,
        discord_destinations: destinations
      });

      if (!result) {
        setOnboardingFeedback('Settings could not be saved. Check the API and try again.', 'error');
        return false;
      }

      document.getElementById('cfg-zip').value = zip;
      document.getElementById('cfg-interval').value = interval;
      document.getElementById('cfg-map-address').value = mapAddress;
      setSettingsMapAddressStatus(
        mapAddress && mapAddressResolved
          ? 'Saved locally for map view in this browser.'
          : mapAddress
            ? 'Saved locally, but this address still needs a more exact match for map distance calculations.'
            : 'Type at least 3 characters for address suggestions.',
        mapAddress && !mapAddressResolved ? 'info' : 'success'
      );
      syncSettingsMapAddressUi();
      manualLocationQuery = mapAddress;
      renderDiscordDestinationRows(destinations);
      await loadSettings();
      latestStatusSnapshot = await loadStatus();
      setOnboardingFeedback(
        mapAddress && !mapAddressResolved
          ? 'Settings saved. Your map-view address was stored locally, but it still needs a more exact match for distance calculations.'
          : 'Settings saved. Next we will add your first product.',
        mapAddress && !mapAddressResolved ? 'info' : 'success'
      );
      return true;
    }

    async function saveOnboardingProduct() {
      const draft = ensureOnboardingDraft();
      const url = draft.productUrl.trim();
      const customName = draft.productName.trim();
      const existingProducts = trackedProductsCache.length || (latestStatusSnapshot?.status?.tracked_products || []).length || 0;

      if (!url) {
        if (existingProducts > 0) {
          setOnboardingFeedback('You already have a tracked product, so we can move on.', 'success');
          return true;
        }
        setOnboardingFeedback('Paste a Walgreens product link before continuing.', 'error');
        return false;
      }

      const resolved = await apiCall('/api/products/resolve', 'POST', { url });
      if (!resolved) {
        setOnboardingFeedback('That link did not resolve. Double-check the product URL.', 'error');
        return false;
      }

      const payload = { url };
      if (customName) payload.name = customName;

      const result = await apiCall('/api/products/add', 'POST', payload);
      if (!result) {
        setOnboardingFeedback('The product could not be added right now. Try again in a moment.', 'error');
        return false;
      }

      draft.productUrl = '';
      draft.productName = '';
      document.getElementById('new-product-url').value = '';
      document.getElementById('new-product-name').value = '';
      await loadSettings();
      latestStatusSnapshot = await loadStatus();
      setOnboardingFeedback(`Added ${customName || resolved.name}.`, 'success');
      return true;
    }

    async function finishOnboardingLaunch() {
      const draft = ensureOnboardingDraft();
      const shouldRunCheck = Boolean(draft.runManualCheck);
      const shouldStartScheduler = Boolean(draft.startScheduler);
      const schedulerAlreadyRunning = Boolean(latestStatusSnapshot?.status?.is_running);

      closeOnboarding(true);
      openTab('results');

      if (shouldStartScheduler && !schedulerAlreadyRunning) {
        await startScheduler();
      }

      if (shouldRunCheck) {
        await triggerManualCheck();
      } else {
        await loadStatus();
        showToast(shouldStartScheduler ? 'Setup finished and scheduler started.' : 'Setup finished. Run your first check when ready.', 'success');
      }

      return 'complete';
    }

    function maybeOpenOnboarding(statusPayload) {
      if (!currentUser || onboardingAutoPrompted || hasCompletedOnboarding()) return;

      const status = statusPayload?.status || {};
      const stats = statusPayload?.statistics || {};
      const looksLikeFirstRun = (
        (stats.total_checks || 0) === 0 &&
        (status.discord_webhook_count || 0) === 0 &&
        !(status.current_zipcode || '').trim() &&
        ((status.tracked_products || []).length <= 1)
      );

      if (!looksLikeFirstRun) return;
      openOnboarding(true);
    }

    async function waitForGoogleIdentity(maxMs = 10000) {
      const startedAt = Date.now();
      while (!window.google?.accounts?.id) {
        if ((Date.now() - startedAt) >= maxMs) {
          return false;
        }
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      return true;
    }

    function renderGoogleSignInButton(clientId) {
      const container = document.getElementById('google-signin-button');
      if (!container) return;
      if (googleButtonRevealTimer) {
        window.clearTimeout(googleButtonRevealTimer);
        googleButtonRevealTimer = 0;
      }

      if (!clientId) {
        container.innerHTML = '';
        container.classList.remove('is-pending');
        updateAuthStatus('');
        return;
      }

      if (!window.google?.accounts?.id) {
        container.classList.add('is-pending');
        updateAuthStatus('Loading Google sign-in...');
        return;
      }

      container.innerHTML = '';
      container.classList.add('is-pending');
      window.google.accounts.id.initialize({
        client_id: clientId,
        callback: handleGoogleCredentialResponse,
        auto_select: false
      });
      window.google.accounts.id.renderButton(container, {
        theme: 'outline',
        size: 'large',
        shape: 'rectangular',
        text: 'continue_with',
        width: 320
      });
      googleButtonRevealTimer = window.setTimeout(() => {
        container.classList.remove('is-pending');
        googleButtonRevealTimer = 0;
      }, 700);
      googleButtonRendered = true;
      updateAuthStatus('');
    }

    function setAuthenticatedUi(isAuthenticated) {
      const authGate = document.getElementById('auth-gate');
      const appShell = document.getElementById('app-shell');
      if (authGate) authGate.hidden = isAuthenticated;
      if (appShell) appShell.hidden = !isAuthenticated;
      document.body.classList.toggle('auth-gate-active', !isAuthenticated);
      document.body.classList.toggle('app-active', isAuthenticated);
      queueShellScrollbarUpdate();
    }

    function updateCurrentUserDisplay(user) {
      const pill = document.getElementById('user-pill');
      const avatar = document.getElementById('user-pill-avatar');
      const name = document.getElementById('user-pill-name');
      const email = document.getElementById('user-pill-email');

      if (!pill || !avatar || !name || !email) return;

      if (!user) {
        pill.hidden = true;
        return;
      }

      pill.hidden = false;
      avatar.src = user.picture || '/frubgreens.webp';
      avatar.alt = user.name ? `${user.name} avatar` : 'User avatar';
      name.textContent = user.name || 'Signed in';
      email.textContent = user.email || '';
    }

    async function handleGoogleCredentialResponse(response) {
      if (!response?.credential) {
        updateAuthStatus('Google sign-in did not return a credential.');
        return;
      }

      updateAuthStatus('Verifying Google sign-in...');
      let result = null;
      try {
        const authResponse = await fetch(apiUrl('/api/auth/google'), {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ credential: response.credential })
        });
        result = await authResponse.json();
        const isWaitlisted = result?.access_state === 'waitlisted';
        if ((!authResponse.ok && !isWaitlisted) || (!result?.authenticated && !isWaitlisted)) {
          const message = result?.error || 'Google sign-in failed. Check your backend Google client ID and allowed origins.';
          updateAuthStatus(message);
          return;
        }
      } catch (error) {
        updateAuthStatus('Google sign-in failed. Check your connection and try again.');
        return;
      }

      await hydrateAuthenticatedSession(result);
    }

    async function hydrateAuthenticatedSession(sessionPayload = null) {
      const payload = sessionPayload || await apiCall('/api/auth/session', 'GET', null, { allowUnauthorized: true });
      authState = payload;
      currentUser = payload?.user || null;
      const isWaitlisted = payload?.access_state === 'waitlisted';
      resetOnboardingSessionState();
      updateCurrentUserDisplay(currentUser);

      if (!currentUser) {
        setBootLoaderPreference(false);
        stopStatusPolling();
        if (isWaitlisted) {
          startAuthStatePolling();
        } else {
          stopAuthStatePolling();
        }
        setAuthenticatedUi(false);
        setAuthGateMode(isWaitlisted ? 'waitlist' : 'signin', payload);
        refreshPublicStatsForLogin();
        if (!isWaitlisted && payload?.google_client_id) {
          const googleReady = await waitForGoogleIdentity();
          if (googleReady) {
            renderGoogleSignInButton(payload.google_client_id);
          } else {
            updateAuthStatus('Google sign-in did not finish loading. Refresh and try again.');
          }
        } else {
          renderGoogleSignInButton('');
          updateAuthStatus('');
        }
        if (!isWaitlisted && payload?.access_denied_reason) {
          updateAuthStatus(payload.access_denied_reason);
        }
        return false;
      }

      setBootLoaderPreference(true);
      stopAuthStatePolling();
      setAuthenticatedUi(true);
      setAuthGateMode('signin');
      updateAuthStatus('');
      await loadSettings();
      const statusPayload = await loadStatus();
      maybeOpenOnboarding(statusPayload);
      startStatusPolling();
      return true;
    }

    async function logoutUser() {
      if (window.google?.accounts?.id?.disableAutoSelect) {
        window.google.accounts.id.disableAutoSelect();
      }
      await apiCall('/api/auth/logout', 'POST', {}, { allowUnauthorized: true, suppressAuthRedirect: true });
      currentUser = null;
      authState = null;
      latestStatusSnapshot = null;
      setBootLoaderPreference(false);
      stopStatusPolling();
      stopAuthStatePolling();
      closeOnboarding(false);
      resetOnboardingSessionState();
      localStorage.removeItem('cached_api_status');
      localStorage.removeItem('cached_api_last_check');
      updateCurrentUserDisplay(null);
      setAuthenticatedUi(false);
      const sessionPayload = await apiCall('/api/auth/session', 'GET', null, { allowUnauthorized: true, suppressAuthRedirect: true });
      await hydrateAuthenticatedSession(sessionPayload);
      updateAuthStatus('Signed out.');
    }

    async function persistPokemonBackgroundSettings() {
      const enabledInput = document.getElementById('cfg-pokemon-bg-enabled');
      const themeInput = document.getElementById('cfg-pokemon-bg-theme');
      const sizeInput = document.getElementById('cfg-pokemon-bg-size');
      if (!enabledInput || !themeInput || !sizeInput) return;

      const result = await apiCall('/api/configure', 'POST', {
        pokemon_background_enabled: enabledInput.checked,
        pokemon_background_theme: themeInput.value || DEFAULT_POKEMON_BACKGROUND_THEME,
        pokemon_background_tile_size: normalizePokemonBackgroundTileSize(sizeInput.value)
      });

      if (!result) {
        showToast('Failed to save Pokemon background setting', 'error');
      }
    }

    function initializePokemonBackgroundControls() {
      const enabledInput = document.getElementById('cfg-pokemon-bg-enabled');
      const themeInput = document.getElementById('cfg-pokemon-bg-theme');
      const sizeInput = document.getElementById('cfg-pokemon-bg-size');
      if (!enabledInput || !themeInput || !sizeInput) return;

      themeInput.innerHTML = POKEMON_BACKGROUNDS.map(background =>
        `<option value="${background.value}">${background.label}</option>`
      ).join('');

      enabledInput.addEventListener('change', () => {
        applyPokemonBackgroundSettings({
          enabled: enabledInput.checked,
          theme: themeInput.value || DEFAULT_POKEMON_BACKGROUND_THEME,
          tileSize: sizeInput.value
        });
        persistPokemonBackgroundSettings();
      });

      themeInput.addEventListener('change', () => {
        applyPokemonBackgroundSettings({
          enabled: enabledInput.checked,
          theme: themeInput.value,
          tileSize: sizeInput.value
        });
        persistPokemonBackgroundSettings();
      });

      sizeInput.addEventListener('input', () => {
        updatePokemonBackgroundSizeLabel(sizeInput.value);
        applyPokemonBackgroundSettings({
          enabled: enabledInput.checked,
          theme: themeInput.value || DEFAULT_POKEMON_BACKGROUND_THEME,
          tileSize: sizeInput.value,
          syncControls: false
        });
      });

      sizeInput.addEventListener('change', () => {
        persistPokemonBackgroundSettings();
      });

      applyPokemonBackgroundSettings();
    }

    function formatIntervalLabel(minutes) {
      const totalMinutes = Number(minutes) || 0;
      if (!totalMinutes) return 'Not configured';
      if (totalMinutes === 60) return 'Every 1 hour';
      if (totalMinutes % 60 === 0) {
        const hours = totalMinutes / 60;
        return `Every ${hours} hour${hours === 1 ? '' : 's'}`;
      }
      return `Every ${totalMinutes} minute${totalMinutes === 1 ? '' : 's'}`;
    }

    function discordDestinationRowHtml(destination = {}) {
      const url = destination.url || '';
      const roleId = destination.role_id || '';
      return `
    <div class="discord-destination">
      <div class="form-group">
        <label class="form-label">Webhook URL</label>
        <input type="url" class="form-input discord-webhook-url" placeholder="https://discord.com/api/webhooks/..." value="${escapeHtml(url)}">
      </div>
      <div class="form-group">
        <label class="form-label">Role ID</label>
        <input type="text" class="form-input discord-role-id" inputmode="numeric" placeholder="Optional role id" value="${escapeHtml(roleId)}">
      </div>
      <div class="discord-row-actions">
        <button class="btn btn-danger-icon" type="button" data-action="remove-discord-destination" title="Remove">
          <span class="btn-icon" style="-webkit-mask-image: url('/trash.webp'); mask-image: url('/trash.webp'); width: 20px; height: 20px;" aria-hidden="true"></span>
        </button>
      </div>
    </div>
  `;
    }

    function renderDiscordDestinationRows(destinations = []) {
      const container = document.getElementById('cfg-discord-destinations');
      const rows = (destinations && destinations.length) ? destinations : [{}];
      container.innerHTML = rows.map(discordDestinationRowHtml).join('');
    }

    function addDiscordDestinationRow(destination = {}) {
      const container = document.getElementById('cfg-discord-destinations');
      container.insertAdjacentHTML('beforeend', discordDestinationRowHtml(destination));
    }

    function removeDiscordDestinationRow(button) {
      const container = document.getElementById('cfg-discord-destinations');
      const row = button.closest('.discord-destination');
      if (row) row.remove();
      if (!container.children.length) {
        addDiscordDestinationRow();
      }
    }

    function getProductItemElement(element) {
      return element ? element.closest('.product-item') : null;
    }

    function getProductIdFromElement(element) {
      return getProductItemElement(element)?.dataset?.productId || '';
    }

    function collectDiscordDestinations() {
      return Array.from(document.querySelectorAll('.discord-destination')).map(row => {
        const url = row.querySelector('.discord-webhook-url')?.value.trim() || '';
        const roleId = (row.querySelector('.discord-role-id')?.value || '').replace(/[^\d]/g, '');
        const destination = { url };
        if (roleId) destination.role_id = roleId;
        return destination;
      }).filter(destination => destination.url);
    }

    function escapeHtml(value) {
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function isAllowedWalgreensHost(hostname) {
      const normalized = String(hostname || '').trim().toLowerCase();
      return Boolean(normalized) && (normalized === 'walgreens.com' || normalized.endsWith('.walgreens.com'));
    }

    function isAllowedProductSourceHost(hostname) {
      return isAllowedWalgreensHost(hostname);
    }

    function isAllowedProductImageHost(hostname) {
      const normalized = String(hostname || '').trim().toLowerCase();
      return isAllowedProductSourceHost(normalized);
    }

    function sanitizeHttpUrl(value, { hostValidator = null } = {}) {
      const normalized = String(value || '').trim();
      if (!normalized) return '';

      try {
        const parsed = new URL(normalized);
        if (!/^https?:$/.test(parsed.protocol)) return '';
        if (typeof hostValidator === 'function' && !hostValidator(parsed.hostname)) return '';
        return parsed.toString();
      } catch (error) {
        return '';
      }
    }

    function sanitizeProductSourceUrl(value) {
      return sanitizeHttpUrl(value, { hostValidator: isAllowedProductSourceHost });
    }

    function sanitizeProductImageUrl(value) {
      return sanitizeHttpUrl(value, { hostValidator: isAllowedProductImageHost });
    }

    function retailerKey(retailer) {
      return 'walgreens';
    }

    function retailerLabel(retailer) {
      return 'Walgreens';
    }

    function retailerIconPath(retailer) {
      return '/icons/walgreens.png';
    }

    function renderTrackedProducts(products = trackedProductsCache) {
      trackedProductsCache = Array.isArray(products) ? products : [];
      const list = document.getElementById('products-list');
      if (!list) return;

      list.innerHTML = trackedProductsCache.map(p => {
        const isEditing = editingProductId === p.id;
        const safeName = escapeHtml(p.name || 'Unnamed product');
        const safePlanogram = escapeHtml(p.planogram || 'missing-planogram');
        const safeId = escapeHtml(p.id || '');
        const safeRetailer = escapeHtml(retailerLabel(p.retailer));
        const safeRetailerIcon = escapeHtml(retailerIconPath(p.retailer));

        return `
            <div class="product-item${isEditing ? ' is-editing' : ''}" data-product-id="${safeId}">
              <div class="product-info">
                <div class="product-name-row">
                  <img class="product-retailer-icon" src="${safeRetailerIcon}" alt="${safeRetailer}" title="${safeRetailer}">
                  ${isEditing
            ? `<input type="text" class="form-input product-name-input" value="${safeName}" maxlength="160" placeholder="Product name">`
            : `<div class="product-name">${safeName}</div>`
          }
                </div>
                <div class="product-id">${safeId} | ${safePlanogram}</div>
              </div>
              <div class="product-actions">
                ${isEditing
            ? `
                    <button class="btn btn-secondary btn-sm" type="button" data-action="cancel-product-name-edit">Cancel</button>
                    <button class="btn btn-primary btn-sm" type="button" data-action="save-product-name">Save</button>
                  `
            : `
                    <button class="btn btn-secondary btn-sm" type="button" data-action="start-product-name-edit">Edit</button>
                    <button class="btn btn-danger-icon" type="button" data-action="remove-product" title="Remove">
                      <span class="btn-icon" style="-webkit-mask-image: url('/trash.webp'); mask-image: url('/trash.webp'); width: 20px; height: 20px;" aria-hidden="true"></span>
                    </button>
                  `
          }
              </div>
            </div>`;
      }).join('');

      if (editingProductId) {
        const input = list.querySelector('.product-item.is-editing .product-name-input');
        if (input) {
          input.focus();
          input.select();
        }
      }
    }

    function startProductNameEdit(productId) {
      editingProductId = productId;
      renderTrackedProducts();
    }

    function cancelProductNameEdit() {
      editingProductId = null;
      renderTrackedProducts();
    }

    function handleProductNameKeydown(event, productId, sourceElement = null) {
      if (event.key === 'Enter') {
        event.preventDefault();
        saveProductName(productId, sourceElement);
        return;
      }

      if (event.key === 'Escape') {
        event.preventDefault();
        cancelProductNameEdit();
      }
    }

    async function saveProductName(productId, sourceElement = null) {
      const row = getProductItemElement(sourceElement);
      const input = row ? row.querySelector('.product-name-input') : null;
      const nextName = input ? input.value.trim() : '';

      if (!nextName) {
        showToast('Product name cannot be empty', 'error');
        if (input) input.focus();
        return;
      }

      const result = await apiCall('/api/products/update', 'POST', { id: productId, name: nextName });
      if (result) {
        editingProductId = null;
        showToast('Product name updated', 'success');
        await loadSettings();
      } else {
        showToast('Failed to update product name', 'error');
      }
    }

    function formatDistance(distance) {
      return typeof distance === 'number' && Number.isFinite(distance)
        ? `${distance.toFixed(2)} mi`
        : 'Distance unavailable';
    }

    function normalizeCoordinate(value) {
      const coordinate = Number(value);
      return Number.isFinite(coordinate) ? coordinate : null;
    }

    function calculateDistanceMiles(originLatitude, originLongitude, targetLatitude, targetLongitude) {
      const toRadians = value => (value * Math.PI) / 180;
      const earthRadiusMiles = 3958.7613;
      const latitudeDelta = toRadians(targetLatitude - originLatitude);
      const longitudeDelta = toRadians(targetLongitude - originLongitude);
      const a = Math.sin(latitudeDelta / 2) ** 2
        + Math.cos(toRadians(originLatitude))
        * Math.cos(toRadians(targetLatitude))
        * Math.sin(longitudeDelta / 2) ** 2;
      return 2 * earthRadiusMiles * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    function getStoreDistance(store, referenceLocation = null) {
      const latitude = normalizeCoordinate(store?.latitude);
      const longitude = normalizeCoordinate(store?.longitude);
      const referenceLatitude = normalizeCoordinate(referenceLocation?.latitude);
      const referenceLongitude = normalizeCoordinate(referenceLocation?.longitude);
      if (referenceLatitude !== null && referenceLongitude !== null && latitude !== null && longitude !== null) {
        return calculateDistanceMiles(
          referenceLatitude,
          referenceLongitude,
          latitude,
          longitude
        );
      }

      const fallbackDistance = Number(store?.distance);
      return Number.isFinite(fallbackDistance) ? fallbackDistance : null;
    }

    function buildResultsMapLocations(products = {}, referenceLocation = null) {
      const storeMap = new Map();
      let missingCoordinates = 0;

      Object.values(products || {}).forEach(product => {
        const productName = String(product.product_name || 'Tracked product').trim();
        const stores = Array.isArray(product.stores) ? product.stores : [];

        stores.forEach(store => {
          const latitude = normalizeCoordinate(store.latitude);
          const longitude = normalizeCoordinate(store.longitude);

          if (latitude === null || longitude === null) {
            missingCoordinates += 1;
            return;
          }

          const storeKey = String(store.store_id || `${latitude},${longitude}`);
          if (!storeMap.has(storeKey)) {
            storeMap.set(storeKey, {
              location_key: storeKey,
              store_id: store.store_id || '',
              name: store.name || 'Walgreens',
              address: store.address || 'Address unavailable',
              distance: getStoreDistance(store, referenceLocation),
              latitude,
              longitude,
              total_inventory: 0,
              products: []
            });
          }

          const location = storeMap.get(storeKey);
          const inventoryCount = Number(store.inventory_count || 0);
          location.total_inventory += Number.isFinite(inventoryCount) ? inventoryCount : 0;
          location.products.push({
            name: productName,
            inventory_count: Number.isFinite(inventoryCount) ? inventoryCount : 0
          });
        });
      });

      const locations = Array.from(storeMap.values())
        .map(location => ({
          ...location,
          product_count: location.products.length,
          distance: Number.isFinite(location.distance) ? location.distance : null,
          products_label: location.products.map(product => product.name).join(', ')
        }))
        .sort((a, b) => {
          if (a.distance === null && b.distance === null) return b.total_inventory - a.total_inventory;
          if (a.distance === null) return 1;
          if (b.distance === null) return -1;
          return a.distance - b.distance;
        });

      return { locations, missingCoordinates };
    }

    async function resolveMapAddressReference(address, { persist = true } = {}) {
      const normalizedAddress = String(address || '').trim();
      if (!normalizedAddress) {
        return setMapDistanceReference(null, { persist });
      }

      if (mapDistanceReference && mapDistanceReference.label === normalizedAddress) {
        return mapDistanceReference;
      }

      const suggestions = await requestManualLocationSuggestions(normalizedAddress);
      if (!suggestions.length) {
        return setMapDistanceReference(null, { persist });
      }

      return setMapDistanceReference(
        {
          label: normalizedAddress,
          latitude: suggestions[0].latitude,
          longitude: suggestions[0].longitude
        },
        { persist }
      );
    }

    async function ensureSavedMapAddressReference() {
      const savedAddress = loadStoredMapAddress();
      if (!savedAddress) {
        setMapDistanceReference(null, { persist: true });
        return null;
      }
      if (mapDistanceReference?.label === savedAddress) {
        return mapDistanceReference;
      }
      try {
        return await resolveMapAddressReference(savedAddress);
      } catch (error) {
        return mapDistanceReference;
      }
    }

    function destroyResultsMap() {
      resultsMapBaseLayer = null;
      resultsMapMarkers = [];
      resultsMapMarkersByKey = new Map();
      activeResultsMapKey = null;
      if (userLocationMarker) {
        userLocationMarker.remove();
        userLocationMarker = null;
      }
      if (!resultsMap) return;
      resultsMap.remove();
      resultsMap = null;
    }

    function renderMapPopup(location) {
      const productsHtml = location.products.map(product => `
          <div>${escapeHtml(product.name)} <strong>${Number(product.inventory_count || 0)}</strong></div>
        `).join('');

      return `
          <div class="map-popup-title">${escapeHtml(location.name)}</div>
          <div class="map-popup-address">${renderAddressLink(location.address)}</div>
          <div class="map-popup-meta">${location.product_count} product${location.product_count === 1 ? '' : 's'} | ${location.total_inventory} total units${location.distance !== null ? ` | ${escapeHtml(formatDistance(location.distance))}` : ''}</div>
          <div class="map-popup-products">${productsHtml}</div>
        `;
    }

    function setActiveResultsMapItem(locationKey) {
      activeResultsMapKey = locationKey || null;
      document.querySelectorAll('.results-map-item').forEach(item => {
        item.classList.toggle('is-active', item.dataset.locationKey === activeResultsMapKey);
      });
      resultsMapMarkers.forEach(marker => {
        const element = marker?.getElement?.();
        if (!element) return;
        element.classList.toggle('is-active', marker?.options?.locationKey === activeResultsMapKey);
      });
    }

    function createResultsMapMarkerIcon(location) {
      const markerClasses = [
        'results-store-marker',
        Number(location?.product_count || 0) > 1 ? 'is-multi' : ''
      ].filter(Boolean).join(' ');

      return L.divIcon({
        className: '',
        html: `
          <div class="${markerClasses}">
            <span class="results-store-marker__halo"></span>
            <span class="results-store-marker__core"></span>
          </div>
        `,
        iconSize: [24, 24],
        iconAnchor: [12, 23],
        popupAnchor: [0, -16]
      });
    }

    function focusResultsMapMarker(locationKey) {
      const marker = resultsMapMarkersByKey.get(locationKey);
      if (!resultsMap || !marker) return;

      resultsMap.invalidateSize();
      const currentZoom = resultsMap.getZoom ? resultsMap.getZoom() : 12;
      const targetZoom = Math.max(currentZoom, 14);
      const targetLatLng = marker.getLatLng();
      resultsMap.once('moveend', () => {
        marker.openPopup();
      });
      if (typeof resultsMap.flyTo === 'function') {
        resultsMap.flyTo(targetLatLng, targetZoom, {
          animate: true,
          duration: 0.8,
          easeLinearity: 0.25
        });
      } else {
        resultsMap.setView(targetLatLng, targetZoom, { animate: true });
      }
      setActiveResultsMapItem(locationKey);
    }

    function focusResultsMapMarkerByElement(element) {
      const locationKey = element?.dataset?.locationKey;
      if (!locationKey) return;
      focusResultsMapMarker(locationKey);
    }

    function getManualLocationSearchElements() {
      return {
        panel: document.getElementById('map-location-search'),
        toggle: document.getElementById('map-location-search-toggle'),
        input: document.getElementById('manual-location-input'),
        status: document.getElementById('map-location-search-status'),
        results: document.getElementById('map-location-search-results'),
        submit: document.querySelector('[data-action="submit-manual-location"]')
      };
    }

    function setManualLocationStatus(message, tone = 'muted') {
      manualLocationStatus = {
        message: String(message || '').trim() || 'Enter an address, city, or ZIP to center the map.',
        tone
      };
    }

    function cancelManualLocationAutocomplete() {
      if (manualLocationAutocompleteTimer) {
        window.clearTimeout(manualLocationAutocompleteTimer);
        manualLocationAutocompleteTimer = 0;
      }

      if (manualLocationAutocompleteController) {
        manualLocationAutocompleteController.abort();
        manualLocationAutocompleteController = null;
      }

      manualLocationAutocompleteBusy = false;
    }

    function renderManualLocationSuggestionsHtml() {
      return manualLocationSuggestions.map((suggestion, index) => `
          <button
            class="map-location-suggestion ${index === manualLocationHighlightedIndex ? 'is-active' : ''}"
            type="button"
            data-action="select-manual-location-suggestion"
            data-suggestion-index="${index}">
            <span class="map-location-suggestion-label">${escapeHtml(suggestion.label)}</span>
          </button>
        `).join('');
    }

    function setSettingsMapAddressStatus(message = '', tone = 'muted') {
      settingsMapAddressStatus = {
        message: String(message || ''),
        tone: String(tone || 'muted')
      };
    }

    function renderSettingsMapAddressSuggestionsHtml() {
      return settingsMapAddressSuggestions.map((suggestion, index) => `
          <button
            class="map-location-suggestion ${index === settingsMapAddressHighlightedIndex ? 'is-active' : ''}"
            type="button"
            data-action="select-settings-map-address-suggestion"
            data-suggestion-index="${index}">
            <span class="map-location-suggestion-label">${escapeHtml(suggestion.label)}</span>
          </button>
        `).join('');
    }

    function syncSettingsMapAddressUi(options = {}) {
      const { focusInput = false } = options;
      const input = document.getElementById('cfg-map-address');
      const status = document.getElementById('settings-map-address-status');
      const results = document.getElementById('settings-map-address-results');

      if (status) {
        status.textContent = settingsMapAddressStatus.message;
        status.dataset.tone = settingsMapAddressStatus.tone || 'muted';
      }

      if (results) {
        const hasSuggestions = settingsMapAddressSuggestions.length > 0;
        results.hidden = !hasSuggestions;
        results.innerHTML = hasSuggestions ? renderSettingsMapAddressSuggestionsHtml() : '';
      }

      if (input && focusInput) {
        window.requestAnimationFrame(() => {
          input.focus({ preventScroll: true });
        });
      }
    }

    function syncManualLocationSearchUi(options = {}) {
      const { focusInput = false } = options;
      const { panel, toggle, input, status, results, submit } = getManualLocationSearchElements();

      if (panel) {
        panel.hidden = !manualLocationFallbackVisible;
      }

      if (toggle) {
        toggle.classList.toggle('is-active', manualLocationFallbackVisible);
        toggle.setAttribute('aria-expanded', manualLocationFallbackVisible ? 'true' : 'false');
        toggle.dataset.action = manualLocationFallbackVisible ? 'hide-manual-location-fallback' : 'show-manual-location-fallback';
      }

      if (input && document.activeElement !== input) {
        input.value = manualLocationQuery;
      }

      if (status) {
        status.textContent = manualLocationStatus.message;
        status.dataset.tone = manualLocationStatus.tone || 'muted';
      }

      if (results) {
        const hasSuggestions = manualLocationSuggestions.length > 0;
        results.hidden = !hasSuggestions;
        results.innerHTML = hasSuggestions ? renderManualLocationSuggestionsHtml() : '';
      }

      if (submit) {
        submit.disabled = manualLocationAutocompleteBusy || !manualLocationQuery.trim();
      }

      if (input && focusInput) {
        window.requestAnimationFrame(() => {
          input.focus({ preventScroll: true });
          input.select();
        });
      }
    }

    function showManualLocationFallback(message = '', tone = 'info') {
      manualLocationFallbackVisible = true;
      if (!manualLocationQuery) {
        manualLocationQuery = loadStoredMapAddress();
      }
      if (message) {
        setManualLocationStatus(message, tone);
      }
      syncManualLocationSearchUi({ focusInput: true });
    }

    function hideManualLocationFallback(options = {}) {
      const { preserveQuery = true } = options;
      manualLocationFallbackVisible = false;
      manualLocationSuggestions = [];
      manualLocationHighlightedIndex = -1;
      cancelManualLocationAutocomplete();
      if (!preserveQuery) {
        manualLocationQuery = '';
      }
      setManualLocationStatus('Enter an address, city, or ZIP to center the map.');
      syncManualLocationSearchUi();
    }

    async function requestManualLocationSuggestions(query) {
      const normalizedQuery = String(query || '').trim();
      const requestId = ++manualLocationAutocompleteRequestId;
      const controller = new AbortController();

      if (manualLocationAutocompleteController) {
        manualLocationAutocompleteController.abort();
      }
      manualLocationAutocompleteController = controller;

      try {
        const params = new URLSearchParams({
          format: 'jsonv2',
          addressdetails: '1',
          limit: '5',
          countrycodes: 'us',
          q: normalizedQuery
        });
        const response = await fetch(`https://nominatim.openstreetmap.org/search?${params.toString()}`, {
          signal: controller.signal,
          headers: {
            Accept: 'application/json'
          }
        });

        if (!response.ok) {
          throw new Error(`Address lookup failed (${response.status})`);
        }

        const payload = await response.json();
        if (requestId !== manualLocationAutocompleteRequestId) {
          return [];
        }

        return Array.isArray(payload)
          ? payload.map(item => {
            const label = String(item?.display_name || '').trim();
            const latitude = Number(item?.lat);
            const longitude = Number(item?.lon);
            if (!label || !Number.isFinite(latitude) || !Number.isFinite(longitude)) {
              return null;
            }
            return { label, latitude, longitude };
          }).filter(Boolean)
          : [];
      } finally {
        if (manualLocationAutocompleteController === controller) {
          manualLocationAutocompleteController = null;
        }
      }
    }

    async function loadManualLocationSuggestions(query) {
      const normalizedQuery = String(query || '').trim();

      if (normalizedQuery.length < 3) {
        cancelManualLocationAutocomplete();
        manualLocationSuggestions = [];
        manualLocationHighlightedIndex = -1;
        setManualLocationStatus('Type at least 3 characters for address suggestions.');
        syncManualLocationSearchUi();
        return [];
      }

      manualLocationAutocompleteBusy = true;
      setManualLocationStatus('Searching addresses...', 'info');
      syncManualLocationSearchUi();

      try {
        const suggestions = await requestManualLocationSuggestions(normalizedQuery);
        manualLocationSuggestions = suggestions;
        manualLocationHighlightedIndex = suggestions.length ? 0 : -1;
        setManualLocationStatus(
          suggestions.length
            ? 'Choose a suggestion or press Enter to use the first match.'
            : 'No matches yet. Try a street, city, or ZIP.',
          suggestions.length ? 'info' : 'error'
        );
        return suggestions;
      } catch (error) {
        if (error?.name === 'AbortError') {
          return [];
        }

        console.warn('Manual location lookup error:', error);
        manualLocationSuggestions = [];
        manualLocationHighlightedIndex = -1;
        setManualLocationStatus('Could not load address suggestions right now.', 'error');
        return [];
      } finally {
        manualLocationAutocompleteBusy = false;
        syncManualLocationSearchUi();
      }
    }

    function queueManualLocationAutocomplete(query) {
      manualLocationQuery = String(query || '');

      if (manualLocationAutocompleteTimer) {
        window.clearTimeout(manualLocationAutocompleteTimer);
        manualLocationAutocompleteTimer = 0;
      }

      if (String(query || '').trim().length < 3) {
        cancelManualLocationAutocomplete();
        manualLocationSuggestions = [];
        manualLocationHighlightedIndex = -1;
        setManualLocationStatus('Type at least 3 characters for address suggestions.');
        syncManualLocationSearchUi();
        return;
      }

      manualLocationSuggestions = [];
      manualLocationHighlightedIndex = -1;
      setManualLocationStatus('Searching addresses...', 'info');
      manualLocationAutocompleteTimer = window.setTimeout(() => {
        manualLocationAutocompleteTimer = 0;
        void loadManualLocationSuggestions(manualLocationQuery);
      }, 280);
      syncManualLocationSearchUi();
    }

    async function loadSettingsMapAddressSuggestions(query) {
      const normalizedQuery = String(query || '').trim();

      if (normalizedQuery.length < 3) {
        settingsMapAddressSuggestions = [];
        settingsMapAddressHighlightedIndex = -1;
        settingsMapAddressAutocompleteBusy = false;
        setSettingsMapAddressStatus('Type at least 3 characters for address suggestions.');
        syncSettingsMapAddressUi();
        return [];
      }

      settingsMapAddressAutocompleteBusy = true;
      setSettingsMapAddressStatus('Searching addresses...', 'info');
      syncSettingsMapAddressUi();

      try {
        const suggestions = await requestManualLocationSuggestions(normalizedQuery);
        settingsMapAddressSuggestions = suggestions;
        settingsMapAddressHighlightedIndex = suggestions.length ? 0 : -1;
        setSettingsMapAddressStatus(
          suggestions.length
            ? 'Choose a suggestion to save this map location.'
            : 'No matches yet. Try a street, city, or ZIP.',
          suggestions.length ? 'info' : 'error'
        );
        return suggestions;
      } catch (error) {
        if (error?.name === 'AbortError') {
          return [];
        }

        console.warn('Settings address lookup error:', error);
        settingsMapAddressSuggestions = [];
        settingsMapAddressHighlightedIndex = -1;
        setSettingsMapAddressStatus('Could not load address suggestions right now.', 'error');
        return [];
      } finally {
        settingsMapAddressAutocompleteBusy = false;
        syncSettingsMapAddressUi();
      }
    }

    function queueSettingsMapAddressAutocomplete(query) {
      const normalizedQuery = String(query || '');

      if (settingsMapAddressAutocompleteTimer) {
        window.clearTimeout(settingsMapAddressAutocompleteTimer);
        settingsMapAddressAutocompleteTimer = 0;
      }

      if (normalizedQuery.trim().length < 3) {
        settingsMapAddressSuggestions = [];
        settingsMapAddressHighlightedIndex = -1;
        settingsMapAddressAutocompleteBusy = false;
        setSettingsMapAddressStatus('Type at least 3 characters for address suggestions.');
        syncSettingsMapAddressUi();
        return;
      }

      settingsMapAddressSuggestions = [];
      settingsMapAddressHighlightedIndex = -1;
      setSettingsMapAddressStatus('Searching addresses...', 'info');
      settingsMapAddressAutocompleteTimer = window.setTimeout(() => {
        settingsMapAddressAutocompleteTimer = 0;
        void loadSettingsMapAddressSuggestions(normalizedQuery);
      }, 280);
      syncSettingsMapAddressUi();
    }

    function clearSettingsMapAddressSuggestions() {
      if (settingsMapAddressAutocompleteTimer) {
        window.clearTimeout(settingsMapAddressAutocompleteTimer);
        settingsMapAddressAutocompleteTimer = 0;
      }
      settingsMapAddressSuggestions = [];
      settingsMapAddressHighlightedIndex = -1;
      settingsMapAddressAutocompleteBusy = false;
      setSettingsMapAddressStatus('Type at least 3 characters for address suggestions.');
      syncSettingsMapAddressUi();
    }

    function centerMapOnManualLocation(suggestion) {
      if (!suggestion) return;

      manualLocationQuery = storeMapAddress(suggestion.label);
      setMapDistanceReference(
        {
          label: suggestion.label,
          latitude: suggestion.latitude,
          longitude: suggestion.longitude
        },
        { persist: true }
      );
      const mapAddressInput = document.getElementById('cfg-map-address');
      if (mapAddressInput) {
        mapAddressInput.value = manualLocationQuery;
      }
      hideManualLocationFallback({ preserveQuery: true });
      if (latestRenderedResults) {
        renderResultsContent(latestRenderedResults.data, latestRenderedResults.configuredZip);
      }
      updateUserLocationOnMap({
        coords: {
          latitude: suggestion.latitude,
          longitude: suggestion.longitude
        }
      });
      showToast('Centered map on the entered address', 'success');
    }

    function applySettingsMapAddressSuggestion(suggestion) {
      if (!suggestion) return;

      const normalizedLabel = storeMapAddress(suggestion.label);
      setMapDistanceReference(
        {
          label: normalizedLabel,
          latitude: suggestion.latitude,
          longitude: suggestion.longitude
        },
        { persist: true }
      );
      manualLocationQuery = normalizedLabel;
      const input = document.getElementById('cfg-map-address');
      if (input) {
        input.value = normalizedLabel;
      }
      settingsMapAddressSuggestions = [];
      settingsMapAddressHighlightedIndex = -1;
      setSettingsMapAddressStatus('Saved for map view in this browser.', 'success');
      syncSettingsMapAddressUi();
      if (latestRenderedResults && currentResultsView === 'map') {
        renderResultsContent(latestRenderedResults.data, latestRenderedResults.configuredZip);
      }
      showToast('Saved map address updated', 'success');
    }

    function selectManualLocationSuggestionByIndex(index) {
      const suggestion = manualLocationSuggestions[index];
      if (!suggestion) return;
      centerMapOnManualLocation(suggestion);
    }

    function selectSettingsMapAddressSuggestionByIndex(index) {
      const suggestion = settingsMapAddressSuggestions[index];
      if (!suggestion) return;
      applySettingsMapAddressSuggestion(suggestion);
    }

    async function submitManualLocationSearch() {
      const { input } = getManualLocationSearchElements();
      const query = String(input?.value || manualLocationQuery || '').trim();

      if (!query) {
        setManualLocationStatus('Enter an address, city, or ZIP first.', 'error');
        syncManualLocationSearchUi({ focusInput: true });
        return;
      }

      manualLocationQuery = query;

      if (manualLocationSuggestions.length) {
        const suggestionIndex = manualLocationHighlightedIndex >= 0 ? manualLocationHighlightedIndex : 0;
        selectManualLocationSuggestionByIndex(suggestionIndex);
        return;
      }

      cancelManualLocationAutocomplete();
      manualLocationAutocompleteBusy = true;
      setManualLocationStatus('Looking up that address...', 'info');
      syncManualLocationSearchUi();

      try {
        const suggestions = await requestManualLocationSuggestions(query);
        manualLocationSuggestions = suggestions;
        manualLocationHighlightedIndex = suggestions.length ? 0 : -1;
        if (!suggestions.length) {
          setManualLocationStatus('No matching address found. Try a fuller street, city, or ZIP.', 'error');
          return;
        }
        centerMapOnManualLocation(suggestions[0]);
      } catch (error) {
        if (error?.name === 'AbortError') {
          return;
        }

        console.warn('Manual location search error:', error);
        setManualLocationStatus('Address lookup is unavailable right now.', 'error');
      } finally {
        manualLocationAutocompleteBusy = false;
        syncManualLocationSearchUi();
      }
    }

    function handleManualLocationInput(value) {
      if (!manualLocationFallbackVisible) {
        manualLocationFallbackVisible = true;
      }
      queueManualLocationAutocomplete(value);
    }

    async function handleActionClick(actionElement) {
      const action = actionElement?.dataset?.action || '';
      if (!action) return;

      switch (action) {
        case 'prompt-install':
          await promptInstallApp();
          return;
        case 'close-onboarding':
          closeOnboarding(actionElement.dataset.markComplete === 'true');
          return;
        case 'open-tech-stack':
          openTechStackModal();
          return;
        case 'close-tech-stack':
          closeTechStackModal();
          return;
        case 'onboarding-back':
          goToOnboardingStep(-1);
          return;
        case 'onboarding-jump':
          handleOnboardingAction();
          return;
        case 'onboarding-next':
          await handleOnboardingNext();
          return;
        case 'show-manual-location-fallback':
          showManualLocationFallback('Enter an address, city, or ZIP to center the map.');
          return;
        case 'hide-manual-location-fallback':
          hideManualLocationFallback();
          return;
        case 'submit-manual-location':
          await submitManualLocationSearch();
          return;
        case 'select-manual-location-suggestion':
          selectManualLocationSuggestionByIndex(Number(actionElement.dataset.suggestionIndex || -1));
          return;
        case 'select-settings-map-address-suggestion':
          selectSettingsMapAddressSuggestionByIndex(Number(actionElement.dataset.suggestionIndex || -1));
          return;
        case 'show-my-location':
          await handleShowMyLocation();
          return;
        case 'start-scheduler':
          await startScheduler();
          return;
        case 'stop-scheduler':
          await stopScheduler();
          return;
        case 'trigger-check':
          await triggerManualCheck();
          return;
        case 'open-tab':
          openTab(actionElement.dataset.tabName || 'results');
          return;
        case 'open-onboarding':
          openOnboarding(true);
          return;
        case 'logout':
          await logoutUser();
          return;
        case 'add-discord-destination':
          addDiscordDestinationRow();
          return;
        case 'remove-discord-destination':
          removeDiscordDestinationRow(actionElement);
          return;
        case 'save-settings':
          await saveSettings();
          return;
        case 'add-product':
          await addProduct();
          return;
        case 'cancel-product-name-edit':
          cancelProductNameEdit();
          return;
        case 'save-product-name': {
          const productId = getProductIdFromElement(actionElement);
          if (productId) {
            await saveProductName(productId, actionElement);
          }
          return;
        }
        case 'start-product-name-edit': {
          const productId = getProductIdFromElement(actionElement);
          if (productId) {
            startProductNameEdit(productId);
          }
          return;
        }
        case 'remove-product': {
          const productId = getProductIdFromElement(actionElement);
          if (productId) {
            await removeProduct(productId);
          }
          return;
        }
        case 'focus-results-marker':
          focusResultsMapMarkerByElement(actionElement);
          return;
        case 'set-results-view':
          setResultsView(actionElement.dataset.resultsView || 'cards');
          return;
        case 'toggle-history-item':
          toggleHistoryItem(actionElement);
          return;
        default:
          return;
      }
    }

    const GEOLOCATION_HIGH_ACCURACY_OPTIONS = Object.freeze({
      enableHighAccuracy: true,
      timeout: 8000,
      maximumAge: 0
    });

    const GEOLOCATION_FALLBACK_OPTIONS = Object.freeze({
      enableHighAccuracy: false,
      timeout: 12000,
      maximumAge: 300000
    });

    function requestCurrentPosition(options) {
      return new Promise((resolve, reject) => {
        navigator.geolocation.getCurrentPosition(resolve, reject, options);
      });
    }

    function getGeolocationErrorMessage(error, fallbackAttempted = false) {
      if (!error || typeof error.code !== 'number') {
        return 'Failed to get location';
      }

      if (error.code === error.PERMISSION_DENIED) {
        return 'Permission denied. Please enable location services.';
      }

      if (error.code === error.TIMEOUT) {
        return fallbackAttempted
          ? 'Location request timed out. Try again or use a less restrictive browser location setting.'
          : 'Location request timed out. Retrying with a faster fallback...';
      }

      if (error.code === error.POSITION_UNAVAILABLE) {
        return fallbackAttempted
          ? 'Your location is unavailable right now. Please try again.'
          : 'Precise location is unavailable. Retrying with a broader estimate...';
      }
      return 'Failed to get location';
    }

    function ensureUserLocationMarker(lat, lng) {
      if (!resultsMap) return;
      if (userLocationMarker) {
        userLocationMarker.setLatLng([lat, lng]);
        return;
      }

      const userPulseIcon = L.divIcon({
        className: 'user-location-pulse-wrapper',
        html: '<div class="user-location-pulse"></div>',
        iconSize: [14, 14],
        iconAnchor: [7, 7]
      });
      userLocationMarker = L.marker([lat, lng], { icon: userPulseIcon }).addTo(resultsMap);
    }

    function updateUserLocationOnMap(position) {
      const lat = position.coords.latitude;
      const lng = position.coords.longitude;

      setMapDistanceReference(
        {
          label: 'Current location',
          latitude: lat,
          longitude: lng
        },
        { persist: false }
      );

      if (latestRenderedResults && currentResultsView === 'map') {
        renderResultsContent(latestRenderedResults.data, latestRenderedResults.configuredZip);
      }

      if (!resultsMap) return;
      ensureUserLocationMarker(lat, lng);
      resultsMap.setView([lat, lng], 14, { animate: true });
    }

    async function focusSavedMapLocation() {
      const savedReference = await ensureSavedMapAddressReference();
      if (!savedReference || !resultsMap) return false;
      ensureUserLocationMarker(savedReference.latitude, savedReference.longitude);
      resultsMap.setView([savedReference.latitude, savedReference.longitude], 14, { animate: true });
      hideManualLocationFallback({ preserveQuery: true });
      showToast(`Centered map on ${savedReference.label || 'your saved address'}`, 'success');
      return true;
    }

    async function handleShowMyLocation() {
      if (await focusSavedMapLocation()) {
        return;
      }

      if (!navigator.geolocation) {
        showToast('Location services are not supported by your browser', 'error');
        showManualLocationFallback('Browser location is unavailable here. Enter an address, city, or ZIP instead.', 'info');
        return;
      }

      showToast('Finding your location...', 'info');

      try {
        const position = await requestCurrentPosition(GEOLOCATION_HIGH_ACCURACY_OPTIONS);
        updateUserLocationOnMap(position);
        showToast('Located', 'success');
        return;
      } catch (error) {
        const canRetry =
          error &&
          (error.code === error.TIMEOUT || error.code === error.POSITION_UNAVAILABLE);

        if (!canRetry) {
          console.warn('Geolocation error:', error);
          showManualLocationFallback('Live location failed. Enter an address, city, or ZIP instead.', 'info');
          showToast(getGeolocationErrorMessage(error, true), 'error');
          return;
        }

        showToast(getGeolocationErrorMessage(error), 'info');
      }

      try {
        const fallbackPosition = await requestCurrentPosition(GEOLOCATION_FALLBACK_OPTIONS);
        updateUserLocationOnMap(fallbackPosition);
        showToast('Located using an approximate location', 'success');
      } catch (fallbackError) {
        console.warn('Geolocation fallback error:', fallbackError);
        showManualLocationFallback('Desktop location still failed. Enter an address, city, or ZIP instead.', 'info');
        showToast(getGeolocationErrorMessage(fallbackError, true), 'error');
      }
    }

    function renderResultsMap(locations = []) {
      const mapElement = document.getElementById('results-map');
      if (!mapElement || typeof L === 'undefined') return;

      destroyResultsMap();

      resultsMap = L.map(mapElement, {
        zoomControl: true,
        scrollWheelZoom: true
      });

      createResultsMapThemeControl().addTo(resultsMap);
      applyResultsMapTheme(currentResultsMapTheme, { persist: false });

      const bounds = [];
      resultsMapMarkersByKey = new Map();
      resultsMapMarkers = locations.map(location => {
        const marker = L.marker([location.latitude, location.longitude], {
          icon: createResultsMapMarkerIcon(location),
          keyboard: false,
          locationKey: location.location_key
        }).addTo(resultsMap);

        marker.bindPopup(renderMapPopup(location));
        marker.on('click', () => setActiveResultsMapItem(location.location_key));
        marker.on('popupopen', () => setActiveResultsMapItem(location.location_key));
        bounds.push([location.latitude, location.longitude]);
        resultsMapMarkersByKey.set(location.location_key, marker);
        return marker;
      });

      if (mapDistanceReference) {
        ensureUserLocationMarker(mapDistanceReference.latitude, mapDistanceReference.longitude);
        bounds.push([mapDistanceReference.latitude, mapDistanceReference.longitude]);
      }

      if (bounds.length === 1) {
        resultsMap.setView(bounds[0], 13);
      } else if (bounds.length > 1) {
        resultsMap.fitBounds(bounds, { padding: [32, 32] });
      } else {
        resultsMap.setView([33.4484, -112.0740], 10);
      }

      setTimeout(() => {
        if (resultsMap) {
          resultsMap.invalidateSize();
          if (locations.length) {
            setActiveResultsMapItem(activeResultsMapKey || locations[0].location_key);
          }
        }
      }, 0);
    }

    function renderResultsMapList(locations = []) {
      if (!locations.length) {
        return `
            <div class="empty-state">
              <div class="empty-title">Map unavailable</div>
              <div class="empty-text">Run a fresh successful scan to capture mappable store coordinates.</div>
            </div>
          `;
      }

      return `
          <div class="results-map-list">
            ${locations.map((location, index) => `
              <button class="results-map-item ${activeResultsMapKey === location.location_key ? 'is-active' : ''}" type="button" data-action="focus-results-marker" data-location-key="${escapeHtml(location.location_key)}">
                <div class="results-map-item-title">${escapeHtml(location.name)}</div>
                <div class="results-map-item-address">${escapeHtml(location.address)}</div>
                <div class="results-map-item-meta">${location.product_count} product${location.product_count === 1 ? '' : 's'} | ${location.total_inventory} units${location.distance !== null ? ` | ${escapeHtml(formatDistance(location.distance))}` : ''}</div>
                <div class="results-map-item-products">${escapeHtml(location.products_label)}</div>
              </button>
            `).join('')}
          </div>
        `;
    }

    function renderResultsContent(data, configuredZip = '') {
      latestRenderedResults = { data, configuredZip };
      const checkResult = data.check_result || {};
      const products = data.products_found || {};
      const total = checkResult.total_stores_checked || data.total_stores_checked || 0;
      const timestamp = data.timestamp || checkResult.timestamp;
      const productCount = Object.keys(products).length;
      const mapData = buildResultsMapLocations(products, mapDistanceReference);

      if (currentResultsView === 'map' && !mapData.locations.length) {
        currentResultsView = 'cards';
      }

      const cardsHtml = Object.entries(products).map(([, product]) => renderResultCard(product, configuredZip)).join('');
      const formattedDate = formatLocalDateTime(timestamp) || 'Latest successful scan';

      document.getElementById('results-container').innerHTML = `
          <div class="results-shell">
            <div class="results-toolbar">
              <div class="results-toolbar-copy">
                <div class="results-toolbar-title">Successful Scan Snapshot</div>
                <div class="results-toolbar-subtitle">${productCount} product${productCount === 1 ? '' : 's'} hit across ${mapData.locations.length} mapped location${mapData.locations.length === 1 ? '' : 's'} after checking ${total} store${total === 1 ? '' : 's'}${timestamp ? ` on ${formattedDate}` : ''}.</div>
              </div>
              <div class="results-view-switch" role="tablist" aria-label="Results view">
                <button class="results-view-btn ${currentResultsView === 'cards' ? 'active' : ''}" type="button" data-action="set-results-view" data-results-view="cards">List</button>
                <button class="results-view-btn ${currentResultsView === 'map' ? 'active' : ''}" type="button" data-action="set-results-view" data-results-view="map" ${mapData.locations.length ? '' : 'disabled title="Run a new successful scan to enable map pins"'}>Map</button>
              </div>
            </div>
            <div class="results-view ${currentResultsView === 'cards' ? 'active' : ''}" id="results-view-cards">
              <div class="results-stack">${cardsHtml}</div>
            </div>
            <div class="results-view ${currentResultsView === 'map' ? 'active' : ''}" id="results-view-map">
              <div class="results-map-shell">
                <div class="results-map-card">
                  <div class="results-map-canvas" id="results-map"></div>
                  <div class="map-location-search" id="map-location-search" ${manualLocationFallbackVisible ? '' : 'hidden'}>
                    <div class="map-location-search-head">
                      <div class="map-location-search-title">Manual Location</div>
                      <button class="map-location-search-close" type="button" data-action="hide-manual-location-fallback">Close</button>
                    </div>
                    <div class="map-location-search-copy">If browser location stalls on desktop, enter an address, city, or ZIP and we will center the map there.</div>
                    <div class="map-location-search-row">
                      <input
                        class="map-location-search-input"
                        id="manual-location-input"
                        type="text"
                        autocomplete="street-address"
                        spellcheck="false"
                        placeholder="Enter address, city, or ZIP"
                        value="${escapeHtml(manualLocationQuery)}">
                      <button class="btn btn-secondary map-location-search-submit" type="button" data-action="submit-manual-location" ${manualLocationAutocompleteBusy || !manualLocationQuery.trim() ? 'disabled' : ''}>Use</button>
                    </div>
                    <div class="map-location-search-status" id="map-location-search-status" data-tone="${escapeHtml(manualLocationStatus.tone || 'muted')}" aria-live="polite">${escapeHtml(manualLocationStatus.message)}</div>
                    <div class="map-location-search-results" id="map-location-search-results" ${manualLocationSuggestions.length ? '' : 'hidden'}>
                      ${renderManualLocationSuggestionsHtml()}
                    </div>
                  </div>
                  <div class="map-utility-controls" aria-label="Map controls">
                    <button
                      class="map-utility-btn map-search-toggle-btn ${manualLocationFallbackVisible ? 'is-active' : ''}"
                      id="map-location-search-toggle"
                      type="button"
                      data-action="${manualLocationFallbackVisible ? 'hide-manual-location-fallback' : 'show-manual-location-fallback'}"
                      aria-label="Search address"
                      title="Search address"
                      aria-controls="map-location-search"
                      aria-expanded="${manualLocationFallbackVisible ? 'true' : 'false'}">
                      <span class="map-utility-icon map-utility-icon-add-location" aria-hidden="true"></span>
                    </button>
                    <button
                      class="map-utility-btn map-locate-btn"
                      type="button"
                      data-action="show-my-location"
                      aria-label="Show my location"
                      title="Show my location">
                      <span class="map-utility-icon map-utility-icon-location" aria-hidden="true"></span>
                    </button>
                  </div>
                </div>
                <aside class="results-map-sidebar">
                  <div class="results-map-sidebar-head">
                    <div class="results-map-sidebar-title">Pinned Store View</div>
                    <div class="results-map-sidebar-subtitle">Tap a store to jump the map and open its stock summary.</div>
                    ${mapData.missingCoordinates ? `<div class="results-map-note">${mapData.missingCoordinates} stocked store result${mapData.missingCoordinates === 1 ? '' : 's'} from older cache data could not be pinned yet.</div>` : ''}
                  </div>
                  ${renderResultsMapList(mapData.locations)}
                </aside>
              </div>
            </div>
          </div>
        `;

      if (currentResultsView === 'map' && mapData.locations.length) {
        renderResultsMap(mapData.locations);
      } else {
        destroyResultsMap();
      }

      syncManualLocationSearchUi();
    }

    async function setResultsView(view) {
      currentResultsView = view === 'map' ? 'map' : 'cards';
      if (currentResultsView === 'map') {
        await ensureSavedMapAddressReference();
      }
      if (!latestRenderedResults) return;
      renderResultsContent(latestRenderedResults.data, latestRenderedResults.configuredZip);
    }

    function parseApiTimestamp(value) {
      const normalized = String(value || '').trim();
      if (!normalized) return null;

      const hasTimezone = /(?:Z|[+-]\d{2}:\d{2})$/i.test(normalized);
      const parsed = new Date(hasTimezone ? normalized : `${normalized}Z`);
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    }

    function formatLocalDateTime(value, options = {}) {
      const parsed = parseApiTimestamp(value);
      return parsed ? parsed.toLocaleString(undefined, options) : '';
    }

    function formatLocalDate(value, options = {}) {
      const parsed = parseApiTimestamp(value);
      return parsed ? parsed.toLocaleDateString(undefined, options) : '';
    }

    function formatLocalTime(value, options = {}) {
      const parsed = parseApiTimestamp(value);
      return parsed ? parsed.toLocaleTimeString(undefined, options) : '';
    }

    function buildDirectionsUrl(address) {
      const normalizedAddress = String(address || '').trim();
      return normalizedAddress
        ? `https://www.google.com/maps/dir/?api=1&destination=${encodeURIComponent(normalizedAddress)}`
        : '';
    }

    function renderAddressLink(address) {
      const normalizedAddress = String(address || '').trim();
      const safeAddress = escapeHtml(normalizedAddress || 'Address unavailable');
      const directionsUrl = buildDirectionsUrl(normalizedAddress);

      if (!directionsUrl) {
        return `<span class="result-store-address">${safeAddress}</span>`;
      }

      return `<a class="result-store-address" href="${directionsUrl}" target="_blank" rel="noopener noreferrer" title="Open Google Maps directions">${safeAddress}</a>`;
    }

    function nearestStore(stores = []) {
      if (!stores.length) return null;
      return [...stores].sort((a, b) => {
        const aDistance = typeof a.distance === 'number' ? a.distance : Number.POSITIVE_INFINITY;
        const bDistance = typeof b.distance === 'number' ? b.distance : Number.POSITIVE_INFINITY;
        return aDistance - bDistance;
      })[0];
    }

    function renderStoreRows(stores = []) {
      return stores.map((store, index) => `
    <div class="result-store-item">
      <div class="result-store-index">${index + 1}.</div>
      <div class="result-store-copy">
        ${renderAddressLink(store.address)}
        <div class="result-store-meta">Qty: <strong>${Number(store.inventory_count || 0)}</strong> | Distance: ${escapeHtml(formatDistance(store.distance))}</div>
      </div>
    </div>
  `).join('');
    }

    function renderStockKicker(zipDisplay, retailer) {
      return `
    <div class="result-card-kicker">
      <span class="result-card-kicker-icon" aria-hidden="true"></span>
      <span>${escapeHtml(retailerLabel(retailer))} In Stock Near ${zipDisplay}</span>
    </div>
  `;
    }

    function renderResultCard(product, configuredZip) {
      const stores = Array.isArray(product.stores) ? product.stores : [];
      const nearest = nearestStore(stores);
      const productName = escapeHtml(product.product_name || 'Tracked product');
      const imageUrl = sanitizeProductImageUrl(product.image_url || '');
      const sourceUrl = sanitizeProductSourceUrl(product.source_url || '');
      const zipDisplay = escapeHtml(configuredZip || 'configured ZIP');
      const nearestAddress = nearest ? renderAddressLink(nearest.address) : 'No stores found';
      const nearestDistance = nearest ? formatDistance(nearest.distance) : 'Distance unavailable';

      return `
    <article class="result-card">
      <div class="result-card-header">
        <div class="result-card-copy">
          ${renderStockKicker(zipDisplay, product.retailer)}
          <div class="result-card-title-row">
            <div class="result-card-title">${productName}</div>
            ${sourceUrl ? `<a class="result-card-link" href="${escapeHtml(sourceUrl)}" target="_blank" rel="noopener noreferrer">Product Page</a>` : ''}
          </div>
          <div class="result-card-subtitle">${Number(product.count || 0)} stores in stock | ${Number(product.total_inventory || 0)} total units</div>
        </div>
        ${imageUrl ? `
          <div class="result-card-thumb">
            <img src="${escapeHtml(imageUrl)}" alt="${productName}">
          </div>
        ` : ''}
      </div>

      <div class="result-card-stats">
        <div class="result-stat">
          <div class="result-stat-label">Store Hits</div>
          <div class="result-stat-value">${Number(product.count || 0)}</div>
        </div>
        <div class="result-stat">
          <div class="result-stat-label">Units</div>
          <div class="result-stat-value">${Number(product.total_inventory || 0)}</div>
        </div>
        <div class="result-stat">
          <div class="result-stat-label">Nearest</div>
          <div class="result-stat-text">${nearestAddress}</div>
          <div class="result-store-meta">Distance: ${escapeHtml(nearestDistance)}</div>
        </div>
      </div>

      <details class="result-details">
        <summary>
          <span class="result-details-label">Store details</span>
          <span class="result-details-hint">${stores.length} location${stores.length === 1 ? '' : 's'}</span>
        </summary>
        <div class="result-stores">
          ${renderStoreRows(stores)}
        </div>
      </details>
    </article>
  `;
    }

    function renderCachedState() {
      try {
        const statusData = JSON.parse(localStorage.getItem('cached_api_status'));
        if (statusData && statusData.status) {
          applyPokemonBackgroundSettings({
            enabled: statusData.status.pokemon_background_enabled ?? DEFAULT_POKEMON_BACKGROUND_ENABLED,
            theme: statusData.status.pokemon_background_theme || DEFAULT_POKEMON_BACKGROUND_THEME,
            tileSize: statusData.status.pokemon_background_tile_size ?? DEFAULT_POKEMON_BACKGROUND_TILE_SIZE,
            syncControls: false
          });
        }

        const checkData = JSON.parse(localStorage.getItem('cached_api_last_check'));
        if (checkData) {
          const products = checkData.products_found || {};
          if (Object.keys(products).length > 0) {
            const configuredZip = statusData?.status?.current_zipcode || '';
            renderResultsContent(checkData, configuredZip);
            lastResultsSignature = buildResultsSignature(checkData);
          }
        }
      } catch (e) {
        console.error('Failed to render cache', e);
      }
    }

    document.addEventListener('DOMContentLoaded', () => {
      const footerYear = document.getElementById('footer-year');
      if (footerYear) {
        footerYear.textContent = String(new Date().getFullYear());
      }
      initializeShellScrollbar();
      ensureOnboardingModalRoot();
      ensureTechStackModalRoot();
      renderCachedState();
      initializePokemonBackgroundControls();
      if (window.matchMedia) {
        installDisplayModeQueries = [
          window.matchMedia('(display-mode: standalone)'),
          window.matchMedia('(display-mode: window-controls-overlay)'),
          window.matchMedia('(display-mode: minimal-ui)'),
          window.matchMedia('(display-mode: fullscreen)')
        ];
        installDisplayModeQueries.forEach(query => {
          if (typeof query.addEventListener === 'function') {
            query.addEventListener('change', refreshInstallButtons);
          } else if (typeof query.addListener === 'function') {
            query.addListener(refreshInstallButtons);
          }
        });
      }
      renderPublicStats();
      refreshPublicStatsForLogin();
      refreshInstallButtons();
      registerServiceWorker();
      waitForInitialFonts();
      hydrateAuthenticatedSession().finally(() => {
        hasInitialSessionResolved = true;
        finalizeInitialReveal();
      });
    });

    document.addEventListener('keydown', event => {
      if (event.target instanceof HTMLElement && event.target.classList.contains('product-name-input')) {
        const productId = getProductIdFromElement(event.target);
        if (productId) {
          handleProductNameKeydown(event, productId, event.target);
          if (event.defaultPrevented) return;
        }
      }

      if (event.target instanceof HTMLInputElement && event.target.id === 'manual-location-input') {
        if (event.key === 'ArrowDown' && manualLocationSuggestions.length) {
          event.preventDefault();
          manualLocationHighlightedIndex = (manualLocationHighlightedIndex + 1) % manualLocationSuggestions.length;
          syncManualLocationSearchUi();
          return;
        }

        if (event.key === 'ArrowUp' && manualLocationSuggestions.length) {
          event.preventDefault();
          manualLocationHighlightedIndex = manualLocationHighlightedIndex <= 0
            ? manualLocationSuggestions.length - 1
            : manualLocationHighlightedIndex - 1;
          syncManualLocationSearchUi();
          return;
        }

        if (event.key === 'Enter') {
          event.preventDefault();
          void submitManualLocationSearch();
          return;
        }

        if (event.key === 'Escape' && manualLocationSuggestions.length) {
          event.preventDefault();
          manualLocationSuggestions = [];
          manualLocationHighlightedIndex = -1;
          syncManualLocationSearchUi();
          return;
        }

        if (event.key === 'Escape' && manualLocationFallbackVisible) {
          event.preventDefault();
          hideManualLocationFallback();
          return;
        }
      }

      if (event.target instanceof HTMLInputElement && event.target.id === 'cfg-map-address') {
        if (event.key === 'ArrowDown' && settingsMapAddressSuggestions.length) {
          event.preventDefault();
          settingsMapAddressHighlightedIndex = (settingsMapAddressHighlightedIndex + 1) % settingsMapAddressSuggestions.length;
          syncSettingsMapAddressUi();
          return;
        }

        if (event.key === 'ArrowUp' && settingsMapAddressSuggestions.length) {
          event.preventDefault();
          settingsMapAddressHighlightedIndex = settingsMapAddressHighlightedIndex <= 0
            ? settingsMapAddressSuggestions.length - 1
            : settingsMapAddressHighlightedIndex - 1;
          syncSettingsMapAddressUi();
          return;
        }

        if (event.key === 'Enter' && settingsMapAddressSuggestions.length) {
          event.preventDefault();
          const suggestionIndex = settingsMapAddressHighlightedIndex >= 0 ? settingsMapAddressHighlightedIndex : 0;
          selectSettingsMapAddressSuggestionByIndex(suggestionIndex);
          return;
        }

        if (event.key === 'Escape' && settingsMapAddressSuggestions.length) {
          event.preventDefault();
          settingsMapAddressSuggestions = [];
          settingsMapAddressHighlightedIndex = -1;
          setSettingsMapAddressStatus('Type at least 3 characters for address suggestions.');
          syncSettingsMapAddressUi();
          return;
        }
      }

      if (event.key !== 'Escape') return;
      const techStackModal = document.getElementById('tech-stack-modal');
      if (techStackModal && !techStackModal.hidden) {
        closeTechStackModal();
        return;
      }

      const onboardingModal = document.getElementById('onboarding-modal');
      if (!onboardingModal || onboardingModal.hidden) return;
      closeOnboarding(false);
    });

    document.addEventListener('input', event => {
      if (!(event.target instanceof HTMLInputElement)) return;
      if (event.target.id === 'manual-location-input') {
        handleManualLocationInput(event.target.value);
        return;
      }
      if (event.target.id === 'cfg-map-address') {
        queueSettingsMapAddressAutocomplete(event.target.value);
      }
    });

    document.addEventListener('click', async event => {
      const actionElement = event.target instanceof Element ? event.target.closest('[data-action]') : null;
      if (actionElement) {
        await handleActionClick(actionElement);
        return;
      }

      const techStackModal = document.getElementById('tech-stack-modal');
      if (techStackModal && !techStackModal.hidden && event.target === techStackModal) {
        closeTechStackModal();
        return;
      }

      if (
        manualLocationSuggestions.length &&
        event.target instanceof Element &&
        !event.target.closest('#map-location-search') &&
        !event.target.closest('#map-location-search-toggle')
      ) {
        manualLocationSuggestions = [];
        manualLocationHighlightedIndex = -1;
        syncManualLocationSearchUi();
      }

      if (
        settingsMapAddressSuggestions.length &&
        event.target instanceof Element &&
        !event.target.closest('.settings-map-address-field')
      ) {
        settingsMapAddressSuggestions = [];
        settingsMapAddressHighlightedIndex = -1;
        setSettingsMapAddressStatus('Type at least 3 characters for address suggestions.');
        syncSettingsMapAddressUi();
      }

      const onboardingModal = document.getElementById('onboarding-modal');
      if (!onboardingModal || onboardingModal.hidden) return;
      if (event.target === onboardingModal) {
        closeOnboarding(false);
        return;
      }
    });

    window.addEventListener('beforeinstallprompt', event => {
      event.preventDefault();
      deferredInstallPrompt = event;
      refreshInstallButtons();
    });

    window.addEventListener('appinstalled', () => {
      deferredInstallPrompt = null;
      refreshInstallButtons();
      showToast('App installed', 'success');
    });

    function openTab(name) {
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
      document.getElementById(`tab-${name}`).classList.add('active');
      document.getElementById(`panel-${name}`).classList.add('active');
      currentTab = name;
      const settingsButton = document.getElementById('settings-btn');
      if (settingsButton) {
        settingsButton.style.display = name === 'settings' ? 'none' : '';
      }

      if (name === 'results') {
        loadStatus();
      }
      if (name === 'history') {
        loadHistory();
      }
      queueShellScrollbarUpdate();
    }

    function toggleHistoryItem(element) {
      element.classList.toggle('is-expanded');
    }

    async function loadHistory() {
      const data = await apiCall('/api/history?limit=10');
      if (!data || !data.history || data.history.length === 0) {
        document.getElementById('history-container').innerHTML = `
            <div class="empty-state">
              <div class="empty-icon">📜</div>
              <div class="empty-title">No history found</div>
              <div class="empty-text">Check results will appear here after they run.</div>
            </div>`;
        return;
      }

      let html = '<div class="history-list">';
      data.history.slice().reverse().forEach(entry => {
        const timeStr = formatLocalDateTime(entry.timestamp) || 'Unknown time';
        const stockClass = entry.has_stock ? 'status-hit' : 'status-miss';
        const stockText = entry.has_stock ? 'STOCK FOUND' : 'NO STOCK';
        const products = entry.products_found || {};
        const productList = Object.values(products);
        const canExpand = entry.has_stock && productList.length > 0;

        let expandedHtml = '';
        if (canExpand) {
          expandedHtml = '<div class="history-item-details">';
          productList.forEach(p => {
            expandedHtml += `
                <div class="history-product-detail">
                  <span class="hpd-name">${escapeHtml(p.product_name)}</span>
                  <span class="hpd-stats">${p.count} stores | ${p.total_inventory} units</span>
                </div>
              `;
          });
          expandedHtml += '</div>';
        }

        html += `
            <div class="history-item ${canExpand ? 'expandable' : ''}"${canExpand ? ' data-action="toggle-history-item"' : ''}>
              <div class="history-main">
                <div class="history-meta">
                  <span class="history-time">${timeStr}</span>
                  <span class="history-badge ${stockClass}">${stockText}</span>
                </div>
                <div class="history-summary">
                  ${entry.check_result?.total_stores_checked || 0} stores scanned
                  ${canExpand ? '<span class="history-expand-icon"></span>' : ''}
                </div>
              </div>
              ${expandedHtml}
            </div>
          `;
      });
      html += '</div>';
      document.getElementById('history-container').innerHTML = html;
    }

    async function apiCall(url, method = 'GET', data = null, options = {}) {
      try {
        const opts = {
          method,
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' }
        };
        if (data) opts.body = JSON.stringify(data);
        const r = await fetch(apiUrl(url), opts);
        if (r.status === 401) {
          if (!options.allowUnauthorized && !options.suppressAuthRedirect) {
            currentUser = null;
            authState = null;
            stopStatusPolling();
            stopAuthStatePolling();
            updateCurrentUserDisplay(null);
            setAuthenticatedUi(false);
            const sessionPayload = await apiCall('/api/auth/session', 'GET', null, {
              allowUnauthorized: true,
              suppressAuthRedirect: true
            });
            authState = sessionPayload;
            if (sessionPayload?.google_client_id) {
              renderGoogleSignInButton(sessionPayload.google_client_id);
            }
            updateAuthStatus('Your session expired. Sign in again to continue.');
          }
          return null;
        }
        if (!r.ok) {
          let errorMessage = `HTTP ${r.status}`;
          try {
            const errorData = await r.json();
            errorMessage = errorData.error || errorData.message || errorMessage;
          } catch (e) {
          }
          throw new Error(errorMessage);
        }
        return await r.json();
      } catch (e) {
        console.error(`API error (${url}):`, e);
        if (!options.silent) {
          console.warn(e.message || 'Request failed');
        }
        return null;
      }
    }

    function isEditingSettingsPanel() {
      if (currentTab !== 'settings') return false;
      const activeElement = document.activeElement;
      if (!activeElement) return false;

      const settingsPanel = document.getElementById('panel-settings');
      if (!settingsPanel || !settingsPanel.contains(activeElement)) return false;

      return ['INPUT', 'TEXTAREA', 'SELECT', 'BUTTON'].includes(activeElement.tagName);
    }

    function buildResultsSignature(data) {
      const checkResult = data.check_result || {};
      return JSON.stringify({
        timestamp: data.timestamp || checkResult.timestamp || null,
        total: checkResult.total_stores_checked || data.total_stores_checked || 0,
        products: data.products_found || {}
      });
    }

    async function loadStatus() {
      if (isStatusRefreshInFlight) return;
      isStatusRefreshInFlight = true;

      try {
        const data = await apiCall('/api/status');
        if (!data) {
          return null;
        }
        latestStatusSnapshot = data;
        localStorage.setItem('cached_api_status', JSON.stringify(data));
        const status = data.status || {};
        const trackedProducts = status.tracked_products || [];
        const lastProductsFound = status.last_products_found || {};
        const intervalMinutes = status.check_interval_minutes || 60;

        document.getElementById('stat-status').textContent = status.is_running ? 'Running' : 'Stopped';
        document.getElementById('stat-scheduler').textContent = status.is_running ? 'On' : 'Off';
        document.querySelector('#stat-scheduler + .stat-sub').textContent = formatIntervalLabel(intervalMinutes);
        document.getElementById('stat-status-sub').textContent = status.discord_webhook_count
          ? `${status.discord_webhook_count} Discord webhook${status.discord_webhook_count === 1 ? '' : 's'} configured`
          : 'No Discord webhooks configured';
        document.getElementById('stat-products').textContent = trackedProducts.length;
        document.getElementById('stat-products-sub').textContent = `${Object.keys(lastProductsFound).length} in stock last check`;

        const statistics = data.statistics || {};
        if (statistics) {
          document.getElementById('stat-total-checks').textContent = statistics.total_checks || 0;
          const rate = statistics.success_rate || 0;
          document.getElementById('stat-success-rate').textContent = (rate % 1 === 0 ? rate : rate.toFixed(1)) + '%';
        }
        applyPokemonBackgroundSettings({
          enabled: status.pokemon_background_enabled ?? DEFAULT_POKEMON_BACKGROUND_ENABLED,
          theme: status.pokemon_background_theme || DEFAULT_POKEMON_BACKGROUND_THEME,
          tileSize: status.pokemon_background_tile_size ?? DEFAULT_POKEMON_BACKGROUND_TILE_SIZE,
          syncControls: !isEditingSettingsPanel()
        });

        const statLastCheck = document.getElementById('stat-lastcheck');
        if (status.last_check) {
          const localTime = formatLocalTime(status.last_check, {
            hour: 'numeric',
            minute: '2-digit',
            hour12: true
          });
          const localDateTime = formatLocalDateTime(status.last_check, {
            dateStyle: 'medium',
            timeStyle: 'short'
          });
          statLastCheck.textContent = localTime || '-';
          statLastCheck.title = localDateTime || '';
        } else if (statLastCheck) {
          statLastCheck.textContent = '-';
          statLastCheck.title = '';
        }

        const startBtns = document.querySelectorAll('#start-btn, .start-scheduler-btn');
        const stopBtns = document.querySelectorAll('#stop-btn, .stop-scheduler-btn');
        if (status.is_running) {
          startBtns.forEach(btn => btn.style.display = 'none');
          stopBtns.forEach(btn => btn.style.display = 'inline-flex');
        } else {
          startBtns.forEach(btn => btn.style.display = 'inline-flex');
          stopBtns.forEach(btn => btn.style.display = 'none');
        }

        if (currentTab === 'results') {
          await loadResults(status.current_zipcode || document.getElementById('cfg-zip')?.value || '');
        }
        return data;
      } finally {
        isStatusRefreshInFlight = false;
      }
    }

    async function loadResults(configuredZip = '') {
      const data = await apiCall('/api/last-check');
      if (!data) {
        lastResultsSignature = '';
        latestRenderedResults = null;
        destroyResultsMap();
        document.getElementById('results-container').innerHTML = `
      <div class="empty-state">
        <div class="empty-icon">?</div>
        <div class="empty-title">No checks yet</div>
        <div class="empty-text">Click <strong>Check Now</strong> or start the scheduler.</div>
      </div>`;
        return;
      }

      localStorage.setItem('cached_api_last_check', JSON.stringify(data));

      const resultsSignature = buildResultsSignature(data);
      if (resultsSignature === lastResultsSignature) {
        return;
      }

      const checkResult = data.check_result || {};
      const total = checkResult.total_stores_checked || data.total_stores_checked || 0;
      const products = data.products_found || {};
      const timestamp = data.timestamp || checkResult.timestamp;

      if (Object.keys(products).length === 0) {
        latestRenderedResults = null;
        destroyResultsMap();
        document.getElementById('results-container').innerHTML = `
      <div class="empty-state">
      <div class="empty-icon"><img src="/out-of-stock.webp" alt="Out of stock"></div>
      <div class="empty-title">No stock found</div>
      <div class="empty-text">Checked ${total} stores${timestamp ? ` on ${formatLocalDate(timestamp)}` : ''}</div>
      </div>`;
        lastResultsSignature = resultsSignature;
        return;
      }

      renderResultsContent(data, configuredZip);
      lastResultsSignature = resultsSignature;
    }

    async function loadSettings() {
      const storedMapAddress = loadStoredMapAddress();
      const mapAddressInput = document.getElementById('cfg-map-address');
      if (mapAddressInput) {
        mapAddressInput.value = storedMapAddress;
      }
      setSettingsMapAddressStatus(
        storedMapAddress
          ? 'Saved locally for map view in this browser.'
          : 'Type at least 3 characters for address suggestions.'
      );
      syncSettingsMapAddressUi();
      manualLocationQuery = storedMapAddress;
      if (storedMapAddress) {
        await ensureSavedMapAddressReference();
      }
      syncManualLocationSearchUi();

      const data = await apiCall('/api/status');
      if (!data) return;
      const status = data.status || {};
      document.getElementById('cfg-zip').value = status.current_zipcode || '';
      document.getElementById('cfg-interval').value = status.check_interval_minutes || 60;
      applyPokemonBackgroundSettings({
        enabled: status.pokemon_background_enabled ?? DEFAULT_POKEMON_BACKGROUND_ENABLED,
        theme: status.pokemon_background_theme || DEFAULT_POKEMON_BACKGROUND_THEME,
        tileSize: status.pokemon_background_tile_size ?? DEFAULT_POKEMON_BACKGROUND_TILE_SIZE
      });
      renderDiscordDestinationRows(status.discord_destinations || []);
      syncManualLocationSearchUi();

      const products = status.tracked_products || [];
      renderTrackedProducts(products);
    }

    async function saveSettings() {
      const zip = document.getElementById('cfg-zip').value.trim();
      const interval = document.getElementById('cfg-interval').value.trim();
      const mapAddress = storeMapAddress(document.getElementById('cfg-map-address').value.trim());
      const webhookDestinations = collectDiscordDestinations();
      const pokemonBackgroundEnabled = document.getElementById('cfg-pokemon-bg-enabled').checked;
      const pokemonBackgroundTheme = document.getElementById('cfg-pokemon-bg-theme').value || DEFAULT_POKEMON_BACKGROUND_THEME;
      const pokemonBackgroundTileSize = normalizePokemonBackgroundTileSize(document.getElementById('cfg-pokemon-bg-size').value);
      manualLocationQuery = mapAddress;
      syncManualLocationSearchUi();
      let mapAddressResolved = true;

      try {
        const resolvedReference = await resolveMapAddressReference(mapAddress);
        mapAddressResolved = !mapAddress || Boolean(resolvedReference);
      } catch (error) {
        mapAddressResolved = false;
      }

      const data = {};
      if (zip) data.zipcode = zip;
      if (interval) data.check_interval_minutes = Number(interval);
      data.discord_destinations = webhookDestinations;
      data.pokemon_background_enabled = pokemonBackgroundEnabled;
      data.pokemon_background_theme = pokemonBackgroundTheme;
      data.pokemon_background_tile_size = pokemonBackgroundTileSize;

      const result = await apiCall('/api/configure', 'POST', data);
      if (result) {
        applyPokemonBackgroundSettings({
          enabled: result.pokemon_background_enabled ?? pokemonBackgroundEnabled,
          theme: result.pokemon_background_theme || pokemonBackgroundTheme,
          tileSize: result.pokemon_background_tile_size ?? pokemonBackgroundTileSize
        });
        document.getElementById('cfg-map-address').value = mapAddress;
        setSettingsMapAddressStatus(
          mapAddress && mapAddressResolved
            ? 'Saved locally for map view in this browser.'
            : mapAddress
              ? 'Saved, but this address still needs a more exact match for map distance calculations.'
              : 'Type at least 3 characters for address suggestions.',
          mapAddress && !mapAddressResolved ? 'info' : 'success'
        );
        syncSettingsMapAddressUi();
        showToast(
          mapAddress && !mapAddressResolved
            ? 'Settings saved, but the map address could not be resolved for distance calculations yet'
            : 'Settings saved',
          mapAddress && !mapAddressResolved ? 'info' : 'success'
        );
        await loadStatus();
        if (latestRenderedResults) {
          renderResultsContent(latestRenderedResults.data, latestRenderedResults.configuredZip);
        }
      } else {
        document.getElementById('cfg-map-address').value = mapAddress;
        setSettingsMapAddressStatus(
          mapAddress
            ? 'Saved locally, but server settings failed to save.'
            : 'Type at least 3 characters for address suggestions.',
          mapAddress ? 'info' : 'muted'
        );
        syncSettingsMapAddressUi();
        if (latestRenderedResults) {
          renderResultsContent(latestRenderedResults.data, latestRenderedResults.configuredZip);
        }
        showToast('Server settings failed to save, but the map address was stored locally', 'error');
      }
    }

    async function startScheduler() {
      const result = await apiCall('/api/start', 'POST', {});
      if (result) {
        showToast('Scheduler started', 'success');
        await loadStatus();
      }
    }

    async function stopScheduler() {
      const result = await apiCall('/api/stop', 'POST', {});
      if (result) {
        showToast('Scheduler stopped', 'success');
        await loadStatus();
      }
    }

    async function triggerManualCheck() {
      console.log('Check button clicked');
      document.getElementById('check-btn').disabled = true;
      document.getElementById('check-btn-text').textContent = 'Checking...';

      const progressSection = document.getElementById('progress-section');
      progressSection.style.display = 'block';

      showToast('Starting stock check...', 'info');

      try {
        console.log('Calling /api/check');
        const result = await apiCall('/api/check', 'POST', {});
        console.log('Check result:', result);

        if (!result) {
          showToast('Check failed - see console for details', 'error');
          document.getElementById('check-btn').disabled = false;
          document.getElementById('check-btn-text').textContent = 'Check Now';
          progressSection.style.display = 'none';
          return;
        }

        // Poll for progress updates while checking
        let isComplete = false;
        let progressLoops = 0;
        const maxLoops = 1800; // ~4.5 minutes at 150ms per poll
        let finalProgress = null;

        while (!isComplete && progressLoops < maxLoops) {
          await new Promise(r => setTimeout(r, 150));
          progressLoops++;

          const progress = await apiCall('/api/progress');
          if (progress && !progress.in_progress) {
            isComplete = true;
            finalProgress = progress;
            // Final update
            document.getElementById('progress-bar').style.width = '100%';
            document.getElementById('progress-percent').textContent = '100%';
            break;
          }

          if (progress && progress.in_progress) {
            updateProgressDisplay(progress);
          }
        }

        await new Promise(r => setTimeout(r, 500));
        await loadStatus();

        const finalMessage = finalProgress?.message || '';
        const finalPhase = finalProgress?.phase || '';
        const noStoresFound = finalMessage.startsWith('No Walgreens stores found near ');

        if (finalPhase === 'error' || noStoresFound) {
          showToast(finalMessage || 'Check failed', 'error');
        } else {
          showToast('Check complete', 'success');
        }

        // Hide progress section after 2 seconds
        await new Promise(r => setTimeout(r, 2000));
        progressSection.style.display = 'none';
      } catch (error) {
        console.error('Check error:', error);
        showToast('Check failed: ' + error.message, 'error');
        progressSection.style.display = 'none';
      } finally {
        document.getElementById('check-btn').disabled = false;
        document.getElementById('check-btn-text').textContent = 'Check Now';
      }
    }

    function updateProgressDisplay(progress) {
      const pct = progress.progress_percent || 0;
      const phase = progress.phase || '';
      const stage = progress.message || 'Working...';
      const store = progress.current_store || 'Working...';
      const product = progress.current_product || 'Waiting for first product...';
      const checked = progress.stores_processed || progress.stores_checked || 0;
      const total = progress.total_stores || 0;
      const productIndex = progress.current_product_index || 0;
      const totalProducts = progress.total_products || 0;
      const storesWithStock = progress.stores_with_stock_current || 0;

      document.getElementById('progress-bar').style.width = pct + '%';
      document.getElementById('progress-percent').textContent = pct + '%';
      document.getElementById('progress-stage').textContent = `Stage: ${stage}`;
      document.getElementById('progress-product').textContent = totalProducts
        ? `Product: ${productIndex} / ${totalProducts} - ${product}`
        : `Product: ${product}`;
      document.getElementById('progress-store').textContent = `Activity: ${store}`;
      if (!total) {
        document.getElementById('progress-count').textContent = `Store processing: waiting for store list | Current product hits: ${storesWithStock}`;
        return;
      }

      if (phase === 'fetching_inventory') {
        document.getElementById('progress-count').textContent = `Store processing: the retailer is checking all ${total} stores in one live request | Current product hits: ${storesWithStock}`;
        return;
      }

      document.getElementById('progress-count').textContent = `Store processing: ${checked} / ${total} | Current product hits: ${storesWithStock}`;
    }

    async function addProduct() {
      const url = document.getElementById('new-product-url').value.trim();
      const customName = document.getElementById('new-product-name').value.trim();

      if (!url) {
        showToast('Paste a Walgreens product link first', 'error');
        return;
      }

      const resolved = await apiCall('/api/products/resolve', 'POST', { url });
      if (!resolved) {
        showToast('Could not read product details from that link', 'error');
        return;
      }

      const payload = { url };
      if (customName) payload.name = customName;

      const result = await apiCall('/api/products/add', 'POST', payload);
      if (result) {
        showToast(`Added ${customName || resolved.name}`, 'success');
        document.getElementById('new-product-url').value = '';
        document.getElementById('new-product-name').value = '';
        await loadSettings();
      } else {
        showToast('Failed to add product', 'error');
      }
    }

    async function removeProduct(id) {
      const result = await apiCall('/api/products/remove', 'POST', { id });
      if (result) {
        showToast('Product removed', 'success');
        await loadSettings();
      }
    }

    function showToast(msg, type = 'info') {
      const toast = document.createElement('div');
      toast.className = `toast ${type}`;
      toast.textContent = msg;
      document.getElementById('toast-container').appendChild(toast);
      setTimeout(() => {
        toast.classList.add('hide');
        setTimeout(() => toast.remove(), 250);
      }, 3500);
    }

    window.addEventListener('load', () => {
      hasWindowLoaded = true;
      sessionStorage.setItem('assetsLoaded', 'true');
      finalizeInitialReveal();
    });
  