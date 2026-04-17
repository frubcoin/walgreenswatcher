import { chromium } from 'playwright';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const DEFAULT_URL =
    'https://www.cvs.com/shop/pok-mon-poke-ball-tin-with-3-trading-card-packs-coin-blind-capsule-prodid-444357';
const DEFAULT_ZIP = '85208';
const DEFAULT_API_KEY = 'a2ff75c6-2da7-4299-929d-d670d827ab4a';
const DEFAULT_HEADLESS = false;
const DEFAULT_TIMEOUT_MS = 30000;
const DEFAULT_INVENTORY_WAIT_MS = 25000;
const OUTPUT_DIR = path.resolve(process.cwd(), 'output');
const WINDOWS_USER_AGENT =
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36';
const WINDOWS_SEC_CH_UA = '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"';
const WINDOWS_SEC_CH_UA_PLATFORM = '"Windows"';
const WINDOWS_SEC_CH_UA_MOBILE = '?0';
const RESULT_MARKER = '__CVS_XVFB_RESULT__=';
const CVS_PRODUCT_IMAGE_PATTERN =
    /(?:(?:https?:)?\/\/www\.cvs\.com)?(\/bizcontent\/merchandising\/productimages\/(?:large|high_res)\/[^"'<>\s]+?\.(?:jpe?g|png|webp)(?:\?[^"'<>\s]*)?)/i;

const targetUrl = String(process.argv[2] || process.env.CVS_TEST_URL || DEFAULT_URL).trim();
const targetZip = String(process.argv[3] || process.env.CVS_TEST_ZIP || DEFAULT_ZIP).trim();
const targetRangeMiles = (() => {
    const rawValue = String(
        process.argv[4] || process.env.CVS_TEST_RANGE_MILES || process.env.CVS_XVFB_RANGE_MILES || '',
    ).trim();
    if (!rawValue) return null;
    const parsed = Number.parseFloat(rawValue);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
})();
const headless = !['0', 'false', 'no', 'off'].includes(
    String(process.env.CVS_XVFB_HEADLESS ?? DEFAULT_HEADLESS).toLowerCase(),
);
const timeoutMs = Math.max(
    5000,
    Number.parseInt(String(process.env.CVS_XVFB_TIMEOUT_MS || DEFAULT_TIMEOUT_MS), 10) || DEFAULT_TIMEOUT_MS,
);
const inventoryWaitMs = Math.max(
    5000,
    Number.parseInt(
        String(process.env.CVS_XVFB_INVENTORY_WAIT_MS || DEFAULT_INVENTORY_WAIT_MS),
        10,
    ) || DEFAULT_INVENTORY_WAIT_MS,
);
const apiKey = String(process.env.CVS_XVFB_API_KEY || process.env.CVS_API_KEY || DEFAULT_API_KEY).trim();

fs.mkdirSync(OUTPUT_DIR, { recursive: true });

function randomInt(min, max) {
    const safeMin = Math.ceil(Number(min) || 0);
    const safeMax = Math.floor(Number(max) || safeMin);
    if (safeMax <= safeMin) return safeMin;
    return Math.floor(Math.random() * (safeMax - safeMin + 1)) + safeMin;
}

function parseCsv(value) {
    return String(value || '')
        .split(/[\r\n,;]+/)
        .map((entry) => entry.trim())
        .filter(Boolean);
}

function detectChallenge(text) {
    const normalized = String(text || '').toLowerCase();
    if (normalized.includes('_incapsula_resource') || normalized.includes('incapsula')) return 'incapsula';
    if (normalized.includes('captcha')) return 'captcha';
    if (normalized.includes('access denied')) return 'access_denied';
    return '';
}

async function buildChallengeResult(page, proxyConfig, challengeType, extractedImageUrl, buttonClicked) {
    const screenshotPath = path.join(OUTPUT_DIR, `debug_challenge_${Date.now()}.png`);
    await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
    return {
        ok: false,
        proxy: proxyLabel(proxyConfig),
        buttonClicked,
        image_url: extractedImageUrl,
        challengeDetected: true,
        challengeType: String(challengeType || '').trim(),
        error: `CVS challenge page detected: ${challengeType}; screenshot=${screenshotPath}`,
        screenshotPath,
    };
}

function normalizeCvsImageUrl(rawValue) {
    const value = String(rawValue || '').trim();
    if (!value) return '';
    try {
        if (value.startsWith('//')) {
            return `https:${value}`;
        }
        if (value.startsWith('/')) {
            return new URL(value, 'https://www.cvs.com').toString();
        }
        const parsed = new URL(value, 'https://www.cvs.com');
        if (
            ['localhost', '127.0.0.1', '0.0.0.0'].includes(String(parsed.hostname || '').toLowerCase()) &&
            String(parsed.pathname || '').toLowerCase().startsWith('/bizcontent/merchandising/productimages/')
        ) {
            return new URL(parsed.pathname, 'https://www.cvs.com').toString();
        }
        return parsed.toString();
    } catch {
        return '';
    }
}

function firstSrcsetCandidate(rawValue) {
    return String(rawValue || '')
        .split(',', 1)[0]
        .trim()
        .split(/\s+/, 1)[0]
        .trim();
}

async function extractProductImageUrl(page, html = '') {
    const domCandidate = await page.evaluate(() => {
        const normalizeValue = (value) => String(value || '').trim();
        const firstSrcset = (value) =>
            String(value || '')
                .split(',', 1)[0]
                .trim()
                .split(/\s+/, 1)[0]
                .trim();

        const collect = [];

        const ogImage = document.querySelector('meta[property="og:image"]')?.getAttribute('content');
        if (ogImage) collect.push(ogImage);

        document
            .querySelectorAll('link[rel="preload"][as="image"]')
            .forEach((element) => {
                collect.push(element.getAttribute('href'));
                collect.push(firstSrcset(element.getAttribute('imagesrcset') || element.getAttribute('imageSrcSet')));
            });

        document.querySelectorAll('img').forEach((element) => {
            [
                'src',
                'data-src',
                'data-lazy-src',
                'data-zoom-src',
                'data-image',
                'data-srcset',
            ].forEach((attribute) => {
                const value = element.getAttribute(attribute);
                collect.push(attribute.includes('srcset') ? firstSrcset(value) : normalizeValue(value));
            });
        });

        for (const candidate of collect) {
            const normalized = normalizeValue(candidate);
            if (normalized && normalized.toLowerCase().includes('/bizcontent/merchandising/productimages/')) {
                return normalized;
            }
        }

        return '';
    }).catch(() => '');

    const normalizedDomCandidate = normalizeCvsImageUrl(domCandidate);
    if (normalizedDomCandidate) {
        return normalizedDomCandidate;
    }

    const normalizedHtml = String(html || '').replace(/\\\//g, '/');
    const htmlMatch = normalizedHtml.match(CVS_PRODUCT_IMAGE_PATTERN);
    if (!htmlMatch) return '';

    return normalizeCvsImageUrl(htmlMatch[1] || htmlMatch[0]);
}

function normalizeDistance(value) {
    const parsed = Number.parseFloat(String(value ?? '').trim());
    return Number.isFinite(parsed) ? parsed : null;
}

function inventoryLocations(payload) {
    if (Array.isArray(payload?.atgResponse)) return payload.atgResponse;
    if (Array.isArray(payload?.response?.atgResponse)) return payload.response.atgResponse;
    return [];
}

function filterInventoryPayloadByRange(payload) {
    if (!payload || typeof payload !== 'object' || !targetRangeMiles) return payload;

    const locations = inventoryLocations(payload);
    if (!locations.length) return payload;

    const filteredLocations = locations.filter((store) => {
        const distance = normalizeDistance(store?.dt);
        return distance !== null && distance <= targetRangeMiles;
    });

    console.log(
        `[response] applying range=${targetRangeMiles} miles kept ${filteredLocations.length}/${locations.length} stores`,
    );

    if (Array.isArray(payload.atgResponse)) {
        return { ...payload, atgResponse: filteredLocations };
    }
    if (Array.isArray(payload?.response?.atgResponse)) {
        return {
            ...payload,
            response: {
                ...payload.response,
                atgResponse: filteredLocations,
            },
        };
    }
    return payload;
}

function proxyLabel(proxyConfig) {
    if (!proxyConfig?.server) return 'direct';
    return String(proxyConfig.server).replace(/\/\/[^@]+@/, '//***:***@');
}

function parseProxyEntry(rawValue) {
    const value = String(rawValue || '').trim().replace(/^['"]|['"]$/g, '');
    if (!value) return null;

    if (!value.includes('://')) {
        const parts = value.split(':');
        if (parts.length >= 4) {
            const [host, port, username, ...passwordParts] = parts;
            const password = passwordParts.join(':');
            if (host && port && username) {
                return {
                    server: `http://${host.trim()}:${port.trim()}`,
                    username: username.trim(),
                    password: password.trim(),
                };
            }
        }
    }

    try {
        const normalized = /^[a-z]+:\/\//i.test(value) ? value : `http://${value}`;
        const parsed = new URL(normalized);
        const result = { server: `${parsed.protocol}//${parsed.host}` };
        if (parsed.username) result.username = decodeURIComponent(parsed.username);
        if (parsed.password) result.password = decodeURIComponent(parsed.password);
        return result;
    } catch {
        return null;
    }
}

function getProxyConfigs() {
    const candidates = [
        ...parseCsv(process.env.CVS_XVFB_PROXY_URLS),
        String(process.env.CVS_XVFB_PROXY_URL || '').trim(),
        ...parseCsv(process.env.CVS_PROXY_URLS),
        String(process.env.CVS_PROXY_URL || '').trim(),
    ].filter(Boolean);

    const parsed = [];
    const seen = new Set();
    for (const candidate of candidates) {
        const proxyConfig = parseProxyEntry(candidate);
        if (!proxyConfig?.server || seen.has(proxyConfig.server)) continue;
        seen.add(proxyConfig.server);
        parsed.push(proxyConfig);
    }

    if (!['0', 'false', 'no', 'off'].includes(String(process.env.CVS_XVFB_INCLUDE_DIRECT || '0').toLowerCase())) {
        parsed.push(null);
    }

    return parsed.length ? parsed : [null];
}

function findLinuxChromiumExecutable() {
    const configured = [
        process.env.CVS_XVFB_BROWSER_EXECUTABLE_PATH,
        process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
        process.env.CVS_PLAYWRIGHT_BROWSER_EXECUTABLE_PATH,
        process.env.CVS_ZENDRIVER_BROWSER_EXECUTABLE_PATH,
        process.env.CHROME_BINARY,
        process.env.CHROMIUM_BINARY,
    ]
        .map((value) => String(value || '').trim())
        .filter(Boolean);

    for (const candidate of configured) {
        if (fs.existsSync(candidate)) return candidate;
    }

    const linuxCandidates = [
        '/usr/bin/google-chrome-stable',
        '/usr/bin/google-chrome',
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        '/snap/bin/chromium',
    ];
    for (const candidate of linuxCandidates) {
        if (fs.existsSync(candidate)) return candidate;
    }

    return '';
}

async function clickPickupTab(page) {
    // CVS fulfillment tabs (Pickup / Same Day Delivery / Shipping) must be selected
    // before the 'Check more stores' link is rendered in the DOM.
    const pickupSelectors = [
        'button:has-text("Pickup")',
        '[role="tab"]:has-text("Pickup")',
        '[role="button"]:has-text("Pickup")',
        'a:has-text("Pickup")',
        'label:has-text("Pickup")',
    ];

    for (const selector of pickupSelectors) {
        try {
            const locator = page.locator(selector).first();
            if (!(await locator.isVisible({ timeout: 1500 }).catch(() => false))) continue;
            await locator.scrollIntoViewIfNeeded().catch(() => {});
            await page.waitForTimeout(randomInt(100, 220));
            await locator.click({ timeout: 4000 });
            await page.waitForTimeout(randomInt(600, 1200));
            console.log(`[pickup] clicked pickup tab via selector: ${selector}`);
            return true;
        } catch {
            continue;
        }
    }

    // Fallback: evaluate-based text match for dynamically rendered tabs
    const clicked = await page.evaluate(() => {
        const matchesPickup = (text) => {
            const t = String(text || '').trim().toLowerCase();
            return t === 'pickup' || t === 'in-store pickup' || t === 'store pickup' || t === 'pick up';
        };
        for (const selector of ['button', '[role="tab"]', '[role="button"]', 'label', 'a', 'span', 'div']) {
            for (const el of document.querySelectorAll(selector)) {
                if (matchesPickup(el.textContent)) {
                    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    el.click();
                    return true;
                }
            }
        }
        return false;
    });

    if (clicked) {
        await page.waitForTimeout(randomInt(600, 1200));
        console.log('[pickup] clicked pickup tab via evaluate fallback');
        return true;
    }

    console.log('[pickup] pickup tab not found; continuing without click');
    return false;
}

async function clickCheckMoreStores(page) {
    const selectors = [
        'button:has-text("Check more stores")',
        'a:has-text("Check more stores")',
        '[role="button"]:has-text("Check more stores")',
        'text=Check more stores',
        'button:has-text("Change store")',
        'a:has-text("Change store")',
        '[role="button"]:has-text("Change store")',
    ];

    for (const selector of selectors) {
        try {
            const locator = page.locator(selector).first();
            if (!(await locator.isVisible({ timeout: 1500 }).catch(() => false))) continue;
            await locator.scrollIntoViewIfNeeded().catch(() => {});
            await page.waitForTimeout(randomInt(140, 320));
            await locator.hover({ timeout: 2000 }).catch(() => {});
            await page.waitForTimeout(randomInt(120, 260));
            await locator.click({ timeout: 5000 });
            await page.waitForTimeout(randomInt(450, 900));
            return true;
        } catch {
            continue;
        }
    }

    const clicked = await page.evaluate(() => {
        const matchesTrigger = (value) => {
            const text = String(value || '').trim().toLowerCase();
            if (!text) return false;
            return (
                text === 'check more stores' ||
                text.includes('check more store') ||
                text === 'change store' ||
                /^get it at \d+ nearby stores?$/.test(text) ||
                text.includes('nearby stores')
            );
        };

        for (const selector of ['a', 'button', 'span', '[role="button"]', 'div']) {
            for (const element of document.querySelectorAll(selector)) {
                if (matchesTrigger(element.textContent)) {
                    element.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    element.click();
                    return true;
                }
            }
        }
        return false;
    });
    return Boolean(clicked);
}

async function waitForDialogInput(page, timeoutMs = 8000) {
    const selector = '[role="dialog"] input[type="text"]';
    const locator = page.locator(selector).first();
    try {
        await locator.waitFor({ state: 'visible', timeout: timeoutMs });
        return true;
    } catch {
        return false;
    }
}

async function submitDialogSearch(page) {
    return page.evaluate(() => {
        const dialog = document.querySelector('[role="dialog"]');
        if (!dialog) return null;
        for (const element of dialog.querySelectorAll('button, [role="button"]')) {
            const text = element.textContent?.trim().toLowerCase();
            if (
                text?.includes('search') ||
                text?.includes('find') ||
                text?.includes('show') ||
                text?.includes('update') ||
                text?.includes('go')
            ) {
                element.click();
                return text;
            }
        }
        for (const element of dialog.querySelectorAll('button')) {
            const text = element.textContent?.trim().toLowerCase();
            if (text && !text.includes('close')) {
                element.click();
                return `fallback: ${text}`;
            }
        }
        return null;
    });
}

async function humanizePage(page) {
    try {
        const viewport = page.viewportSize() || { width: 1920, height: 1080 };
        await page.mouse.move(randomInt(160, 380), randomInt(120, 260), { steps: randomInt(10, 22) });
        await page.waitForTimeout(randomInt(160, 320));
        await page.mouse.wheel(0, randomInt(260, 520));
        await page.waitForTimeout(randomInt(180, 340));
        await page.mouse.move(
            randomInt(Math.floor(viewport.width / 3), Math.max(Math.floor(viewport.width / 3), viewport.width - 180)),
            randomInt(160, Math.max(220, viewport.height - 220)),
            { steps: randomInt(10, 24) },
        );
        await page.waitForTimeout(randomInt(120, 260));
        await page.mouse.wheel(0, -randomInt(120, 260));
        await page.waitForTimeout(randomInt(180, 320));
    } catch {}
}

async function runAttempt(proxyConfig) {
    const executablePath = findLinuxChromiumExecutable();
    const launchOptions = {
        headless,
        ...(proxyConfig ? { proxy: proxyConfig } : {}),
        args: [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--disable-gpu',
            '--window-size=1920,1080',
        ],
    };
    if (executablePath) {
        launchOptions.executablePath = executablePath;
    }

    console.log(`\n[proxy] Trying proxy: ${proxyLabel(proxyConfig)}`);
    if (executablePath) {
        console.log(`[browser] executable: ${executablePath}`);
    }

    const browser = await chromium.launch(launchOptions);
    const context = await browser.newContext({
        viewport: { width: 1920, height: 1080 },
        userAgent: WINDOWS_USER_AGENT,
        locale: 'en-US',
        timezoneId: process.env.CVS_XVFB_TIMEZONE || 'America/New_York',
        permissions: ['geolocation'],
        extraHTTPHeaders: {
            'Accept-Language': 'en-US,en;q=0.9',
            'Sec-CH-UA': WINDOWS_SEC_CH_UA,
            'Sec-CH-UA-Mobile': WINDOWS_SEC_CH_UA_MOBILE,
            'Sec-CH-UA-Platform': WINDOWS_SEC_CH_UA_PLATFORM,
        },
    });

    await context.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', {
            get: () => {
                const arr = [
                    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                    { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                    { name: 'Native Client', filename: 'internal-nacl-plugin' },
                ];
                arr.__proto__ = PluginArray.prototype;
                return arr;
            },
        });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
        Object.defineProperty(navigator, 'userAgent', {
            get: () =>
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        });
        if ('userAgentData' in navigator) {
            Object.defineProperty(navigator, 'userAgentData', {
                get: () => ({
                    brands: [
                        { brand: 'Chromium', version: '134' },
                        { brand: 'Not:A-Brand', version: '24' },
                        { brand: 'Google Chrome', version: '134' },
                    ],
                    mobile: false,
                    platform: 'Windows',
                    getHighEntropyValues: async (hints) => {
                        const values = {
                            architecture: 'x86',
                            bitness: '64',
                            mobile: false,
                            model: '',
                            platform: 'Windows',
                            platformVersion: '10.0.0',
                            uaFullVersion: '134.0.0.0',
                            wow64: false,
                        };
                        if (!Array.isArray(hints)) return values;
                        return hints.reduce((accumulator, hint) => {
                            if (hint in values) accumulator[hint] = values[hint];
                            return accumulator;
                        }, {});
                    },
                }),
            });
        }
        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
        Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
        window.chrome = {
            runtime: { connect: () => {}, sendMessage: () => {}, onMessage: { addListener: () => {} } },
            loadTimes: function () {},
            csi: function () {},
            app: {},
        };
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (p) =>
            p.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(p);
        Object.defineProperty(screen, 'width', { get: () => 1920 });
        Object.defineProperty(screen, 'height', { get: () => 1080 });
        Object.defineProperty(screen, 'availWidth', { get: () => 1920 });
        Object.defineProperty(screen, 'availHeight', { get: () => 1040 });
        Object.defineProperty(screen, 'colorDepth', { get: () => 24 });
        Object.defineProperty(screen, 'pixelDepth', { get: () => 24 });
        Object.defineProperty(window, 'outerWidth', { get: () => 1920 });
        Object.defineProperty(window, 'outerHeight', { get: () => 1080 });
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function (parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel Iris OpenGL Engine';
            return getParameter.call(this, parameter);
        };
    });

    const page = await context.newPage();
    let responseCount = 0;
    let capturedInventory = null;
    let extractedImageUrl = '';
    let buttonClicked = false;
    let pageChallenge = '';
    let inventoryRequestCount = 0;

    let resolveInventoryRequestSeen = () => {};
    const inventoryRequestSeen = new Promise((resolve) => {
        resolveInventoryRequestSeen = resolve;
    });

    const inventoryPromise = new Promise((resolve) => {
        page.on('response', async (res) => {
            if (!res.url().includes('getStoreDetailsAndInventory')) return;
            responseCount += 1;
            console.log(`\n[response] #${responseCount} status=${res.status()}`);
            if (res.status() !== 200) return;
            const data = await res.json().catch(() => null);
            if (data) {
                const filteredData = filterInventoryPayloadByRange(data);
                console.log('[response] inventory payload captured');
                console.dir(filteredData, { depth: 6 });
                capturedInventory = filteredData;
                resolve(filteredData);
            }
        });
    });

    page.on('request', (req) => {
        if (!req.url().includes('getStoreDetailsAndInventory')) return;
        inventoryRequestCount += 1;
        resolveInventoryRequestSeen();
        console.log('\n[request] outgoing inventory request');
        console.log(`[request] url=${req.url()}`);
        if (req.postData()) {
            try {
                console.log(JSON.stringify(JSON.parse(req.postData()), null, 2));
            } catch {
                console.log(req.postData());
            }
        }
    });

    await page.route('**/*getStoreDetailsAndInventory*', async (route) => {
        const request = route.request();
        const originalBody = request.postData();
        if (originalBody) {
            try {
                const body = JSON.parse(originalBody);
                const req = body.getStoreDetailsAndInventoryRequest;
                if (req) {
                    req.addressLine = targetZip;
                    req.geolatitude = '';
                    req.geolongitude = '';
                    if (apiKey && req.header && typeof req.header === 'object') {
                        req.header.apiKey = req.header.apiKey || apiKey;
                    }
                }
                console.log(`[route] rewrote addressLine -> ${targetZip}`);
                await route.continue({ postData: JSON.stringify(body) });
                return;
            } catch (error) {
                console.log(`[route] failed to rewrite payload: ${error.message}`);
            }
        }
        await route.continue();
    });

    try {
        await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
        await page.waitForTimeout(5000);
        console.log('[page] loaded');

        const html = await page.content();
        pageChallenge = detectChallenge(html);
        if (pageChallenge) {
            console.log(`[page] challenge marker detected: ${pageChallenge}`);
            return await buildChallengeResult(page, proxyConfig, pageChallenge, extractedImageUrl, buttonClicked);
        }
        extractedImageUrl = await extractProductImageUrl(page, html);
        if (extractedImageUrl) {
            console.log(`[page] product image=${extractedImageUrl}`);
        }

        await humanizePage(page);
        await page.evaluate(() => window.scrollTo(0, 600));
        await page.waitForTimeout(randomInt(1200, 2200));

        // CVS requires the Pickup fulfillment tab to be selected first before
        // the 'Check more stores' trigger link becomes visible in the DOM.
        await clickPickupTab(page);
        await page.evaluate(() => window.scrollTo(0, 600));
        await page.waitForTimeout(randomInt(600, 1000));

        let dialogVisible = await waitForDialogInput(page, 1200);
        if (dialogVisible) {
            console.log('[dialog] inventory modal already open');
        } else {
            buttonClicked = await clickCheckMoreStores(page);
            if (buttonClicked) {
                console.log('[page] clicked store availability trigger');
                await page.waitForTimeout(randomInt(350, 900));
                dialogVisible = await waitForDialogInput(page, 8000);
                if (!dialogVisible) {
                    await Promise.race([
                        waitForDialogInput(page, 6000).then((visible) => {
                            dialogVisible = visible;
                        }),
                        inventoryRequestSeen,
                        page.waitForTimeout(6000),
                    ]);
                    dialogVisible = dialogVisible || (await waitForDialogInput(page, 800).catch(() => false));
                }
            } else {
                await Promise.race([
                    waitForDialogInput(page, 2500).then((visible) => {
                        dialogVisible = visible;
                    }),
                    inventoryRequestSeen,
                    page.waitForTimeout(2500),
                ]);
                dialogVisible = dialogVisible || (await waitForDialogInput(page, 800).catch(() => false));
            }
        }

        if (dialogVisible) {
            const inputSel = '[role="dialog"] input[type="text"]';
            const inputId = await page.$eval(inputSel, (el) => el.id || '');
            console.log(`[dialog] input id="${inputId}"`);

            await page.click(inputSel);
            await page.waitForTimeout(randomInt(120, 260));
            await page.keyboard.press('Control+A');
            await page.waitForTimeout(randomInt(60, 140));
            await page.keyboard.press('Backspace');
            await page.waitForTimeout(randomInt(80, 180));
            await page.type(inputSel, targetZip, { delay: 80 });
            console.log(`[dialog] typed zip=${targetZip}`);

            await page.waitForTimeout(randomInt(350, 700));

            const searchClicked = await submitDialogSearch(page);
            if (searchClicked) {
                console.log(`[dialog] clicked button="${searchClicked}"`);
            } else {
                console.log('[dialog] no button found, pressing Enter');
                await page.keyboard.press('Enter');
            }
        } else {
            const refreshedChallenge = detectChallenge(await page.content().catch(() => ''));
            if (refreshedChallenge) {
                pageChallenge = refreshedChallenge;
                return await buildChallengeResult(page, proxyConfig, pageChallenge, extractedImageUrl, buttonClicked);
            }
            if (!buttonClicked && inventoryRequestCount === 0) {
                const screenshotPath = path.join(OUTPUT_DIR, `debug_nobutton_${Date.now()}.png`);
                await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
                const reason = pageChallenge ? `; page challenge detected: ${pageChallenge}` : '';
                throw new Error(`'Check more stores' button not found; screenshot=${screenshotPath}${reason}`);
            }
            console.log('[dialog] modal not visible; waiting for rewritten inventory request already in flight');
        }

        console.log('[wait] waiting for inventory response');
        await Promise.race([
            inventoryPromise,
            page.waitForTimeout(inventoryWaitMs).then(() => {
                throw new Error(
                    `timed out waiting for inventory response after ${inventoryWaitMs}ms (requests=${inventoryRequestCount}, responses=${responseCount})`,
                );
            }),
        ]);

        return {
            ok: true,
            proxy: proxyLabel(proxyConfig),
            buttonClicked,
            inventoryCaptured: Boolean(capturedInventory),
            image_url: extractedImageUrl,
            payload: capturedInventory,
        };
    } catch (error) {
        const screenshotPath = path.join(OUTPUT_DIR, `debug_error_${Date.now()}.png`);
        await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
        return {
            ok: false,
            proxy: proxyLabel(proxyConfig),
            buttonClicked,
            image_url: extractedImageUrl,
            challengeDetected: Boolean(pageChallenge),
            challengeType: pageChallenge || '',
            error: pageChallenge && !String(error?.message || '').includes('page challenge detected:')
                ? `${error.message}; page challenge detected: ${pageChallenge}`
                : error.message,
            screenshotPath,
        };
    } finally {
        await context.close().catch(() => {});
        await browser.close().catch(() => {});
    }
}

async function run() {
    const proxyConfigs = getProxyConfigs();
    console.log(`[config] url=${targetUrl}`);
    console.log(`[config] zip=${targetZip}`);
    console.log(`[config] rangeMiles=${targetRangeMiles ?? 'none'}`);
    console.log(`[config] headless=${headless}`);
    console.log(`[config] proxyCount=${proxyConfigs.length}`);
    console.log(`[config] platform=${os.platform()}`);

    const results = [];
    for (const proxyConfig of proxyConfigs) {
        const result = await runAttempt(proxyConfig);
        results.push(result);
        if (result.ok) {
            console.log('\n[success] inventory flow succeeded');
            console.log(JSON.stringify(result, null, 2));
            console.log(`${RESULT_MARKER}${JSON.stringify(result)}`);
            return;
        }
        console.log('\n[failure] attempt failed');
        console.log(JSON.stringify(result, null, 2));
    }

    console.error('\n[error] all proxy attempts failed');
    console.error(JSON.stringify(results, null, 2));
    console.error(`${RESULT_MARKER}${JSON.stringify({ ok: false, attempts: results })}`);
    process.exitCode = 1;
}

run().catch((error) => {
    console.error('[fatal]', error);
    console.error(`${RESULT_MARKER}${JSON.stringify({ ok: false, fatal: String(error?.message || error || 'unknown error') })}`);
    process.exitCode = 1;
});
