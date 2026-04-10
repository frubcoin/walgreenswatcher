import { PlaywrightCrawler } from 'crawlee';
import * as playwright from 'playwright';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const targetUrl =
    process.argv[2] || 'https://www.cvs.com/shop/uno-flip-card-game-prodid-318928';
const zipCode = process.argv[3] || '85208';
const maxAttempts = Math.max(1, Number.parseInt(String(process.env.CVS_TEST_MAX_ATTEMPTS || ''), 10) || 3);
const includeDirectAttempt = !['0', 'false', 'no', 'off'].includes(
    String(process.env.CVS_TEST_USE_DIRECT || '1').toLowerCase(),
);
const camoufoxRequested = ['1', 'true', 'yes', 'on'].includes(String(process.env.CAMOUFOX_ENABLED || '1').toLowerCase());
const camoufoxEnvPath = String(process.env.CAMOUFOX_EXECUTABLE_PATH || '').trim();

function findPlaywrightFirefoxExecutable() {
    const localAppData = process.env.LOCALAPPDATA || path.join(os.homedir(), 'AppData', 'Local');
    const msPlaywrightDir = path.join(localAppData, 'ms-playwright');
    if (!fs.existsSync(msPlaywrightDir)) return '';

    try {
        const entries = fs
            .readdirSync(msPlaywrightDir, { withFileTypes: true })
            .filter((entry) => entry.isDirectory() && entry.name.startsWith('firefox-'))
            .map((entry) => entry.name)
            .sort((a, b) => b.localeCompare(a));
        for (const dirName of entries) {
            const executable = path.join(msPlaywrightDir, dirName, 'firefox', 'firefox.exe');
            if (fs.existsSync(executable)) return executable;
        }
    } catch {
        return '';
    }
    return '';
}

const camoufoxExecutablePath = camoufoxEnvPath || findPlaywrightFirefoxExecutable();
const useCamoufox = camoufoxRequested && Boolean(camoufoxExecutablePath) && fs.existsSync(camoufoxExecutablePath);

if (camoufoxRequested && !useCamoufox) {
    console.warn(
        [
            'CAMOUFOX_ENABLED is on, but no Firefox/Camoufox executable was found.',
            `CAMOUFOX_EXECUTABLE_PATH=${camoufoxEnvPath || '(not set)'}`,
            'Falling back to Playwright Chromium.',
            'Set CAMOUFOX_EXECUTABLE_PATH to your camoufox/firefox executable, or install Firefox for Playwright via `npx playwright install firefox`.',
        ].join(' '),
    );
}

const productIdMatch = targetUrl.match(/prodid-(\d+)/i);
const productId = productIdMatch ? productIdMatch[1] : '318928';
const inventoryUrl = 'https://www.cvs.com/RETAGPV3/Inventory/V1/getStoreDetailsAndInventory';
const apiKeyFallback = 'a2ff75c6-2da7-4299-929d-d670d827ab4a';

function detectChallenge(text) {
    const normalized = String(text || '').toLowerCase();
    if (normalized.includes('_incapsula_resource') || normalized.includes('incapsula')) return 'incapsula';
    if (normalized.includes('captcha')) return 'captcha';
    if (normalized.includes('access denied')) return 'access_denied';
    return '';
}

function shuffled(list) {
    const copy = [...list];
    for (let i = copy.length - 1; i > 0; i -= 1) {
        const j = Math.floor(Math.random() * (i + 1));
        [copy[i], copy[j]] = [copy[j], copy[i]];
    }
    return copy;
}

function proxyLabel(proxyUrl) {
    return proxyUrl ? proxyUrl.replace(/\/\/[^@]+@/, '//***:***@') : 'direct';
}

function parseCsv(value) {
    return String(value || '')
        .split(/[\r\n,;]+/)
        .map((s) => s.trim())
        .filter(Boolean);
}

function parseProxyEntry(rawValue) {
    const value = String(rawValue || '').trim().replace(/^['"]|['"]$/g, '');
    if (!value) return null;
    // Support "host:port:user:pass" provider format.
    const parts = value.split(':');
    if (parts.length >= 4 && !value.includes('://')) {
        const [host, port, username, ...passwordParts] = parts;
        const password = passwordParts.join(':');
        if (host && port && username) {
            const normalizedHost = host.trim().toLowerCase();
            if (!/^[a-z0-9.-]+$/.test(normalizedHost)) return null;
            if (!/^\d+$/.test(String(port).trim())) return null;
            return {
                raw: value,
                server: `http://${normalizedHost}:${String(port).trim()}`,
                username,
                password,
            };
        }
    }
    const normalized = /^[a-z]+:\/\//i.test(value) ? value : `http://${value}`;
    try {
        const parsed = new URL(normalized);
        const hostname = String(parsed.hostname || '').trim().toLowerCase();
        if (!hostname || !/^[a-z0-9.-]+$/.test(hostname)) return null;
        const server = `${parsed.protocol}//${parsed.host}`;
        const config = { raw: value, server };
        if (parsed.username) config.username = decodeURIComponent(parsed.username);
        if (parsed.password) config.password = decodeURIComponent(parsed.password);
        return config;
    } catch {
        return null;
    }
}

function getProxyConfigs() {
    const proxyCandidates = [];
    proxyCandidates.push(...parseCsv(process.env.CVS_PROXY_URLS));
    proxyCandidates.push(String(process.env.CVS_PROXY_URL || '').trim());
    proxyCandidates.push(...parseCsv(process.env.DECODO_PROXY_URLS));
    proxyCandidates.push(String(process.env.DECODO_PROXY_URL || '').trim());
    const deduped = [...new Set(proxyCandidates.filter(Boolean))];
    const parsed = deduped.map(parseProxyEntry).filter(Boolean);
    if (deduped.length && !parsed.length) {
        console.warn('Proxy env variables were provided, but none could be parsed. Check formatting.');
    }
    return parsed;
}

async function runAttempt(proxyConfig, attempt, totalAttempts) {
    if (proxyConfig?.server) {
        console.log(`[proxy] attempt ${attempt}/${totalAttempts} using ${proxyConfig.server}`);
    } else {
        console.log(`[proxy] attempt ${attempt}/${totalAttempts} using direct`);
    }
    const launchContext = {
        launcher: useCamoufox ? playwright.firefox : playwright.chromium,
        launchOptions: {
            headless: true,
            ...(proxyConfig
                ? {
                      proxy: proxyConfig,
                  }
                : {}),
            ...(useCamoufox && camoufoxExecutablePath ? { executablePath: camoufoxExecutablePath } : {}),
        },
    };

    let terminalResult = null;
    const crawler = new PlaywrightCrawler({
        headless: true,
        navigationTimeoutSecs: 25,
        maxRequestRetries: 0,
        // Disable session pool so Crawlee does not auto-throw on 403 before requestHandler runs.
        useSessionPool: false,
        launchContext,
        async requestHandler({ page, request, log, pushData }) {
        const pageStatus = await page.evaluate(() => document.readyState);
        log.info(`Loaded ${request.loadedUrl} (readyState=${pageStatus})`);
        const html = await page.content();
        const pageChallenge = detectChallenge(html);
        if (pageChallenge) {
            const result = {
                pageUrl: request.loadedUrl,
                apiKeyFound: false,
                xsrfFound: false,
                status: 403,
                challengeDetected: true,
                challengeType: pageChallenge,
                bodyPreview: html.slice(0, 500),
            };
            await pushData(result);
            terminalResult = {
                attempt,
                totalAttempts,
                proxyUsed: proxyLabel(proxyConfig?.raw || ''),
                browser: useCamoufox ? 'camoufox/firefox' : 'playwright/chromium',
                camoufoxExecutablePathUsed: useCamoufox && Boolean(camoufoxExecutablePath),
                ...result,
            };
            console.log(
                JSON.stringify(
                    terminalResult,
                    null,
                    2,
                ),
            );
            return;
        }

        const result = await page.evaluate(
            async ({ inventoryUrl, productId, zipCode, apiKeyFallback }) => {
                const html = document.documentElement?.outerHTML || '';
                const patterns = [
                    /"x-api-key"\s*:\s*"([^"]+)"/i,
                    /"apiKey"\s*:\s*"([^"]+)"/i,
                    /['"]x-api-key['"]\s*,\s*['"]([^'"]+)['"]/i,
                ];
                let apiKey = '';
                for (const pattern of patterns) {
                    const m = html.match(pattern);
                    if (m?.[1]) {
                        apiKey = String(m[1]).trim();
                        break;
                    }
                }
                if (!apiKey) apiKey = apiKeyFallback;

                const payload = {
                    getStoreDetailsAndInventoryRequest: {
                        header: {
                            apiKey,
                            channelName: 'WEB',
                            deviceToken: Math.random().toString(16).slice(2, 18),
                            deviceType: 'DESKTOP',
                            responseFormat: 'JSON',
                            securityType: 'apiKey',
                            source: 'CVS_WEB',
                            appName: 'CVS_WEB',
                            lineOfBusiness: 'RETAIL',
                            type: 'rdp',
                        },
                        productId: String(productId),
                        geolatitude: '',
                        geolongitude: '',
                        addressLine: String(zipCode),
                    },
                };

                const cookieMap = Object.fromEntries(
                    document.cookie
                        .split(';')
                        .map((part) => part.trim())
                        .filter(Boolean)
                        .map((part) => {
                            const [k, ...rest] = part.split('=');
                            return [k, rest.join('=')];
                        }),
                );
                const xsrfToken =
                    cookieMap['x-xsrf-token'] || cookieMap['XSRF-TOKEN'] || cookieMap['csrf-token'] || '';

                const headers = {
                    accept: 'application/json',
                    'content-type': 'application/json',
                    'x-requested-with': 'XMLHttpRequest',
                    origin: 'https://www.cvs.com',
                    referer: window.location.href,
                    'x-api-key': apiKey,
                };
                if (xsrfToken) headers['x-xsrf-token'] = xsrfToken;

                const response = await fetch(inventoryUrl, {
                    method: 'POST',
                    credentials: 'include',
                    headers,
                    body: JSON.stringify(payload),
                    referrer: window.location.href,
                });
                const text = await response.text();
                return {
                    pageUrl: window.location.href,
                    apiKeyFound: Boolean(apiKey && apiKey !== apiKeyFallback),
                    xsrfFound: Boolean(xsrfToken),
                    status: response.status,
                    challengeDetected: /_incapsula_resource|incapsula|captcha|access denied/i.test(text || ''),
                    challengeType: /_incapsula_resource|incapsula/i.test(text || '')
                        ? 'incapsula'
                        : /captcha/i.test(text || '')
                          ? 'captcha'
                          : /access denied/i.test(text || '')
                            ? 'access_denied'
                            : '',
                    bodyPreview: text.slice(0, 500),
                };
            },
            { inventoryUrl, productId, zipCode, apiKeyFallback },
        );

        await pushData(result);
        terminalResult = {
            attempt,
            totalAttempts,
            proxyUsed: proxyLabel(proxyConfig?.raw || ''),
            browser: useCamoufox ? 'camoufox/firefox' : 'playwright/chromium',
            camoufoxExecutablePathUsed: useCamoufox && Boolean(camoufoxExecutablePath),
            ...result,
        };
        // Also print immediately for local terminal runs.
        console.log(
            JSON.stringify(
                terminalResult,
                null,
                2,
            ),
        );
    },
    failedRequestHandler({ request, log }, error) {
        log.error(`Request failed for ${request.url}: ${error?.message || error}`);
    },
    });

    await crawler.run([
        {
            url: targetUrl,
            uniqueKey: `${targetUrl}#attempt-${attempt}`,
        },
    ]);
    return terminalResult;
}

const proxyConfigs = shuffled(getProxyConfigs());
const attemptConfigs = includeDirectAttempt ? [null, ...proxyConfigs] : [...proxyConfigs];
const selectedAttempts = attemptConfigs.slice(0, maxAttempts);
if (!selectedAttempts.length) {
    console.warn('No proxies configured and direct mode disabled (CVS_TEST_USE_DIRECT=0).');
    process.exitCode = 1;
}
let finalResult = null;

for (let i = 0; i < selectedAttempts.length; i += 1) {
    const result = await runAttempt(selectedAttempts[i], i + 1, selectedAttempts.length);
    finalResult = result || finalResult;
    if (result && !result.challengeDetected && Number(result.status || 0) < 400) {
        break;
    }
}

if (!finalResult) {
    process.exitCode = 1;
}
