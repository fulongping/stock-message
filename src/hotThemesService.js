const fs = require('fs/promises');
const path = require('path');

const SOURCE_URL = 'https://www.10jqka.com.cn/';
const FETCH_INTERVAL_MS = 10 * 60 * 1000;
const FETCH_TIMEOUT_MS = 20 * 1000;
const EMPTY_STATE_RETRY_MS = 60 * 1000;
const CACHE_FILE = path.join(__dirname, '..', 'data', 'hot-themes-cache.json');

const REQUEST_HEADERS = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'Accept-Language': 'zh-CN,zh;q=0.9',
  'Cache-Control': 'no-cache',
  'Pragma': 'no-cache',
  'Referer': SOURCE_URL,
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
};

const state = {
  error: null,
  intervalMs: FETCH_INTERVAL_MS,
  lastAttemptAt: null,
  lastSuccessAt: null,
  nextRefreshAt: null,
  sourceUrl: SOURCE_URL,
  status: 'idle',
  themes: [],
};

let pollingStarted = false;
let inFlightRefresh = null;

function cloneState() {
  return {
    error: state.error,
    intervalMs: state.intervalMs,
    lastAttemptAt: state.lastAttemptAt,
    lastSuccessAt: state.lastSuccessAt,
    nextRefreshAt: state.nextRefreshAt,
    sourceUrl: state.sourceUrl,
    status: state.status,
    themes: [...state.themes],
  };
}

function decodeHtmlEntities(value) {
  return value
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function extractHotThemes(html, maxCount = 3) {
  const compactHtml = html.replace(/\r?\n/g, '');
  const sectionStart = compactHtml.indexOf('主题投资');

  if (sectionStart === -1) {
    throw new Error('未找到“主题投资”区块');
  }

  const sectionScope = compactHtml.slice(sectionStart, sectionStart + 12000);
  const headlineIndex = sectionScope.indexOf('头条</span>');
  const candidateScope = headlineIndex === -1
    ? sectionScope
    : sectionScope.slice(headlineIndex, headlineIndex + 6000);

  const topicPattern = /aria-controls="[^"]*content-TZ-[^"]*"[\s\S]*?<span[^>]*>([^<]+)<\/span>/g;
  const themes = [];
  let match;

  while ((match = topicPattern.exec(candidateScope)) !== null) {
    const themeName = decodeHtmlEntities(match[1]).trim();

    if (!themeName || themeName === '头条' || themes.includes(themeName)) {
      continue;
    }

    themes.push(themeName);

    if (themes.length === maxCount) {
      break;
    }
  }

  if (themes.length < 3) {
    throw new Error('未能从首页解析出 3 个热门主题');
  }

  return themes.map((name, index) => ({
    id: index + 1,
    name,
  }));
}

async function loadCache() {
  try {
    const rawCache = await fs.readFile(CACHE_FILE, 'utf8');
    const cachedState = JSON.parse(rawCache);

    if (!Array.isArray(cachedState.themes) || cachedState.themes.length === 0) {
      return;
    }

    state.error = cachedState.error || null;
    state.lastAttemptAt = cachedState.lastAttemptAt || null;
    state.lastSuccessAt = cachedState.lastSuccessAt || null;
    state.nextRefreshAt = cachedState.nextRefreshAt || null;
    state.status = cachedState.status || 'stale';
    state.themes = cachedState.themes;
  } catch (error) {
    if (error.code !== 'ENOENT') {
      console.warn('Failed to load cache:', error.message);
    }
  }
}

async function saveCache() {
  await fs.mkdir(path.dirname(CACHE_FILE), { recursive: true });
  await fs.writeFile(CACHE_FILE, JSON.stringify(cloneState(), null, 2), 'utf8');
}

async function fetchHomepageHtml() {
  const response = await fetch(SOURCE_URL, {
    headers: REQUEST_HEADERS,
    redirect: 'follow',
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });

  const html = await response.text();

  if (!response.ok) {
    throw new Error(`同花顺首页请求失败，HTTP ${response.status}`);
  }

  if (/Nginx forbidden/i.test(html)) {
    throw new Error('同花顺返回了访问拦截页面');
  }

  return html;
}

async function refreshThemes(options = {}) {
  const { force = false } = options;

  if (inFlightRefresh) {
    return inFlightRefresh;
  }

  const now = Date.now();
  const lastSuccessTime = state.lastSuccessAt ? Date.parse(state.lastSuccessAt) : 0;
  const isFresh = state.themes.length > 0 && now - lastSuccessTime < FETCH_INTERVAL_MS;

  if (!force && isFresh) {
    return cloneState();
  }

  inFlightRefresh = (async () => {
    state.error = null;
    state.lastAttemptAt = new Date().toISOString();
    state.status = state.themes.length > 0 ? 'refreshing' : 'loading';

    try {
      const html = await fetchHomepageHtml();
      const themes = extractHotThemes(html);
      const timestamp = new Date().toISOString();

      state.error = null;
      state.lastSuccessAt = timestamp;
      state.nextRefreshAt = new Date(Date.now() + FETCH_INTERVAL_MS).toISOString();
      state.status = 'ready';
      state.themes = themes;

      await saveCache();
      return cloneState();
    } catch (error) {
      state.error = error.message;
      state.nextRefreshAt = new Date(Date.now() + FETCH_INTERVAL_MS).toISOString();
      state.status = state.themes.length > 0 ? 'stale' : 'error';

      await saveCache();
      return cloneState();
    } finally {
      inFlightRefresh = null;
    }
  })();

  return inFlightRefresh;
}

async function getSnapshot(options = {}) {
  const { hydrateIfEmpty = false } = options;

  if (hydrateIfEmpty && state.themes.length === 0) {
    const lastAttemptTime = state.lastAttemptAt ? Date.parse(state.lastAttemptAt) : 0;
    const canRetry = Date.now() - lastAttemptTime > EMPTY_STATE_RETRY_MS;

    if (!inFlightRefresh && canRetry) {
      await refreshThemes({ force: true });
    }
  }

  return cloneState();
}

function startPolling() {
  if (pollingStarted) {
    return;
  }

  pollingStarted = true;

  loadCache()
    .then(() => refreshThemes({ force: true }))
    .catch((error) => {
      console.warn('Initial refresh failed:', error.message);
    });

  setInterval(() => {
    refreshThemes({ force: true }).catch((error) => {
      console.warn('Scheduled refresh failed:', error.message);
    });
  }, FETCH_INTERVAL_MS);
}

module.exports = {
  extractHotThemes,
  fetchHomepageHtml,
  getSnapshot,
  refreshThemes,
  startPolling,
};

