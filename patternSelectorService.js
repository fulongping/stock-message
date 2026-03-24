const fs = require('fs/promises');
const path = require('path');

const { fetchHomepageHtml, extractHotThemes } = require('./src/hotThemesService');
const { fetchMarketSnapshot, getMarketLeadersSnapshot } = require('./marketLeadersService');

const POLL_INTERVAL_MS = 10 * 60 * 1000;
const FETCH_TIMEOUT_MS = 20 * 1000;
const EMPTY_STATE_RETRY_MS = 60 * 1000;
const AFTER_CLOSE_READY_MINUTES = 15 * 60 + 5;
const TOP_HOT_THEME_COUNT = 5;
const MAX_THEME_CANDIDATES = 60;
const MAX_TOTAL_CANDIDATES = 220;
const TOP_PICK_COUNT = 5;
const MIN_BAR_COUNT = 25;
const KLINE_CONCURRENCY = 8;
const CONCEPT_INDEX_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const THEME_MEMBER_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const KLINE_CACHE_TTL_MS = 12 * 60 * 60 * 1000;
const MA_TOLERANCE_RATIO = 0.02;
const CONCEPT_INDEX_URL = 'https://q.10jqka.com.cn/gn/index/';
const CONCEPT_DETAIL_BASE_URL = 'https://q.10jqka.com.cn/gn/detail/code';
const CACHE_FILE = path.join(__dirname, 'data', 'pattern-picks-cache.json');
const SUPPORTED_CODE_PATTERN = /^(60|00)\d{4}$/;
const ST_NAME_PATTERN = /(?:^|\b)\*?ST/i;

const REQUEST_HEADERS = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'Accept-Language': 'zh-CN,zh;q=0.9',
  'Cache-Control': 'no-cache',
  'Pragma': 'no-cache',
  'Referer': CONCEPT_INDEX_URL,
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
};

function createEmptyFilterCounts() {
  return {
    candidateCount: 0,
    evaluatedCount: 0,
    finalPickCount: 0,
    strictMatchCount: 0,
    themeMemberCount: 0,
  };
}

function createEmptyMarketOverview() {
  return {
    available: false,
    lastIntervalEndedAt: null,
    latestLeaders: [],
    latestModeLabel: null,
    savedIntervals: 0,
  };
}

const state = {
  day: null,
  error: null,
  filterCounts: createEmptyFilterCounts(),
  intervalMs: POLL_INTERVAL_MS,
  lastAttemptAt: null,
  lastSuccessAt: null,
  marketOverview: createEmptyMarketOverview(),
  nextRefreshAt: null,
  picks: [],
  sourceUrls: {
    conceptIndex: CONCEPT_INDEX_URL,
    dailyLinePattern: 'https://d.10jqka.com.cn/v6/line/33_{code}/01/all.js',
    hotThemes: 'https://www.10jqka.com.cn/',
  },
  status: 'idle',
  summary: [],
  topThemes: [],
  warnings: [],
};

const internalState = {
  conceptIndex: null,
  klineCache: {},
  themeMembers: {},
};

let pollingStarted = false;
let inFlightRefresh = null;
let cacheLoaded = false;
let cacheLoadPromise = null;

function cloneState() {
  return JSON.parse(JSON.stringify(state));
}

function pad2(value) {
  return String(value).padStart(2, '0');
}

function roundNumber(value, digits = 2) {
  if (!Number.isFinite(value)) {
    return null;
  }

  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function formatTradingDay(date) {
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;
}

function createDateAtMinutes(referenceDate, totalMinutes) {
  const date = new Date(referenceDate);
  date.setHours(Math.floor(totalMinutes / 60), totalMinutes % 60, 0, 0);
  return date;
}

function isWeekend(date) {
  const day = date.getDay();
  return day === 0 || day === 6;
}

function getNextTradingCloseTime(referenceDate = new Date()) {
  const target = createDateAtMinutes(referenceDate, AFTER_CLOSE_READY_MINUTES);

  if (!isWeekend(referenceDate) && referenceDate.getTime() < target.getTime()) {
    return target;
  }

  do {
    target.setDate(target.getDate() + 1);
  } while (isWeekend(target));

  return target;
}

function isAfterCloseReady(referenceDate = new Date()) {
  if (isWeekend(referenceDate)) {
    return false;
  }

  const minutes = referenceDate.getHours() * 60 + referenceDate.getMinutes();
  return minutes >= AFTER_CLOSE_READY_MINUTES;
}

function syncSchedule(referenceDate = new Date()) {
  state.intervalMs = POLL_INTERVAL_MS;
  state.nextRefreshAt = getNextTradingCloseTime(referenceDate).toISOString();
}

function resetDailyState(dayKey) {
  state.day = dayKey;
  state.error = null;
  state.filterCounts = createEmptyFilterCounts();
  state.lastAttemptAt = null;
  state.lastSuccessAt = null;
  state.marketOverview = createEmptyMarketOverview();
  state.picks = [];
  state.status = 'idle';
  state.summary = [];
  state.topThemes = [];
  state.warnings = [];
  internalState.klineCache = {};
}

function ensureTradingDayState(referenceDate = new Date()) {
  const dayKey = formatTradingDay(referenceDate);

  if (state.day !== dayKey) {
    resetDailyState(dayKey);
  }
}

function decodeHtmlEntities(value) {
  return String(value || '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .trim();
}

function normalizeKey(value) {
  return decodeHtmlEntities(value)
    .toLowerCase()
    .replace(/\s+/g, '')
    .replace(/[·•()（）【】\[\]{}'"`“”‘’、,，.。:：!！?？+\-_/\\]/g, '');
}

function stripConceptSuffix(value) {
  return normalizeKey(value).replace(/概念$/, '');
}

function decodeGbkBuffer(buffer) {
  try {
    return new TextDecoder('gbk').decode(buffer);
  } catch (error) {
    return Buffer.from(buffer).toString('utf8');
  }
}

function parseJsonpPayload(rawText) {
  const text = String(rawText || '').trim();
  const start = text.indexOf('(');
  const end = text.lastIndexOf(')');

  if (start === -1 || end === -1 || end <= start + 1) {
    throw new Error('同花顺返回的 JSONP 格式异常');
  }

  return JSON.parse(text.slice(start + 1, end));
}

function getKlineReferer(code) {
  return `https://m.10jqka.com.cn/stockpage/hs_${code}/`;
}

async function fetchText(url, headers = REQUEST_HEADERS) {
  const response = await fetch(url, {
    headers,
    redirect: 'follow',
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(`同花顺请求失败，HTTP ${response.status}`);
  }

  if (!text.trim()) {
    throw new Error('同花顺返回了空响应');
  }

  return text;
}

async function fetchBuffer(url, headers = REQUEST_HEADERS) {
  const response = await fetch(url, {
    headers,
    redirect: 'follow',
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });

  if (!response.ok) {
    throw new Error(`同花顺请求失败，HTTP ${response.status}`);
  }

  return response.arrayBuffer();
}

function parseConceptIndexHtml(html) {
  const items = new Map();
  const pattern = /<a[^>]+href="https?:\/\/q\.10jqka\.com\.cn\/gn\/detail\/code\/(\d+)\/"[^>]*>([^<]+)<\/a>/g;
  let match;

  while ((match = pattern.exec(html)) !== null) {
    const conceptCode = String(match[1] || '').trim();
    const name = decodeHtmlEntities(match[2]);

    if (!conceptCode || !name || items.has(conceptCode)) {
      continue;
    }

    items.set(conceptCode, {
      conceptCode,
      name,
      normalizedName: normalizeKey(name),
      strippedName: stripConceptSuffix(name),
      url: `${CONCEPT_DETAIL_BASE_URL}/${conceptCode}/`,
    });
  }

  return [...items.values()];
}

function parseConceptPageCount(html) {
  const matched = html.match(/<span[^>]*class="page_info"[^>]*>\s*(\d+)\s*\/\s*(\d+)\s*<\/span>/i);
  if (!matched) {
    return 1;
  }

  const totalPages = Number(matched[2]);
  return Number.isFinite(totalPages) && totalPages > 0 ? totalPages : 1;
}

function parseConceptStocks(html) {
  const stocks = new Map();
  const pattern = /<a[^>]+href="https?:\/\/stockpage\.10jqka\.com\.cn\/(\d{6})\/"[^>]*>([^<]+)<\/a>/g;
  let match;

  while ((match = pattern.exec(html)) !== null) {
    const code = String(match[1] || '').trim();
    const name = decodeHtmlEntities(match[2]);

    if (!code || !name || stocks.has(code)) {
      continue;
    }

    stocks.set(code, { code, name });
  }

  return [...stocks.values()];
}

function isEligibleStock(code, name = '') {
  return SUPPORTED_CODE_PATTERN.test(String(code || '').trim()) && !ST_NAME_PATTERN.test(String(name));
}

function matchThemeToConcept(themeName, conceptItems) {
  const normalized = normalizeKey(themeName);
  const stripped = stripConceptSuffix(themeName);

  const exact = conceptItems.find((item) => item.normalizedName === normalized);
  if (exact) {
    return exact;
  }

  const strippedExact = conceptItems.filter((item) => item.strippedName === stripped);
  if (strippedExact.length === 1) {
    return strippedExact[0];
  }

  const partial = conceptItems.filter((item) => item.normalizedName.includes(stripped) || stripped.includes(item.normalizedName));
  if (partial.length === 1) {
    return partial[0];
  }

  if (partial.length > 1) {
    return partial
      .sort((left, right) => Math.abs(left.name.length - themeName.length) - Math.abs(right.name.length - themeName.length))[0];
  }

  return null;
}

function pruneCaches(currentDayKey) {
  const now = Date.now();

  if (!internalState.conceptIndex || now - Date.parse(internalState.conceptIndex.updatedAt || 0) > CONCEPT_INDEX_CACHE_TTL_MS) {
    internalState.conceptIndex = null;
  }

  internalState.themeMembers = Object.fromEntries(
    Object.entries(internalState.themeMembers || {}).filter(([, item]) => item && now - Date.parse(item.updatedAt || 0) <= THEME_MEMBER_CACHE_TTL_MS),
  );

  internalState.klineCache = Object.fromEntries(
    Object.entries(internalState.klineCache || {}).filter(([dayKey, item]) => dayKey === currentDayKey && item && typeof item === 'object'),
  );
}

async function loadCache() {
  try {
    const rawCache = await fs.readFile(CACHE_FILE, 'utf8');
    const cached = JSON.parse(rawCache);

    if (!cached || typeof cached !== 'object') {
      return;
    }

    state.day = cached.day || null;
    state.error = cached.error || null;
    state.filterCounts = cached.filterCounts || createEmptyFilterCounts();
    state.intervalMs = cached.intervalMs || POLL_INTERVAL_MS;
    state.lastAttemptAt = cached.lastAttemptAt || null;
    state.lastSuccessAt = cached.lastSuccessAt || null;
    state.marketOverview = cached.marketOverview || createEmptyMarketOverview();
    state.nextRefreshAt = cached.nextRefreshAt || null;
    state.picks = Array.isArray(cached.picks) ? cached.picks : [];
    state.sourceUrls = cached.sourceUrls || state.sourceUrls;
    state.status = cached.status || 'idle';
    state.summary = Array.isArray(cached.summary) ? cached.summary : [];
    state.topThemes = Array.isArray(cached.topThemes) ? cached.topThemes : [];
    state.warnings = Array.isArray(cached.warnings) ? cached.warnings : [];

    internalState.conceptIndex = cached.conceptIndex || null;
    internalState.themeMembers = cached.themeMembers || {};
  } catch (error) {
    if (error.code !== 'ENOENT') {
      console.warn('Failed to load pattern picks cache:', error.message);
    }
  }
}

async function ensureCacheLoaded() {
  if (cacheLoaded) {
    return;
  }

  if (!cacheLoadPromise) {
    cacheLoadPromise = loadCache()
      .catch((error) => {
        console.warn('Failed to hydrate pattern picks cache:', error.message);
      })
      .finally(() => {
        cacheLoaded = true;
      });
  }

  await cacheLoadPromise;
}

async function saveCache() {
  await fs.mkdir(path.dirname(CACHE_FILE), { recursive: true });
  await fs.writeFile(CACHE_FILE, JSON.stringify({
    ...cloneState(),
    conceptIndex: internalState.conceptIndex,
    themeMembers: internalState.themeMembers,
  }, null, 2), 'utf8');
}

async function getConceptIndex() {
  const cached = internalState.conceptIndex;
  const cachedAt = cached?.updatedAt ? Date.parse(cached.updatedAt) : 0;

  if (cached && Date.now() - cachedAt <= CONCEPT_INDEX_CACHE_TTL_MS) {
    return cached.items;
  }

  const buffer = await fetchBuffer(CONCEPT_INDEX_URL);
  const html = decodeGbkBuffer(buffer);
  const items = parseConceptIndexHtml(html);

  if (items.length === 0) {
    throw new Error('概念索引页没有解析出可用题材');
  }

  internalState.conceptIndex = {
    items,
    updatedAt: new Date().toISOString(),
  };
  return items;
}

async function fetchThemeMembers(concept) {
  const cached = internalState.themeMembers[concept.conceptCode];
  const cachedAt = cached?.updatedAt ? Date.parse(cached.updatedAt) : 0;

  if (cached && Date.now() - cachedAt <= THEME_MEMBER_CACHE_TTL_MS) {
    return cached.stocks;
  }

  const firstPageBuffer = await fetchBuffer(concept.url, {
    ...REQUEST_HEADERS,
    Referer: CONCEPT_INDEX_URL,
  });
  const firstPageHtml = decodeGbkBuffer(firstPageBuffer);
  const totalPages = parseConceptPageCount(firstPageHtml);
  const stocks = new Map(parseConceptStocks(firstPageHtml).map((item) => [item.code, item]));

  for (let page = 2; page <= totalPages; page += 1) {
    const pageUrl = `${concept.url}page/${page}/`;
    const pageBuffer = await fetchBuffer(pageUrl, {
      ...REQUEST_HEADERS,
      Referer: concept.url,
    });
    const pageHtml = decodeGbkBuffer(pageBuffer);

    parseConceptStocks(pageHtml).forEach((item) => {
      if (!stocks.has(item.code)) {
        stocks.set(item.code, item);
      }
    });
  }

  const parsedStocks = [...stocks.values()]
    .filter((item) => isEligibleStock(item.code, item.name));

  internalState.themeMembers[concept.conceptCode] = {
    stocks: parsedStocks,
    updatedAt: new Date().toISOString(),
  };

  return parsedStocks;
}

function buildYearIndex(sortYear) {
  const yearIndex = [];
  let offset = 0;

  sortYear.forEach((item) => {
    if (!Array.isArray(item) || item.length < 2) {
      return;
    }

    yearIndex.push({
      startIndex: offset,
      year: String(item[0]),
    });
    offset += Number(item[1]) || 0;
  });

  return yearIndex;
}

function resolveHistoryDate(dateToken, yearIndex, position, firstDate) {
  if (String(dateToken || '').length === 8) {
    const raw = String(dateToken);
    return `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}`;
  }

  const matchedYear = [...yearIndex]
    .reverse()
    .find((item) => position >= item.startIndex);
  const year = matchedYear?.year || String(firstDate || '').slice(0, 4);
  const token = String(dateToken || '').padStart(4, '0');
  return `${year}-${token.slice(0, 2)}-${token.slice(2, 4)}`;
}

function decodeDailyBars(payload) {
  const volumeParts = String(payload.volumn || '')
    .split(',')
    .filter(Boolean)
    .map((item) => Number(item));
  const priceParts = String(payload.price || '')
    .split(',')
    .filter((item) => item !== '')
    .map((item) => Number(item));
  const dateParts = String(payload.dates || '')
    .split(',')
    .filter(Boolean);
  const priceFactor = Number(payload.priceFactor) || 1;
  const firstPrevClose = Number(payload.issuePrice) || 0;
  const yearIndex = buildYearIndex(Array.isArray(payload.sortYear) ? payload.sortYear : []);
  const bars = [];

  for (let index = 0; index < dateParts.length; index += 1) {
    const low = priceParts[index * 4] / priceFactor;
    const open = low + (priceParts[index * 4 + 1] / priceFactor);
    const high = low + (priceParts[index * 4 + 2] / priceFactor);
    const close = low + (priceParts[index * 4 + 3] / priceFactor);

    bars.push({
      close,
      date: resolveHistoryDate(dateParts[index], yearIndex, index, payload.start),
      high,
      low,
      open,
      prevClose: index === 0 ? firstPrevClose || open : bars[index - 1].close,
      volume: Number.isFinite(volumeParts[index]) ? volumeParts[index] : 0,
    });
  }

  return bars;
}

function addMa5(bars) {
  return bars.map((bar, index) => {
    if (index < 4) {
      return {
        ...bar,
        ma5: null,
      };
    }

    const recent = bars.slice(index - 4, index + 1);
    const total = recent.reduce((sum, item) => sum + item.close, 0);

    return {
      ...bar,
      ma5: total / recent.length,
    };
  });
}

function getDailyChangePercent(lastBar) {
  if (!lastBar || !Number.isFinite(lastBar.prevClose) || lastBar.prevClose === 0) {
    return null;
  }

  return ((lastBar.close - lastBar.prevClose) / lastBar.prevClose) * 100;
}

function getMaxConsecutiveBelowMa5(bars) {
  let streak = 0;
  let maxStreak = 0;

  bars.forEach((bar) => {
    if (!Number.isFinite(bar.ma5)) {
      return;
    }

    if (bar.close < bar.ma5 * (1 - MA_TOLERANCE_RATIO)) {
      streak += 1;
      maxStreak = Math.max(maxStreak, streak);
      return;
    }

    streak = 0;
  });

  return maxStreak;
}

function countNearOrAboveMa5(bars) {
  return bars.filter((bar) => Number.isFinite(bar.ma5) && bar.close >= bar.ma5 * (1 - MA_TOLERANCE_RATIO)).length;
}

function findBottomVolumeRun(bars) {
  const recent = bars.slice(-15);
  let bestRun = null;
  const endCandidates = [recent.length - 1, recent.length - 2, recent.length - 3, recent.length - 4].filter((index) => index >= 2);

  for (const endIndex of endCandidates) {
    const minStartIndex = Math.max(0, endIndex - 7);

    for (let startIndex = minStartIndex; startIndex <= endIndex - 2; startIndex += 1) {
      const run = recent.slice(startIndex, endIndex + 1);
      let risingClose = true;
      let volumeStrictIncreases = 0;
      let volumeTrendOk = true;

      for (let index = 1; index < run.length; index += 1) {
        if (!(run[index].close > run[index - 1].close)) {
          risingClose = false;
          break;
        }

        if (run[index].volume < run[index - 1].volume * 0.9) {
          volumeTrendOk = false;
          break;
        }

        if (run[index].volume > run[index - 1].volume * 1.02) {
          volumeStrictIncreases += 1;
        }
      }

      if (!risingClose || !volumeTrendOk || volumeStrictIncreases < Math.max(2, run.length - 2)) {
        continue;
      }

      const absoluteStartIndex = bars.length - recent.length + startIndex;
      const floorWindow = bars.slice(Math.max(0, absoluteStartIndex - 15), absoluteStartIndex + 1);
      const floorLow = Math.min(...floorWindow.map((item) => item.low));

      if (!(run[0].low <= floorLow * 1.06)) {
        continue;
      }

      const runReturnPercent = ((run[run.length - 1].close - run[0].close) / run[0].close) * 100;
      const volumeExpandPercent = run[0].volume > 0
        ? ((run[run.length - 1].volume - run[0].volume) / run[0].volume) * 100
        : 0;
      const lastBar = bars[bars.length - 1];
      const bottomDistancePercent = floorLow > 0
        ? ((lastBar.close - floorLow) / floorLow) * 100
        : null;
      const candidate = {
        bottomDistancePercent,
        endAt: run[run.length - 1].date,
        endIndex: absoluteStartIndex + run.length - 1,
        runLength: run.length,
        runReturnPercent,
        startAt: run[0].date,
        startIndex: absoluteStartIndex,
        volumeExpandPercent,
      };

      if (!bestRun
        || candidate.runLength > bestRun.runLength
        || (candidate.runLength === bestRun.runLength && candidate.runReturnPercent > bestRun.runReturnPercent)
        || (candidate.runLength === bestRun.runLength && candidate.runReturnPercent === bestRun.runReturnPercent && candidate.endIndex > bestRun.endIndex)) {
        bestRun = candidate;
      }
    }
  }

  return bestRun;
}

function buildPickReasons(metrics) {
  const reasons = [];

  if (metrics.runDays >= 3) {
    reasons.push(`最近 ${metrics.runDays} 天收盘连升，量能放大 ${roundNumber(metrics.volumeExpandPercent, 1)}%。`);
  }

  if (Number.isFinite(metrics.aboveMa5Percent)) {
    if (metrics.aboveMa5Percent >= 0) {
      reasons.push(`最新收盘高于 MA5 ${roundNumber(metrics.aboveMa5Percent, 2)}%。`);
    } else {
      reasons.push(`最新收盘贴近 MA5，偏离 ${roundNumber(Math.abs(metrics.aboveMa5Percent), 2)}%。`);
    }
  }

  reasons.push(`最近 ${metrics.recentWindowLength} 天没有连续 2 天跌破 MA5。`);
  reasons.push(`热门主题匹配：${metrics.matchedThemes.join('、')}。`);
  return reasons;
}

function evaluatePatternCandidate(candidate) {
  if (!Array.isArray(candidate.bars) || candidate.bars.length < MIN_BAR_COUNT) {
    return {
      code: candidate.code,
      error: 'bar_count_insufficient',
      passed: false,
    };
  }

  const bars = addMa5(candidate.bars);
  const recentBars = bars.slice(-8).filter((item) => Number.isFinite(item.ma5));
  const lastBar = bars[bars.length - 1];
  const maReferenceBar = bars[bars.length - 4] || bars[bars.length - 1];
  const aboveMa5Percent = Number.isFinite(lastBar.ma5) && lastBar.ma5 !== 0
    ? ((lastBar.close - lastBar.ma5) / lastBar.ma5) * 100
    : null;
  const maSlopePercent = Number.isFinite(lastBar.ma5) && Number.isFinite(maReferenceBar.ma5) && maReferenceBar.ma5 !== 0
    ? ((lastBar.ma5 - maReferenceBar.ma5) / Math.abs(maReferenceBar.ma5)) * 100
    : 0;
  const maxBelowStreak = getMaxConsecutiveBelowMa5(recentBars);
  const nearAboveCount = countNearOrAboveMa5(recentBars);
  const run = findBottomVolumeRun(bars);
  const passMa = recentBars.length >= 5
    && maxBelowStreak < 2
    && nearAboveCount >= Math.max(recentBars.length - 4, 4)
    && maSlopePercent > 0
    && lastBar.close >= lastBar.ma5 * (1 - MA_TOLERANCE_RATIO);
  const passed = Boolean(run) && passMa;
  const themeScore = candidate.themeRankScore || 0;
  const dailyChangePercent = Number.isFinite(candidate.dailyChangePercent)
    ? candidate.dailyChangePercent
    : getDailyChangePercent(lastBar);
  const score = (themeScore * 6)
    + ((run?.runLength || 0) * 5)
    + (run?.runReturnPercent || 0)
    + ((run?.volumeExpandPercent || 0) / 12)
    + Math.max(aboveMa5Percent || 0, 0)
    + (dailyChangePercent || 0);

  const metrics = {
    aboveMa5Percent: roundNumber(aboveMa5Percent, 2),
    close: roundNumber(lastBar.close, 3),
    dailyChangePercent: roundNumber(dailyChangePercent, 2),
    ma5: roundNumber(lastBar.ma5, 3),
    maSlopePercent: roundNumber(maSlopePercent, 2),
    matchedThemes: candidate.matchedThemes,
    maxBelowMa5Streak: maxBelowStreak,
    recentAboveCount: nearAboveCount,
    recentWindowLength: recentBars.length,
    runDays: run?.runLength || 0,
    runEndDate: run?.endAt || null,
    runReturnPercent: roundNumber(run?.runReturnPercent || 0, 2),
    runStartDate: run?.startAt || null,
    score: roundNumber(score, 2),
    themeRankScore: themeScore,
    turnoverRate: roundNumber(candidate.turnoverRate, 2),
    volumeExpandPercent: roundNumber(run?.volumeExpandPercent || 0, 1),
  };

  return {
    ...metrics,
    code: candidate.code,
    matchedThemes: candidate.matchedThemes,
    name: candidate.name,
    passed,
    reasons: buildPickReasons(metrics),
  };
}

function sortByMarketStrength(left, right) {
  const leftScore = Number.isFinite(left.dailyChangePercent) ? left.dailyChangePercent : Number.NEGATIVE_INFINITY;
  const rightScore = Number.isFinite(right.dailyChangePercent) ? right.dailyChangePercent : Number.NEGATIVE_INFINITY;

  if (rightScore !== leftScore) {
    return rightScore - leftScore;
  }

  return String(left.code).localeCompare(String(right.code), 'zh-CN');
}

async function buildCandidatePool(topThemes, marketSnapshot) {
  const conceptItems = await getConceptIndex();
  const marketByCode = new Map((marketSnapshot?.stocks || []).map((item) => [item.code, item]));
  const membership = new Map();
  const themeUniverse = new Set();
  const unresolvedThemes = [];
  const detailedThemes = [];

  for (const theme of topThemes) {
    const concept = matchThemeToConcept(theme.name, conceptItems);

    if (!concept) {
      unresolvedThemes.push(theme.name);
      detailedThemes.push({
        conceptCode: null,
        conceptUrl: null,
        constituentCount: 0,
        id: theme.id,
        matchedConcept: false,
        name: theme.name,
        sampledCandidateCount: 0,
      });
      continue;
    }

    const members = await fetchThemeMembers(concept);
    const eligibleMembers = members.filter((item) => isEligibleStock(item.code, item.name));
    const sampledCandidates = eligibleMembers
      .map((item) => {
        const marketInfo = marketByCode.get(item.code);
        return {
          code: item.code,
          dailyChangePercent: marketInfo?.dailyChangePercent ?? null,
          name: marketInfo?.name || item.name,
          turnoverRate: marketInfo?.turnoverRate ?? null,
        };
      })
      .sort(sortByMarketStrength)
      .slice(0, MAX_THEME_CANDIDATES);

    eligibleMembers.forEach((item) => {
      themeUniverse.add(item.code);
    });

    sampledCandidates.forEach((item) => {
      if (!membership.has(item.code)) {
        membership.set(item.code, {
          code: item.code,
          dailyChangePercent: item.dailyChangePercent,
          matchedThemes: [],
          name: item.name,
          themeRankScore: 0,
          turnoverRate: item.turnoverRate,
        });
      }

      const current = membership.get(item.code);
      current.dailyChangePercent = Number.isFinite(current.dailyChangePercent)
        ? current.dailyChangePercent
        : item.dailyChangePercent;
      current.name = current.name || item.name;
      current.turnoverRate = Number.isFinite(current.turnoverRate) ? current.turnoverRate : item.turnoverRate;
      current.matchedThemes.push(theme.name);
      current.themeRankScore += (TOP_HOT_THEME_COUNT + 1 - theme.id);
    });

    detailedThemes.push({
      conceptCode: concept.conceptCode,
      conceptUrl: concept.url,
      constituentCount: eligibleMembers.length,
      id: theme.id,
      matchedConcept: true,
      name: theme.name,
      sampledCandidateCount: sampledCandidates.length,
    });
  }

  const candidates = [...membership.values()]
    .map((item) => ({
      ...item,
      matchedThemes: [...new Set(item.matchedThemes)],
    }))
    .sort((left, right) => {
      if (right.matchedThemes.length !== left.matchedThemes.length) {
        return right.matchedThemes.length - left.matchedThemes.length;
      }

      if ((right.themeRankScore || 0) !== (left.themeRankScore || 0)) {
        return (right.themeRankScore || 0) - (left.themeRankScore || 0);
      }

      return sortByMarketStrength(left, right);
    })
    .slice(0, MAX_TOTAL_CANDIDATES);

  return {
    candidateCount: candidates.length,
    candidates,
    themeMemberCount: themeUniverse.size,
    topThemes: detailedThemes,
    unresolvedThemes,
  };
}

async function fetchDailyBars(code, dayKey) {
  const dayCache = internalState.klineCache[dayKey] || {};
  const cached = dayCache[code];
  const cachedAt = cached?.updatedAt ? Date.parse(cached.updatedAt) : 0;

  if (cached && Date.now() - cachedAt <= KLINE_CACHE_TTL_MS) {
    return cached.bars;
  }

  const url = `https://d.10jqka.com.cn/v6/line/33_${code}/01/all.js`;
  const text = await fetchText(url, {
    ...REQUEST_HEADERS,
    Referer: getKlineReferer(code),
  });
  const payload = parseJsonpPayload(text);
  const bars = decodeDailyBars(payload);

  internalState.klineCache[dayKey] = {
    ...dayCache,
    [code]: {
      bars,
      updatedAt: new Date().toISOString(),
    },
  };

  return bars;
}

async function mapWithConcurrency(items, concurrency, iterator) {
  const results = new Array(items.length);
  let cursor = 0;

  async function worker() {
    while (cursor < items.length) {
      const currentIndex = cursor;
      cursor += 1;
      results[currentIndex] = await iterator(items[currentIndex], currentIndex);
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

async function getTopThemes() {
  const homepageHtml = await fetchHomepageHtml();
  return extractHotThemes(homepageHtml, TOP_HOT_THEME_COUNT);
}

async function getMarketOverview() {
  try {
    const snapshot = await getMarketLeadersSnapshot({ hydrateIfEmpty: false });
    const latestLeaders = (snapshot.history?.[0]?.leaders || snapshot.leaders || [])
      .slice(0, 3)
      .map((item) => ({
        code: item.code,
        name: item.name,
        windowChangePercent: roundNumber(item.windowChangePercent, 2),
      }));

    return {
      available: latestLeaders.length > 0 || (snapshot.history?.length || 0) > 0,
      lastIntervalEndedAt: snapshot.comparisonEndedAt || snapshot.lastSuccessAt || null,
      latestLeaders,
      latestModeLabel: snapshot.comparisonModeLabel || null,
      savedIntervals: snapshot.history?.length || 0,
    };
  } catch (error) {
    return createEmptyMarketOverview();
  }
}

function buildWaitingSummary(referenceDate = new Date()) {
  const closeTime = createDateAtMinutes(referenceDate, AFTER_CLOSE_READY_MINUTES);
  return [
    `收盘复盘会在交易日 ${pad2(closeTime.getHours())}:${pad2(closeTime.getMinutes())} 后生成。`,
    '筛选范围只保留 00 / 60 开头股票，并且要求属于同花顺当日前五大热门主题。',
    '模式条件为：底部起来放量至少三天、基本沿 MA5 上行、最近没有连续两天跌破 MA5。',
  ];
}

function buildSummary(context) {
  const lines = [];
  const themeNames = context.topThemes.map((item) => item.name).join('、');
  const activeThemes = context.topThemes
    .filter((item) => item.sampledCandidateCount > 0)
    .slice(0, 3)
    .map((item) => `${item.name}${item.sampledCandidateCount}只候选`)
    .join('，');

  if (themeNames) {
    lines.push(`今日同花顺前五热门主题：${themeNames}。`);
  }

  if (activeThemes) {
    lines.push(`热点主线里更活跃的候选主要集中在：${activeThemes}。`);
  }

  lines.push(`前五热门主题共整理出 ${context.themeMemberCount} 只 00 / 60 主板成分股，并优先评估其中 ${context.candidateCount} 只。`);
  lines.push(`严格满足“底部放量至少三天 + 沿 MA5 上行 + 最近没有连续两天跌破 MA5”的股票共有 ${context.strictMatchCount} 只，当前展示前 ${context.finalPickCount} 只。`);

  if (context.marketOverview.available && context.marketOverview.latestLeaders.length > 0) {
    const names = context.marketOverview.latestLeaders
      .map((item) => `${item.name}${roundNumber(item.windowChangePercent, 2)}%`)
      .join('，');
    lines.push(`盘中 5 分钟历史已保存 ${context.marketOverview.savedIntervals} 个区间，收盘前最近一次主榜靠前的是：${names}。`);
  } else {
    lines.push('盘中 5 分钟历史不可用，本次复盘主要依据同花顺热门主题、成分股与日线形态。');
  }

  return lines;
}

async function refreshPatternPicks(options = {}) {
  const { force = false } = options;

  await ensureCacheLoaded();

  if (inFlightRefresh) {
    return inFlightRefresh;
  }

  const now = new Date();
  ensureTradingDayState(now);
  pruneCaches(state.day);
  syncSchedule(now);

  if (!force && !isAfterCloseReady(now)) {
    state.error = null;
    state.status = 'idle';
    state.summary = buildWaitingSummary(now);
    return cloneState();
  }

  const lastSuccessDay = state.lastSuccessAt ? formatTradingDay(new Date(state.lastSuccessAt)) : null;
  if (!force && state.picks.length > 0 && lastSuccessDay === state.day) {
    state.status = 'ready';
    return cloneState();
  }

  inFlightRefresh = (async () => {
    state.error = null;
    state.lastAttemptAt = new Date().toISOString();
    state.status = state.picks.length > 0 ? 'refreshing' : 'loading';

    try {
      const [topThemes, marketSnapshot, marketOverview] = await Promise.all([
        getTopThemes(),
        fetchMarketSnapshot(),
        getMarketOverview(),
      ]);
      const candidateContext = await buildCandidatePool(topThemes, marketSnapshot);
      const evaluationResults = await mapWithConcurrency(candidateContext.candidates, KLINE_CONCURRENCY, async (candidate) => {
        try {
          const bars = await fetchDailyBars(candidate.code, state.day);
          return evaluatePatternCandidate({
            ...candidate,
            bars,
          });
        } catch (error) {
          return {
            code: candidate.code,
            error: error.message,
            name: candidate.name,
            passed: false,
          };
        }
      });

      const strictMatches = evaluationResults
        .filter((item) => item && item.passed)
        .sort((left, right) => {
          if ((right.themeRankScore || 0) !== (left.themeRankScore || 0)) {
            return (right.themeRankScore || 0) - (left.themeRankScore || 0);
          }

          if ((right.runDays || 0) !== (left.runDays || 0)) {
            return (right.runDays || 0) - (left.runDays || 0);
          }

          if ((right.runReturnPercent || 0) !== (left.runReturnPercent || 0)) {
            return (right.runReturnPercent || 0) - (left.runReturnPercent || 0);
          }

          return (right.score || 0) - (left.score || 0);
        });
      const strictPicks = strictMatches
        .slice(0, TOP_PICK_COUNT)
        .map((item) => ({
          ...item,
          selectionMode: 'strict',
        }));
      const selectedCodes = new Set(strictPicks.map((item) => item.code));
      const fallbackMatches = evaluationResults
        .filter((item) => item
          && !item.error
          && !selectedCodes.has(item.code)
          && item.maxBelowMa5Streak < 2
          && item.recentAboveCount >= Math.max(item.recentWindowLength - 4, 4)
          && item.maSlopePercent > -0.5
          && (
            (item.runDays || 0) >= 2
            || (item.runReturnPercent || 0) >= 4
            || ((item.aboveMa5Percent || -100) >= -2 && (item.maSlopePercent || 0) > 0)
          ))
        .sort((left, right) => {
          if ((right.themeRankScore || 0) !== (left.themeRankScore || 0)) {
            return (right.themeRankScore || 0) - (left.themeRankScore || 0);
          }

          if ((right.recentAboveCount || 0) !== (left.recentAboveCount || 0)) {
            return (right.recentAboveCount || 0) - (left.recentAboveCount || 0);
          }

          return (right.score || 0) - (left.score || 0);
        });
      const fallbackPicks = fallbackMatches
        .slice(0, Math.max(0, TOP_PICK_COUNT - strictPicks.length))
        .map((item) => ({
          ...item,
          reasons: ['严格候选不足，当前按热门主题内最接近模式的 MA5 走势补位。', ...(item.reasons || [])],
          selectionMode: 'fallback',
        }));
      const picks = [...strictPicks, ...fallbackPicks]
        .slice(0, TOP_PICK_COUNT)
        .map((item, index) => ({
          ...item,
          rank: index + 1,
        }));
      const failedKlineCount = evaluationResults.filter((item) => item && item.error).length;
      const warnings = [];

      if (candidateContext.unresolvedThemes.length > 0) {
        warnings.push(`这些首页热门主题未能在概念题材库里精确匹配：${candidateContext.unresolvedThemes.join('、')}。`);
      }

      if (failedKlineCount > 0) {
        warnings.push(`有 ${failedKlineCount} 只候选股票的日线数据抓取失败，已自动跳过。`);
      }

      if (strictMatches.length < TOP_PICK_COUNT && fallbackPicks.length > 0) {
        warnings.push(`严格满足当前规则的股票只有 ${strictMatches.length} 只，剩余 ${fallbackPicks.length} 只按最接近模式补位。`);
      } else if (picks.length < TOP_PICK_COUNT) {
        warnings.push(`严格满足当前规则的股票只有 ${picks.length} 只。`);
      }

      state.error = null;
      state.filterCounts = {
        candidateCount: candidateContext.candidateCount,
        evaluatedCount: evaluationResults.length,
        finalPickCount: picks.length,
        strictMatchCount: strictMatches.length,
        themeMemberCount: candidateContext.themeMemberCount,
      };
      state.lastSuccessAt = new Date().toISOString();
      state.marketOverview = marketOverview;
      state.picks = picks;
      state.status = 'ready';
      state.summary = buildSummary({
        candidateCount: candidateContext.candidateCount,
        finalPickCount: picks.length,
        marketOverview,
        strictMatchCount: strictMatches.length,
        themeMemberCount: candidateContext.themeMemberCount,
        topThemes: candidateContext.topThemes,
      });
      state.topThemes = candidateContext.topThemes;
      state.warnings = warnings;
      syncSchedule(new Date());

      await saveCache();
      return cloneState();
    } catch (error) {
      state.error = error.message;
      state.status = state.picks.length > 0 ? 'stale' : 'error';
      if (state.summary.length === 0) {
        state.summary = buildWaitingSummary(now);
      }
      syncSchedule(new Date());
      await saveCache();
      return cloneState();
    } finally {
      inFlightRefresh = null;
    }
  })();

  return inFlightRefresh;
}

async function getPatternPicksSnapshot(options = {}) {
  const { hydrateIfEmpty = false } = options;

  await ensureCacheLoaded();

  const now = new Date();
  ensureTradingDayState(now);
  pruneCaches(state.day);
  syncSchedule(now);

  if (!isAfterCloseReady(now)) {
    state.error = null;
    state.status = 'idle';
    state.summary = buildWaitingSummary(now);
    return cloneState();
  }

  if (hydrateIfEmpty) {
    const lastSuccessDay = state.lastSuccessAt ? formatTradingDay(new Date(state.lastSuccessAt)) : null;
    const lastAttemptTime = state.lastAttemptAt ? Date.parse(state.lastAttemptAt) : 0;
    const canRetry = Date.now() - lastAttemptTime > EMPTY_STATE_RETRY_MS;

    if (!inFlightRefresh && canRetry && lastSuccessDay !== state.day) {
      await refreshPatternPicks({ force: true });
    }
  }

  return cloneState();
}

function startPatternPicksPolling() {
  if (pollingStarted) {
    return;
  }

  pollingStarted = true;

  ensureCacheLoaded()
    .then(() => {
      const now = new Date();
      ensureTradingDayState(now);
      pruneCaches(state.day);
      syncSchedule(now);

      if (isAfterCloseReady(now)) {
        const lastSuccessDay = state.lastSuccessAt ? formatTradingDay(new Date(state.lastSuccessAt)) : null;
        if (lastSuccessDay !== state.day) {
          return refreshPatternPicks({ force: true });
        }
      }

      state.status = isAfterCloseReady(now) && state.picks.length > 0 ? 'ready' : 'idle';
      if (state.summary.length === 0) {
        state.summary = buildWaitingSummary(now);
      }
      return null;
    })
    .catch((error) => {
      console.warn('Initial pattern picks refresh failed:', error.message);
    });

  setInterval(() => {
    refreshPatternPicks().catch((error) => {
      console.warn('Scheduled pattern picks refresh failed:', error.message);
    });
  }, POLL_INTERVAL_MS);
}

module.exports = {
  getPatternPicksSnapshot,
  refreshPatternPicks,
  startPatternPicksPolling,
};



