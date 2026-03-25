const fs = require('fs/promises');
const path = require('path');

const { fetchHomepageHtml, extractHotThemes } = require('./src/hotThemesService');
const { fetchMarketSnapshot, getMarketLeadersSnapshot } = require('./marketLeadersService');

const POLL_INTERVAL_MS = 10 * 60 * 1000;
const FETCH_TIMEOUT_MS = 20 * 1000;
const EMPTY_STATE_RETRY_MS = 60 * 1000;
const AFTER_CLOSE_READY_MINUTES = 15 * 60;
const TOP_HOT_THEME_COUNT = 5;
const MAX_THEME_CANDIDATES = 60;
const MAX_TOTAL_CANDIDATES = 220;
const TOP_PICK_COUNT = 5;
const BACKTEST_SIGNAL_DAY_COUNT = 10;
const MIN_BAR_COUNT = 25;
const KLINE_CONCURRENCY = 8;
const CONCEPT_INDEX_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const THEME_MEMBER_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const KLINE_CACHE_TTL_MS = 12 * 60 * 60 * 1000;
const MA_TOLERANCE_RATIO = 0.02;
const VOLUME_SHORT_WINDOW = 6;
const VOLUME_LONG_WINDOW = 10;
const VOLUME_BURST_WINDOW = 3;
const VOLUME_SUPPORT_THRESHOLD_RATIO = 1.03;
const STRICT_MAX_TREND_RETURN_PERCENT = 30;
const STRICT_MAX_BOTTOM_LIFT_PERCENT = 42;
const STABLE_MAX_TREND_RETURN_PERCENT = 28;
const STABLE_MAX_BOTTOM_LIFT_PERCENT = 49;
const NEAR_MAX_TREND_RETURN_PERCENT = 38;
const NEAR_MAX_BOTTOM_LIFT_PERCENT = 54;
const PREFERRED_RUN_DAYS = 6;
const PREFERRED_RUN_RETURN_PERCENT = 16;
const PREFERRED_ABOVE_MA5_PERCENT = 3.5;
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

function createEmptyBacktest() {
  return {
    averageReturnPercent: null,
    available: false,
    basis: '按当前筛选池回放，不追溯历史热门主题；当前实盘口径按小资金串行模式处理，只做 rank1，信号日收盘选股，下一交易日开盘买入，第三交易日收盘卖出，卖出后的下一交易日才能再次开仓，并剔除次日一字涨停无法买入的股票。',
    cumulativeReturnPercent: null,
    dayWinRatePercent: null,
    days: [],
    preferredRank: 1,
    rankStats: [],
    reviewedSignalCount: 0,
    skippedCapitalLockSignals: 0,
    signalDayCount: BACKTEST_SIGNAL_DAY_COUNT,
    totalSkippedLockedLimitUps: 0,
    totalTrades: 0,
    tradeWinRatePercent: null,
  };
}

const state = {
  day: null,
  error: null,
  filterCounts: createEmptyFilterCounts(),
  intervalMs: POLL_INTERVAL_MS,
  lastAttemptAt: null,
  lastSuccessAt: null,
  backtest: createEmptyBacktest(),
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
let nextRefreshTimer = null;

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

function roundPriceToTick(value) {
  if (!Number.isFinite(value)) {
    return null;
  }

  return Math.round(value * 100) / 100;
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

function buildSummaryFromState() {
  return buildSummary({
    backtest: state.backtest,
    candidateCount: state.filterCounts.candidateCount,
    finalPickCount: state.filterCounts.finalPickCount,
    marketOverview: state.marketOverview,
    strictMatchCount: state.filterCounts.strictMatchCount,
    themeMemberCount: state.filterCounts.themeMemberCount,
    topThemes: state.topThemes,
  });
}

function normalizeReadyState(referenceDate = new Date()) {
  const lastSuccessDay = state.lastSuccessAt ? formatTradingDay(new Date(state.lastSuccessAt)) : null;
  const currentDay = formatTradingDay(referenceDate);
  const sameDayReady = isAfterCloseReady(referenceDate) && lastSuccessDay === currentDay;
  const carryForwardReady = lastSuccessDay === state.day && lastSuccessDay !== currentDay;

  if ((!sameDayReady && !carryForwardReady) || state.picks.length === 0) {
    return false;
  }

  state.status = sameDayReady ? 'ready' : 'stale';
  state.error = null;
  state.summary = buildSummaryFromState();
  if (carryForwardReady) {
    state.summary.unshift(`当前展示的是 ${state.day} 收盘后的最新复盘结果，新的复盘会在下一个交易日收盘后覆盖。`);
  }
  return true;
}

function shouldCarryForwardLatestSnapshot(referenceDate = new Date()) {
  const currentDay = formatTradingDay(referenceDate);
  const lastSuccessDay = state.lastSuccessAt ? formatTradingDay(new Date(state.lastSuccessAt)) : null;

  return Boolean(
    state.day
    && lastSuccessDay
    && state.day === lastSuccessDay
    && state.day !== currentDay
    && state.picks.length > 0
    && !isAfterCloseReady(referenceDate),
  );
}

function resetDailyState(dayKey) {
  state.day = dayKey;
  state.error = null;
  state.filterCounts = createEmptyFilterCounts();
  state.lastAttemptAt = null;
  state.lastSuccessAt = null;
  state.backtest = createEmptyBacktest();
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
    if (shouldCarryForwardLatestSnapshot(referenceDate)) {
      return;
    }

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
    state.backtest = cached.backtest || createEmptyBacktest();
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

function average(values) {
  if (!Array.isArray(values) || values.length === 0) {
    return null;
  }

  const total = values.reduce((sum, value) => sum + value, 0);
  return total / values.length;
}

function getLimitUpPrice(prevClose) {
  if (!Number.isFinite(prevClose) || prevClose <= 0) {
    return null;
  }

  return roundPriceToTick(prevClose * 1.1);
}

function isLockedLimitUpBar(bar) {
  if (!bar) {
    return false;
  }

  const limitUpPrice = getLimitUpPrice(bar.prevClose);
  if (!Number.isFinite(limitUpPrice)) {
    return false;
  }

  return roundPriceToTick(bar.open) >= limitUpPrice
    && roundPriceToTick(bar.low) >= limitUpPrice
    && roundPriceToTick(bar.high) >= limitUpPrice
    && roundPriceToTick(bar.close) >= limitUpPrice;
}

function calculateVolumeTrendMetrics(bars) {
  const recentShort = bars.slice(-VOLUME_SHORT_WINDOW);
  const recentLong = bars.slice(-VOLUME_LONG_WINDOW);
  const recentBurst = recentShort.slice(-VOLUME_BURST_WINDOW);
  const volumeBurstAvg = average(recentBurst.map((bar) => bar.volume));
  const volumeShortAvg = average(recentShort.map((bar) => bar.volume));
  const volumeLongAvg = average(recentLong.map((bar) => bar.volume));
  const volumeSupportDays = Number.isFinite(volumeLongAvg)
    ? recentShort.filter((bar) => bar.volume >= volumeLongAvg * VOLUME_SUPPORT_THRESHOLD_RATIO).length
    : 0;
  const higherVolumeDays = recentShort.slice(1).filter((bar, index) => bar.volume >= recentShort[index].volume * 0.95).length;
  const volumeCenterLiftPercent = Number.isFinite(volumeShortAvg) && Number.isFinite(volumeLongAvg) && volumeLongAvg > 0
    ? ((volumeShortAvg - volumeLongAvg) / volumeLongAvg) * 100
    : null;
  const shortVolumeLiftPercent = Number.isFinite(volumeBurstAvg) && Number.isFinite(volumeShortAvg) && volumeShortAvg > 0
    ? ((volumeBurstAvg - volumeShortAvg) / volumeShortAvg) * 100
    : null;

  return {
    higherVolumeDays,
    volumeBurstAvg,
    volumeCenterLiftPercent,
    volumeLongAvg,
    volumeLongWindow: VOLUME_LONG_WINDOW,
    volumeShortAvg,
    volumeShortWindow: VOLUME_SHORT_WINDOW,
    volumeSupportDays,
    volumeSupportThresholdRatio: VOLUME_SUPPORT_THRESHOLD_RATIO,
    shortVolumeLiftPercent,
  };
}

function calculateTrendProgressMetrics(bars) {
  const recent10 = bars.slice(-10);
  const recent20 = bars.slice(-20);
  let advanceDays = recent10.length > 0 ? 1 : 0;
  let pullbackDays = 0;

  for (let index = 1; index < recent10.length; index += 1) {
    if (recent10[index].close >= recent10[index - 1].close * 0.995) {
      advanceDays += 1;
    } else {
      pullbackDays += 1;
    }
  }

  const startBar = recent10[0];
  const lastBar = recent10[recent10.length - 1];
  const baseLow = recent20.length > 0 ? Math.min(...recent20.map((bar) => bar.low)) : null;
  const trendReturnPercent = startBar && lastBar && startBar.close > 0
    ? ((lastBar.close - startBar.close) / startBar.close) * 100
    : null;
  const bottomLiftPercent = Number.isFinite(baseLow) && baseLow > 0 && lastBar
    ? ((lastBar.close - baseLow) / baseLow) * 100
    : null;

  return {
    advanceDays,
    bottomLiftPercent,
    pullbackDays,
    trendReturnPercent,
  };
}

function buildPickReasons(metrics) {
  const reasons = [];

  if (Number.isFinite(metrics.volumeCenterLiftPercent)) {
    reasons.push(`近 ${metrics.volumeShortWindow || VOLUME_SHORT_WINDOW} 日量能相对近 ${metrics.volumeLongWindow || VOLUME_LONG_WINDOW} 日抬升 ${roundNumber(metrics.volumeCenterLiftPercent, 1)}%，量能支撑日有 ${metrics.volumeSupportDays} 天。`);
  }

  if (Number.isFinite(metrics.aboveMa5Percent)) {
    if (metrics.aboveMa5Percent >= 0) {
      reasons.push(`最新收盘高于 MA5 ${roundNumber(metrics.aboveMa5Percent, 2)}%，5 日线斜率 ${roundNumber(metrics.maSlopePercent, 2)}%。`);
    } else {
      reasons.push(`最新收盘回踩 MA5 ${roundNumber(Math.abs(metrics.aboveMa5Percent), 2)}%，但仍维持趋势。`);
    }
  }

  reasons.push(`最近 ${metrics.recentWindowLength} 天里有 ${metrics.recentAboveCount} 天贴着 MA5 运行，没有连续 2 天跌破。`);

  if (Number.isFinite(metrics.trendReturnPercent) && Number.isFinite(metrics.bottomLiftPercent)) {
    reasons.push(`近 10 日趋势推进 ${roundNumber(metrics.trendReturnPercent, 2)}%，离近 20 日低点 ${roundNumber(metrics.bottomLiftPercent, 2)}%，期间震荡换手 ${metrics.pullbackDays || 0} 天。`);
  }

  if (metrics.stableTrendRide) {
    reasons.push('量能刚从近端中枢抬起来，虽然中途有一次额外震荡，但整体仍沿 MA5 稳定推进。');
  }

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
  const recentBars = bars.slice(-10).filter((item) => Number.isFinite(item.ma5));
  const lastBar = bars[bars.length - 1];
  const maReferenceBar = bars[bars.length - 5] || bars[bars.length - 1];
  const aboveMa5Percent = Number.isFinite(lastBar.ma5) && lastBar.ma5 !== 0
    ? ((lastBar.close - lastBar.ma5) / lastBar.ma5) * 100
    : null;
  const maSlopePercent = Number.isFinite(lastBar.ma5) && Number.isFinite(maReferenceBar.ma5) && maReferenceBar.ma5 !== 0
    ? ((lastBar.ma5 - maReferenceBar.ma5) / Math.abs(maReferenceBar.ma5)) * 100
    : 0;
  const maxBelowStreak = getMaxConsecutiveBelowMa5(recentBars);
  const nearAboveCount = countNearOrAboveMa5(recentBars);
  const volumeMetrics = calculateVolumeTrendMetrics(bars);
  const progressMetrics = calculateTrendProgressMetrics(bars);
  const passMa = recentBars.length >= 8
    && maxBelowStreak < 2
    && nearAboveCount >= Math.max(recentBars.length - 3, 6)
    && maSlopePercent > 0.5
    && maSlopePercent <= 10
    && lastBar.close >= lastBar.ma5 * (1 - MA_TOLERANCE_RATIO);
  const passVolume = Number.isFinite(volumeMetrics.volumeCenterLiftPercent)
    && volumeMetrics.volumeCenterLiftPercent >= 2
    && volumeMetrics.volumeSupportDays >= 4
    && Number.isFinite(volumeMetrics.shortVolumeLiftPercent)
    && volumeMetrics.shortVolumeLiftPercent >= 0;
  const strictStructure = Number.isFinite(progressMetrics.trendReturnPercent)
    && progressMetrics.trendReturnPercent >= 5
    && progressMetrics.trendReturnPercent <= STRICT_MAX_TREND_RETURN_PERCENT
    && Number.isFinite(progressMetrics.bottomLiftPercent)
    && progressMetrics.bottomLiftPercent >= 8
    && progressMetrics.bottomLiftPercent <= STRICT_MAX_BOTTOM_LIFT_PERCENT
    && progressMetrics.pullbackDays <= 3;
  const stableTrendRide = recentBars.length >= 8
    && maxBelowStreak === 0
    && nearAboveCount >= Math.max(recentBars.length - 1, 8)
    && Number.isFinite(aboveMa5Percent)
    && aboveMa5Percent >= 0
    && aboveMa5Percent <= 9
    && maSlopePercent >= 1.5
    && maSlopePercent <= 10
    && Number.isFinite(progressMetrics.trendReturnPercent)
    && progressMetrics.trendReturnPercent >= 8
    && progressMetrics.trendReturnPercent <= STABLE_MAX_TREND_RETURN_PERCENT
    && Number.isFinite(progressMetrics.bottomLiftPercent)
    && progressMetrics.bottomLiftPercent >= 12
    && progressMetrics.bottomLiftPercent <= STABLE_MAX_BOTTOM_LIFT_PERCENT
    && progressMetrics.pullbackDays <= 4
    && volumeMetrics.volumeSupportDays >= 5;
  const passDeviation = Number.isFinite(aboveMa5Percent)
    && aboveMa5Percent >= -1
    && (aboveMa5Percent <= 8 || stableTrendRide);
  const passStructure = strictStructure || stableTrendRide;
  const nearTrendCandidate = recentBars.length >= 8
    && maxBelowStreak < 2
    && nearAboveCount >= Math.max(recentBars.length - 3, 6)
    && maSlopePercent > 0
    && maSlopePercent <= 12
    && Number.isFinite(aboveMa5Percent)
    && aboveMa5Percent >= -2
    && aboveMa5Percent <= 9
    && Number.isFinite(progressMetrics.trendReturnPercent)
    && progressMetrics.trendReturnPercent >= 3
    && progressMetrics.trendReturnPercent <= NEAR_MAX_TREND_RETURN_PERCENT
    && Number.isFinite(progressMetrics.bottomLiftPercent)
    && progressMetrics.bottomLiftPercent >= 5
    && progressMetrics.bottomLiftPercent <= NEAR_MAX_BOTTOM_LIFT_PERCENT
    && progressMetrics.pullbackDays <= 4
    && ((Number.isFinite(volumeMetrics.volumeCenterLiftPercent) && volumeMetrics.volumeCenterLiftPercent >= -2)
      || volumeMetrics.volumeSupportDays >= 3);
  const passed = passDeviation && passVolume && passStructure && passMa;
  const themeScore = candidate.themeRankScore || 0;
  const dailyChangePercent = Number.isFinite(candidate.dailyChangePercent)
    ? candidate.dailyChangePercent
    : getDailyChangePercent(lastBar);
  const deviationPenalty = Number.isFinite(aboveMa5Percent)
    ? Math.abs(aboveMa5Percent - PREFERRED_ABOVE_MA5_PERCENT)
    : 20;
  const runPenalty = Number.isFinite(progressMetrics.advanceDays)
    ? Math.abs(progressMetrics.advanceDays - PREFERRED_RUN_DAYS)
    : 5;
  const runReturnPenalty = Number.isFinite(progressMetrics.trendReturnPercent)
    ? Math.abs(progressMetrics.trendReturnPercent - PREFERRED_RUN_RETURN_PERCENT)
    : 15;
  const score = (themeScore * 8)
    + (nearAboveCount * 4)
    + ((volumeMetrics.volumeSupportDays || 0) * 3)
    + Math.min(progressMetrics.trendReturnPercent || 0, 25)
    + (volumeMetrics.volumeCenterLiftPercent || 0)
    + (volumeMetrics.shortVolumeLiftPercent || 0)
    + (dailyChangePercent || 0)
    - (deviationPenalty * 2)
    - (runPenalty * 1.5)
    - (runReturnPenalty * 0.5)
    - (Math.max(maSlopePercent - 8, 0) * 1.5)
    - (Math.max((progressMetrics.bottomLiftPercent || 0) - STRICT_MAX_BOTTOM_LIFT_PERCENT, 0) * 1.2);

  const metrics = {
    aboveMa5Percent: roundNumber(aboveMa5Percent, 2),
    bottomLiftPercent: roundNumber(progressMetrics.bottomLiftPercent, 2),
    close: roundNumber(lastBar.close, 3),
    dailyChangePercent: roundNumber(dailyChangePercent, 2),
    ma5: roundNumber(lastBar.ma5, 3),
    maSlopePercent: roundNumber(maSlopePercent, 2),
    matchedThemes: candidate.matchedThemes,
    maxBelowMa5Streak: maxBelowStreak,
    nearTrendCandidate,
    pullbackDays: progressMetrics.pullbackDays,
    recentAboveCount: nearAboveCount,
    recentWindowLength: recentBars.length,
    runDays: progressMetrics.advanceDays || 0,
    runReturnPercent: roundNumber(progressMetrics.trendReturnPercent || 0, 2),
    score: roundNumber(score, 2),
    stableTrendRide,
    themeRankScore: themeScore,
    turnoverRate: roundNumber(candidate.turnoverRate, 2),
    shortVolumeLiftPercent: roundNumber(volumeMetrics.shortVolumeLiftPercent, 1),
    volumeCenterLiftPercent: roundNumber(volumeMetrics.volumeCenterLiftPercent, 1),
    volumeExpandPercent: roundNumber(volumeMetrics.volumeCenterLiftPercent || 0, 1),
    volumeLongAvg: roundNumber(volumeMetrics.volumeLongAvg, 0),
    volumeLongWindow: volumeMetrics.volumeLongWindow,
    volumeShortAvg: roundNumber(volumeMetrics.volumeShortAvg, 0),
    volumeShortWindow: volumeMetrics.volumeShortWindow,
    volumeSupportDays: volumeMetrics.volumeSupportDays,
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
      .filter((item) => isEligibleStock(item.code, item.name))
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

function getRankingPreferences(rankingFeedback = null) {
  return {
    preferredAboveMa5Percent: Number.isFinite(rankingFeedback?.preferredAboveMa5Percent)
      ? rankingFeedback.preferredAboveMa5Percent
      : PREFERRED_ABOVE_MA5_PERCENT,
    preferredRunDays: Number.isFinite(rankingFeedback?.preferredRunDays)
      ? rankingFeedback.preferredRunDays
      : PREFERRED_RUN_DAYS,
    preferredRunReturnPercent: Number.isFinite(rankingFeedback?.preferredRunReturnPercent)
      ? rankingFeedback.preferredRunReturnPercent
      : PREFERRED_RUN_RETURN_PERCENT,
    preferredShortVolumeLiftPercent: Number.isFinite(rankingFeedback?.preferredShortVolumeLiftPercent)
      ? rankingFeedback.preferredShortVolumeLiftPercent
      : 8,
    preferredVolumeCenterLiftPercent: Number.isFinite(rankingFeedback?.preferredVolumeCenterLiftPercent)
      ? rankingFeedback.preferredVolumeCenterLiftPercent
      : 16,
  };
}

function buildFreshnessPenalty(item, rankingPreferences) {
  return Math.abs((item.runDays || 0) - rankingPreferences.preferredRunDays)
    + (Math.abs((item.runReturnPercent || 0) - rankingPreferences.preferredRunReturnPercent) / 6)
    + (Math.abs((item.aboveMa5Percent || 0) - rankingPreferences.preferredAboveMa5Percent) / 2);
}

function buildFeedbackBonus(item, rankingPreferences) {
  const volumeGap = Math.abs((item.volumeCenterLiftPercent || 0) - rankingPreferences.preferredVolumeCenterLiftPercent);
  const shortLiftGap = Math.abs((item.shortVolumeLiftPercent || 0) - rankingPreferences.preferredShortVolumeLiftPercent);
  return 24 - (volumeGap / 3) - (shortLiftGap / 4);
}

function sortStrictMatches(left, right, rankingFeedback = null) {
  const rankingPreferences = getRankingPreferences(rankingFeedback);
  if ((right.themeRankScore || 0) !== (left.themeRankScore || 0)) {
    return (right.themeRankScore || 0) - (left.themeRankScore || 0);
  }

  const leftFreshnessPenalty = buildFreshnessPenalty(left, rankingPreferences);
  const rightFreshnessPenalty = buildFreshnessPenalty(right, rankingPreferences);

  if (leftFreshnessPenalty !== rightFreshnessPenalty) {
    return leftFreshnessPenalty - rightFreshnessPenalty;
  }

  const leftFeedbackBonus = buildFeedbackBonus(left, rankingPreferences);
  const rightFeedbackBonus = buildFeedbackBonus(right, rankingPreferences);
  if (rightFeedbackBonus !== leftFeedbackBonus) {
    return rightFeedbackBonus - leftFeedbackBonus;
  }

  if ((right.volumeSupportDays || 0) !== (left.volumeSupportDays || 0)) {
    return (right.volumeSupportDays || 0) - (left.volumeSupportDays || 0);
  }

  return (right.score || 0) - (left.score || 0);
}

function sortFallbackMatches(left, right, rankingFeedback = null) {
  const rankingPreferences = getRankingPreferences(rankingFeedback);
  if ((right.themeRankScore || 0) !== (left.themeRankScore || 0)) {
    return (right.themeRankScore || 0) - (left.themeRankScore || 0);
  }

  const leftFeedbackBonus = buildFeedbackBonus(left, rankingPreferences);
  const rightFeedbackBonus = buildFeedbackBonus(right, rankingPreferences);
  if (rightFeedbackBonus !== leftFeedbackBonus) {
    return rightFeedbackBonus - leftFeedbackBonus;
  }

  if ((right.recentAboveCount || 0) !== (left.recentAboveCount || 0)) {
    return (right.recentAboveCount || 0) - (left.recentAboveCount || 0);
  }

  return (right.score || 0) - (left.score || 0);
}

function sortReserveMatches(left, right, rankingFeedback = null) {
  const rankingPreferences = getRankingPreferences(rankingFeedback);
  if ((right.themeRankScore || 0) !== (left.themeRankScore || 0)) {
    return (right.themeRankScore || 0) - (left.themeRankScore || 0);
  }

  const leftDeviation = Math.abs((left.aboveMa5Percent ?? 99) - rankingPreferences.preferredAboveMa5Percent);
  const rightDeviation = Math.abs((right.aboveMa5Percent ?? 99) - rankingPreferences.preferredAboveMa5Percent);
  if (leftDeviation !== rightDeviation) {
    return leftDeviation - rightDeviation;
  }

  const leftFeedbackBonus = buildFeedbackBonus(left, rankingPreferences);
  const rightFeedbackBonus = buildFeedbackBonus(right, rankingPreferences);
  if (rightFeedbackBonus !== leftFeedbackBonus) {
    return rightFeedbackBonus - leftFeedbackBonus;
  }

  return (right.score || 0) - (left.score || 0);
}

function selectPicksFromEvaluations(evaluationResults, options = {}) {
  const { rankingFeedback = null, unresolvedThemes = [] } = options;
  const strictMatches = evaluationResults
    .filter((item) => item && item.passed)
    .sort((left, right) => sortStrictMatches(left, right, rankingFeedback));
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
      && item.nearTrendCandidate)
    .sort((left, right) => sortFallbackMatches(left, right, rankingFeedback));
  const fallbackPicks = fallbackMatches
    .slice(0, Math.max(0, TOP_PICK_COUNT - strictPicks.length))
    .map((item) => ({
      ...item,
      reasons: ['严格候选不足，当前按热门主题内最接近模式的趋势推进形态补位。', ...(item.reasons || [])],
      selectionMode: 'fallback',
    }));
  const fallbackCodes = new Set([...selectedCodes, ...fallbackPicks.map((item) => item.code)]);
  const reserveMatches = evaluationResults
    .filter((item) => item
      && !item.error
      && !fallbackCodes.has(item.code)
      && item.maxBelowMa5Streak < 2
      && item.recentAboveCount >= Math.max(item.recentWindowLength - 4, 4)
      && (item.maSlopePercent || 0) > 0)
    .sort((left, right) => sortReserveMatches(left, right, rankingFeedback));
  const reservePicks = reserveMatches
    .slice(0, Math.max(0, TOP_PICK_COUNT - strictPicks.length - fallbackPicks.length))
    .map((item) => ({
      ...item,
      reasons: ['趋势推进候选仍不足，当前按同主题里 MA5 最顺的备选形态补位。', ...(item.reasons || [])],
      selectionMode: 'reserve',
    }));
  const picks = [...strictPicks, ...fallbackPicks, ...reservePicks]
    .slice(0, TOP_PICK_COUNT)
    .map((item, index) => ({
      ...item,
      rank: index + 1,
    }));
  const failedKlineCount = evaluationResults.filter((item) => item && item.error).length;
  const warnings = [];

  if (unresolvedThemes.length > 0) {
    warnings.push(`这些首页热门主题未能在概念题材库里精确匹配：${unresolvedThemes.join('、')}。`);
  }

  if (failedKlineCount > 0) {
    warnings.push(`有 ${failedKlineCount} 只候选股票的日线数据抓取失败，已自动跳过。`);
  }

  if (strictMatches.length < TOP_PICK_COUNT && fallbackPicks.length > 0) {
    warnings.push(`严格满足当前规则的股票只有 ${strictMatches.length} 只，剩余 ${fallbackPicks.length} 只按最接近的趋势推进形态补位。`);
  }

  if (reservePicks.length > 0) {
    warnings.push(`趋势推进候选仍不足，另补了 ${reservePicks.length} 只 MA5 更顺的备选形态。`);
  } else if (picks.length < TOP_PICK_COUNT) {
    warnings.push(`严格满足当前规则的股票只有 ${picks.length} 只。`);
  }

  return {
    failedKlineCount,
    picks,
    reservePicks,
    strictMatches,
    warnings,
  };
}

function buildBacktestTradeAttempt(record, signalDate, pick) {
  if (!record) {
    return null;
  }

  const signalIndex = record.bars.findIndex((bar) => bar.date === signalDate);
  const entryBar = record.bars[signalIndex + 1];
  const exitBar = record.bars[signalIndex + 2];
  const tradeBase = {
    aboveMa5Percent: pick.aboveMa5Percent,
    code: pick.code,
    name: pick.name,
    rank: pick.rank,
    runDays: pick.runDays,
    runReturnPercent: pick.runReturnPercent,
    score: pick.score,
    selectionMode: pick.selectionMode,
    shortVolumeLiftPercent: pick.shortVolumeLiftPercent,
    themeRankScore: pick.themeRankScore,
    volumeCenterLiftPercent: pick.volumeCenterLiftPercent,
  };

  if (!entryBar || !exitBar || !Number.isFinite(entryBar.open) || !Number.isFinite(exitBar.close) || entryBar.open <= 0) {
    return {
      ...tradeBase,
      skippedReason: 'invalid_trade_window',
    };
  }

  if (isLockedLimitUpBar(entryBar)) {
    return {
      ...tradeBase,
      entryDate: entryBar.date,
      exitDate: exitBar.date,
      skippedReason: 'locked_limit_up',
    };
  }

  return {
    ...tradeBase,
    entryDate: entryBar.date,
    entryOpen: roundNumber(entryBar.open, 3),
    exitClose: roundNumber(exitBar.close, 3),
    exitDate: exitBar.date,
    returnPercent: roundNumber(((exitBar.close - entryBar.open) / entryBar.open) * 100, 2),
    skippedReason: null,
  };
}

function buildBacktestSignalReviews(candidateRecords, selectionOptions = {}) {
  const usableRecords = candidateRecords.filter((item) => item && !item.error && Array.isArray(item.bars) && item.bars.length >= MIN_BAR_COUNT + 2);
  if (usableRecords.length === 0) {
    return [];
  }

  const recordByCode = new Map(usableRecords.map((item) => [item.candidate.code, item]));
  const referenceBars = usableRecords
    .map((item) => item.bars)
    .sort((left, right) => right.length - left.length)[0];
  const signalBars = referenceBars.slice(-(BACKTEST_SIGNAL_DAY_COUNT + 2), -2);

  return signalBars.map((signalBar) => {
    const signalDate = signalBar.date;
    const historicalResults = usableRecords.map((record) => {
      const signalIndex = record.bars.findIndex((bar) => bar.date === signalDate);

      if (signalIndex === -1 || signalIndex + 2 >= record.bars.length) {
        return {
          code: record.candidate.code,
          error: 'trade_window_unavailable',
          name: record.candidate.name,
          passed: false,
        };
      }

      return evaluatePatternCandidate({
        ...record.candidate,
        dailyChangePercent: null,
        turnoverRate: null,
        bars: record.bars.slice(0, signalIndex + 1),
      });
    });
    const selection = selectPicksFromEvaluations(historicalResults, selectionOptions);
    const picks = selection.picks
      .map((pick) => buildBacktestTradeAttempt(recordByCode.get(pick.code), signalDate, pick))
      .filter(Boolean)
      .sort((left, right) => (left.rank || 99) - (right.rank || 99));

    return {
      pickCount: selection.picks.length,
      picks,
      signalDate,
      skippedLockedLimitUps: picks.filter((item) => item.skippedReason === 'locked_limit_up').length,
      strictCount: selection.picks.filter((item) => item.selectionMode === 'strict').length,
      tradablePickCount: picks.filter((item) => !item.skippedReason && Number.isFinite(item.returnPercent)).length,
    };
  });
}

function buildSerialTradePlan(signalReviews, preferredRank = 1) {
  let nextEligibleSignalDate = null;
  let skippedCapitalLockSignals = 0;
  let skippedLockedLimitUps = 0;
  const trades = [];

  signalReviews.forEach((review) => {
    const preferredPick = review.picks.find((item) => item.rank === preferredRank);
    if (!preferredPick) {
      return;
    }

    if (nextEligibleSignalDate && review.signalDate < nextEligibleSignalDate) {
      skippedCapitalLockSignals += 1;
      return;
    }

    if (preferredPick.skippedReason === 'locked_limit_up') {
      skippedLockedLimitUps += 1;
      return;
    }

    if (preferredPick.skippedReason || !Number.isFinite(preferredPick.returnPercent)) {
      return;
    }

    trades.push({
      ...preferredPick,
      pickCount: review.pickCount,
      signalDate: review.signalDate,
      strictCount: review.strictCount,
    });
    nextEligibleSignalDate = preferredPick.exitDate || review.signalDate;
  });

  const averageReturnPercent = average(trades.map((item) => item.returnPercent));
  const cumulativeReturnPercent = trades.reduce((accumulator, item) => accumulator * (1 + (item.returnPercent / 100)), 1);
  const winningTrades = trades.filter((item) => item.returnPercent > 0).length;

  return {
    averageReturnPercent: roundNumber(averageReturnPercent, 2),
    cumulativeReturnPercent: roundNumber((cumulativeReturnPercent - 1) * 100, 2),
    preferredRank,
    skippedCapitalLockSignals,
    skippedLockedLimitUps,
    totalTrades: trades.length,
    trades,
    winRatePercent: trades.length > 0
      ? roundNumber((winningTrades / trades.length) * 100, 2)
      : null,
  };
}

function buildRankStats(signalReviews) {
  return Array.from({ length: TOP_PICK_COUNT }, (_, index) => {
    const rank = index + 1;
    const rankPicks = signalReviews
      .map((review) => review.picks.find((item) => item.rank === rank) || null)
      .filter(Boolean);
    const serialPlan = buildSerialTradePlan(signalReviews, rank);

    return {
      averageReturnPercent: serialPlan.averageReturnPercent,
      cumulativeReturnPercent: serialPlan.cumulativeReturnPercent,
      rank,
      reviewedSignals: rankPicks.length,
      skippedCapitalLockSignals: serialPlan.skippedCapitalLockSignals,
      skippedLockedLimitUps: serialPlan.skippedLockedLimitUps,
      totalTrades: serialPlan.totalTrades,
      tradeWinRatePercent: serialPlan.winRatePercent,
    };
  });
}

function buildRankingFeedbackProfile(signalReviews) {
  const winners = signalReviews
    .flatMap((review) => review.picks)
    .filter((item) => !item.skippedReason && Number.isFinite(item.returnPercent) && item.returnPercent > 0);

  if (winners.length < 6) {
    return null;
  }

  const weights = winners.map((item) => Math.max(item.returnPercent, 0.5));
  const weightedAverage = (values) => {
    if (!Array.isArray(values) || values.length === 0) {
      return null;
    }

    const totalWeight = weights.reduce((sum, weight) => sum + weight, 0);
    if (!Number.isFinite(totalWeight) || totalWeight <= 0) {
      return null;
    }

    const total = values.reduce((sum, value, index) => sum + (value * weights[index]), 0);
    return total / totalWeight;
  };
  const clamp = (value, min, max) => {
    if (!Number.isFinite(value)) {
      return null;
    }

    return Math.min(max, Math.max(min, value));
  };

  return {
    preferredAboveMa5Percent: clamp(weightedAverage(winners.map((item) => item.aboveMa5Percent || 0)), 0.5, 6.5),
    preferredRunDays: clamp(weightedAverage(winners.map((item) => item.runDays || 0)), 4, 8),
    preferredRunReturnPercent: clamp(weightedAverage(winners.map((item) => item.runReturnPercent || 0)), 6, 22),
    preferredShortVolumeLiftPercent: clamp(weightedAverage(winners.map((item) => item.shortVolumeLiftPercent || 0)), 0, 25),
    preferredVolumeCenterLiftPercent: clamp(weightedAverage(winners.map((item) => item.volumeCenterLiftPercent || 0)), 4, 30),
  };
}

function buildBacktestSnapshot(candidateRecords, options = {}) {
  const { preferredRank = 1, rankingFeedback = null } = options;
  const signalReviews = buildBacktestSignalReviews(candidateRecords, { rankingFeedback });
  if (signalReviews.length === 0) {
    return createEmptyBacktest();
  }

  const serialPlan = buildSerialTradePlan(signalReviews, preferredRank);
  const rankStats = buildRankStats(signalReviews);

  return {
    averageReturnPercent: serialPlan.averageReturnPercent,
    available: true,
    basis: `按当前筛选池回放，不追溯历史热门主题；当前实盘口径按小资金串行模式处理，只做 rank${preferredRank}，信号日收盘选股，下一交易日开盘买入，第三交易日收盘卖出，卖出后的下一交易日才能再次开仓，并剔除次日一字涨停无法买入的股票。`,
    cumulativeReturnPercent: serialPlan.cumulativeReturnPercent,
    dayWinRatePercent: serialPlan.winRatePercent,
    days: serialPlan.trades,
    preferredRank,
    rankStats,
    reviewedSignalCount: signalReviews.length,
    signalDayCount: signalReviews.length,
    skippedCapitalLockSignals: serialPlan.skippedCapitalLockSignals,
    totalSkippedLockedLimitUps: serialPlan.skippedLockedLimitUps,
    totalTrades: serialPlan.totalTrades,
    tradeWinRatePercent: serialPlan.winRatePercent,
  };
}

function buildWaitingSummary(referenceDate = new Date()) {
  const closeTime = createDateAtMinutes(referenceDate, AFTER_CLOSE_READY_MINUTES);
  return [
    `收盘复盘会在交易日 ${pad2(closeTime.getHours())}:${pad2(closeTime.getMinutes())} 后自动生成，生成后会持续展示直到下一次收盘结果覆盖。`,
    '筛选范围只保留 00 / 60 开头股票，并且要求属于同花顺当日前五大热门主题。',
    '模式条件为：近 6 日量能相对近 10 日明显抬升、沿 MA5 稳定推进、允许少量震荡换手，但最近没有连续两天跌破 MA5，且收盘不要离 5 日线太远。',
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
  lines.push(`严格满足“近端量能刚抬升 + 沿 MA5 稳定推进 + 允许少量震荡换手 + 乖离不过大”的股票共有 ${context.strictMatchCount} 只，当前展示前 ${context.finalPickCount} 只。`);

  if (context.marketOverview.available && context.marketOverview.latestLeaders.length > 0) {
    const names = context.marketOverview.latestLeaders
      .map((item) => `${item.name}${roundNumber(item.windowChangePercent, 2)}%`)
      .join('，');
    lines.push(`盘中 5 分钟历史已保存 ${context.marketOverview.savedIntervals} 个区间，收盘前最近一次主榜靠前的是：${names}。`);
  } else {
    lines.push('盘中 5 分钟历史不可用，本次复盘主要依据同花顺热门主题、成分股与日线形态。');
  }

  if (context.backtest?.available) {
    const bestRank = [...(context.backtest.rankStats || [])]
      .sort((left, right) => {
        if ((right.cumulativeReturnPercent || 0) !== (left.cumulativeReturnPercent || 0)) {
          return (right.cumulativeReturnPercent || 0) - (left.cumulativeReturnPercent || 0);
        }

        return (right.averageReturnPercent || 0) - (left.averageReturnPercent || 0);
      })[0];
    lines.push(`最近 ${context.backtest.reviewedSignalCount || context.backtest.signalDayCount} 个信号日按小资金串行口径只回放 rank${context.backtest.preferredRank}，单笔均值 ${roundNumber(context.backtest.averageReturnPercent, 2)}%，累计收益 ${roundNumber(context.backtest.cumulativeReturnPercent, 2)}%，期间因资金占用跳过 ${context.backtest.skippedCapitalLockSignals || 0} 个信号。`);
    if (bestRank) {
      lines.push(`五个候选位次分开统计后，近期最强的是 rank${bestRank.rank}，累计收益 ${roundNumber(bestRank.cumulativeReturnPercent, 2)}%，单笔胜率 ${roundNumber(bestRank.tradeWinRatePercent, 2)}%。当前排序会参考最近收益反馈，尽量把更适合实盘的图形往前推。`);
    }
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
    if (normalizeReadyState(now)) {
      return cloneState();
    }

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
      const candidateRecords = await mapWithConcurrency(candidateContext.candidates, KLINE_CONCURRENCY, async (candidate) => {
        try {
          const bars = await fetchDailyBars(candidate.code, state.day);
          return {
            bars,
            candidate,
            evaluation: evaluatePatternCandidate({
              ...candidate,
              bars,
            }),
          };
        } catch (error) {
          return {
            ...candidate,
            bars: [],
            error: error.message,
            evaluation: {
              code: candidate.code,
              error: error.message,
              name: candidate.name,
              passed: false,
            },
          };
        }
      });
      const evaluationResults = candidateRecords.map((item) => item.evaluation);
      const baseSignalReviews = buildBacktestSignalReviews(candidateRecords);
      const rankingFeedback = buildRankingFeedbackProfile(baseSignalReviews);
      const selection = selectPicksFromEvaluations(evaluationResults, {
        rankingFeedback,
        unresolvedThemes: candidateContext.unresolvedThemes,
      });
      const backtest = buildBacktestSnapshot(candidateRecords, {
        preferredRank: 1,
        rankingFeedback,
      });

      state.error = null;
      state.filterCounts = {
        candidateCount: candidateContext.candidateCount,
        evaluatedCount: evaluationResults.length,
        finalPickCount: selection.picks.length,
        strictMatchCount: selection.strictMatches.length,
        themeMemberCount: candidateContext.themeMemberCount,
      };
      state.lastSuccessAt = new Date().toISOString();
      state.backtest = backtest;
      state.marketOverview = marketOverview;
      state.picks = selection.picks;
      state.status = 'ready';
      state.summary = buildSummary({
        backtest,
        candidateCount: candidateContext.candidateCount,
        finalPickCount: selection.picks.length,
        marketOverview,
        strictMatchCount: selection.strictMatches.length,
        themeMemberCount: candidateContext.themeMemberCount,
        topThemes: candidateContext.topThemes,
      });
      state.topThemes = candidateContext.topThemes;
      state.warnings = selection.warnings;
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
    if (normalizeReadyState(now)) {
      return cloneState();
    }

    state.error = null;
    state.status = 'idle';
    state.summary = buildWaitingSummary(now);
    return cloneState();
  }

  normalizeReadyState(now);

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

function scheduleNextPatternPicksRefresh() {
  if (!pollingStarted) {
    return;
  }

  if (nextRefreshTimer) {
    clearTimeout(nextRefreshTimer);
  }

  const now = new Date();
  ensureTradingDayState(now);
  pruneCaches(state.day);
  syncSchedule(now);

  const nextRefreshAt = getNextTradingCloseTime(now);
  state.nextRefreshAt = nextRefreshAt.toISOString();
  const delay = Math.max(nextRefreshAt.getTime() - Date.now(), 1000);

  nextRefreshTimer = setTimeout(() => {
    refreshPatternPicks({ force: true })
      .catch((error) => {
        console.warn('Scheduled pattern picks refresh failed:', error.message);
      })
      .finally(() => {
        scheduleNextPatternPicksRefresh();
      });
  }, delay);
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

      if (!normalizeReadyState(now)) {
        state.status = 'idle';
      }

      if (state.summary.length === 0) {
        state.summary = buildWaitingSummary(now);
      }
      return null;
    })
    .catch((error) => {
      console.warn('Initial pattern picks refresh failed:', error.message);
    })
    .finally(() => {
      scheduleNextPatternPicksRefresh();
    });
}

module.exports = {
  getPatternPicksSnapshot,
  refreshPatternPicks,
  startPatternPicksPolling,
};

