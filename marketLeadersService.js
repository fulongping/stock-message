const fs = require('fs/promises');
const path = require('path');

const SOURCE_URL = 'https://m.10jqka.com.cn/hq/rank/market.html';
const RANK_API_BASE = 'https://d.10jqka.com.cn/v2/rank/33,17/199112';
const CONCEPT_PAGE_BASE = 'https://basic.10jqka.com.cn';
const FETCH_INTERVAL_MS = 5 * 60 * 1000;
const FETCH_TIMEOUT_MS = 20 * 1000;
const EMPTY_STATE_RETRY_MS = 60 * 1000;
const RANK_BATCH_SIZE = 3000;
const TOP_STOCK_COUNT = 10;
const MAX_GROUPS = 8;
const MAX_CONCEPTS_PER_STOCK = 5;
const CONCEPT_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const MAX_CONCEPT_CACHE_ITEMS = 2000;
const SLOT_SNAPSHOT_LIMIT = 8;
const DAILY_HISTORY_LIMIT = 64;
const DISPLAY_SWITCH_INTERVAL_MINUTES = 15;
const HYDRATE_BOUNDARY_GRACE_MS = 90 * 1000;
const MARKET_SCOPE_LABEL = '沪深两市主板';
const SUPPORTED_CODE_LABEL = '60 / 00';
const TRADING_SESSIONS_LABEL = '09:30-11:30、13:00-15:00';
const MORNING_SESSION_START_MINUTES = 9 * 60 + 30;
const MORNING_SESSION_END_MINUTES = 11 * 60 + 30;
const AFTERNOON_SESSION_START_MINUTES = 13 * 60;
const AFTERNOON_SESSION_END_MINUTES = 15 * 60;
const CACHE_FILE = path.join(__dirname, 'data', 'market-leaders-cache.json');
const ROTATION_REPORT_DIR = path.join(__dirname, 'data', 'theme-rotation');
const ROTATION_CATEGORY_LIMIT = 6;
const ROTATION_MIN_INTERVALS = 4;
const ROTATION_REPORT_DEFINITIONS = [
  {
    cutoffMinutes: MORNING_SESSION_END_MINUTES,
    key: 'midday',
    label: '午盘热点迁移',
  },
  {
    cutoffMinutes: AFTERNOON_SESSION_END_MINUTES,
    key: 'close',
    label: '收盘热点迁移',
  },
];
const SUPPORTED_CODE_PATTERN = /^(60|00)/;
const ST_NAME_PATTERN = /(?:^|\b)\*?ST/i;
const SESSION_START_MINUTES = new Set([
  MORNING_SESSION_START_MINUTES,
  AFTERNOON_SESSION_START_MINUTES,
]);
const TRADING_BOUNDARY_MINUTES = buildTradingBoundaryMinutes();

const REQUEST_HEADERS = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'Accept-Language': 'zh-CN,zh;q=0.9',
  'Cache-Control': 'no-cache',
  'Pragma': 'no-cache',
  'Referer': SOURCE_URL,
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
};

const state = {
  comparisonEndedAt: null,
  comparisonMode: '5m',
  comparisonModeLabel: '当前 5 分钟榜',
  comparisonReady: false,
  comparisonStartedAt: null,
  coverageCount: 0,
  error: null,
  fallers: [],
  groups: [],
  history: [],
  historyDay: null,
  intervalMs: FETCH_INTERVAL_MS,
  lastAttemptAt: null,
  lastSuccessAt: null,
  leaders: [],
  nextRefreshAt: null,
  rotationInsights: [],
  rotationReports: [],
  sourceUrl: SOURCE_URL,
  status: 'idle',
  summary: [],
  windowMinutes: null,
};

const internalState = {
  conceptCache: {},
  historyDay: null,
  slotSnapshots: [],
};

let pollingStarted = false;
let inFlightRefresh = null;
let cacheLoaded = false;
let cacheLoadPromise = null;
let nextRefreshTimer = null;

function buildTradingBoundaryMinutes() {
  const minutes = [];

  for (let current = MORNING_SESSION_START_MINUTES; current <= MORNING_SESSION_END_MINUTES; current += 5) {
    minutes.push(current);
  }

  for (let current = AFTERNOON_SESSION_START_MINUTES; current <= AFTERNOON_SESSION_END_MINUTES; current += 5) {
    minutes.push(current);
  }

  return minutes;
}

function cloneHistoryEntry(entry) {
  return {
    comparisonEndedAt: entry.comparisonEndedAt,
    comparisonStartedAt: entry.comparisonStartedAt,
    fallers: (entry.fallers || []).map((leader) => ({ ...leader })),
    leaders: (entry.leaders || []).map((leader) => ({ ...leader })),
    themeScores: (entry.themeScores || []).map((score) => ({ ...score })),
    topFaller: entry.topFaller ? { ...entry.topFaller } : null,
    topLeader: entry.topLeader ? { ...entry.topLeader } : null,
    windowMinutes: entry.windowMinutes,
  };
}

function cloneRotationReport(report) {
  return {
    artifacts: report.artifacts ? { ...report.artifacts } : null,
    categories: (report.categories || []).map((category) => ({ ...category })),
    dominantTheme: report.dominantTheme,
    generatedAt: report.generatedAt,
    headline: report.headline,
    intervalCount: report.intervalCount,
    label: report.label,
    series: (report.series || []).map((item) => ({
      ...item,
      categoryScores: (item.categoryScores || []).map((score) => ({ ...score })),
    })),
    sessionKey: report.sessionKey,
    summaryLines: [...(report.summaryLines || [])],
    svgMarkup: report.svgMarkup || '',
    windowEndedAt: report.windowEndedAt,
    windowStartedAt: report.windowStartedAt,
  };
}

function cloneState() {
  return {
    comparisonEndedAt: state.comparisonEndedAt,
    comparisonMode: state.comparisonMode,
    comparisonModeLabel: state.comparisonModeLabel,
    comparisonReady: state.comparisonReady,
    comparisonStartedAt: state.comparisonStartedAt,
    coverageCount: state.coverageCount,
    error: state.error,
    fallers: state.fallers.map((faller) => ({
      ...faller,
      concepts: Array.isArray(faller.concepts) ? [...faller.concepts] : [],
    })),
    groups: state.groups.map((group) => ({
      ...group,
      stocks: group.stocks.map((stock) => ({ ...stock })),
    })),
    history: state.history.map(cloneHistoryEntry),
    historyDay: state.historyDay,
    intervalMs: state.intervalMs,
    lastAttemptAt: state.lastAttemptAt,
    lastSuccessAt: state.lastSuccessAt,
    leaders: state.leaders.map((leader) => ({
      ...leader,
      concepts: Array.isArray(leader.concepts) ? [...leader.concepts] : [],
    })),
    nextRefreshAt: state.nextRefreshAt,
    rotationInsights: [...state.rotationInsights],
    rotationReports: state.rotationReports.map(cloneRotationReport),
    sourceUrl: state.sourceUrl,
    status: state.status,
    summary: [...state.summary],
    windowMinutes: state.windowMinutes,
  };
}

function pad2(value) {
  return String(value).padStart(2, '0');
}

function formatTradingDay(date) {
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;
}

function secondsSinceMidnight(date) {
  return (date.getHours() * 60 * 60) + (date.getMinutes() * 60) + date.getSeconds();
}

function isWeekend(date) {
  const day = date.getDay();
  return day === 0 || day === 6;
}

function createDateAtMinutes(referenceDate, totalMinutes) {
  const date = new Date(referenceDate);
  date.setHours(Math.floor(totalMinutes / 60), totalMinutes % 60, 0, 0);
  return date;
}

function buildBoundaryDatesForDay(referenceDate) {
  if (isWeekend(referenceDate)) {
    return [];
  }

  return TRADING_BOUNDARY_MINUTES.map((minutes) => createDateAtMinutes(referenceDate, minutes));
}

function getNextWeekdayMorning(referenceDate) {
  const next = createDateAtMinutes(referenceDate, MORNING_SESSION_START_MINUTES);

  do {
    next.setDate(next.getDate() + 1);
  } while (isWeekend(next));

  return next;
}

function getNextScheduledBoundary(referenceDate = new Date()) {
  const day = new Date(referenceDate);
  day.setHours(0, 0, 0, 0);

  for (let offset = 0; offset < 10; offset += 1) {
    if (!isWeekend(day)) {
      const boundaries = buildBoundaryDatesForDay(day);

      for (const boundary of boundaries) {
        if (boundary.getTime() > referenceDate.getTime()) {
          return boundary;
        }
      }
    }

    day.setDate(day.getDate() + 1);
  }

  return getNextWeekdayMorning(referenceDate);
}

function getLatestScheduledBoundary(referenceDate = new Date()) {
  if (isWeekend(referenceDate)) {
    return null;
  }

  const day = new Date(referenceDate);
  day.setHours(0, 0, 0, 0);
  const boundaries = buildBoundaryDatesForDay(day);

  for (let index = boundaries.length - 1; index >= 0; index -= 1) {
    if (boundaries[index].getTime() <= referenceDate.getTime()) {
      return boundaries[index];
    }
  }

  return null;
}

function isWithinSession(date, startMinutes, endMinutes) {
  const seconds = secondsSinceMidnight(date);
  return seconds >= startMinutes * 60 && seconds <= endMinutes * 60;
}

function isTradingSessionOpen(date = new Date()) {
  if (isWeekend(date)) {
    return false;
  }

  return isWithinSession(date, MORNING_SESSION_START_MINUTES, MORNING_SESSION_END_MINUTES)
    || isWithinSession(date, AFTERNOON_SESSION_START_MINUTES, AFTERNOON_SESSION_END_MINUTES);
}

function isSessionStartBoundary(boundaryDate) {
  if (!boundaryDate) {
    return false;
  }

  const totalMinutes = boundaryDate.getHours() * 60 + boundaryDate.getMinutes();
  return SESSION_START_MINUTES.has(totalMinutes);
}

function shouldDisplayFifteenMinuteLeaders(boundaryDate) {
  if (!boundaryDate || isSessionStartBoundary(boundaryDate)) {
    return false;
  }

  return boundaryDate.getMinutes() % DISPLAY_SWITCH_INTERVAL_MINUTES === 0;
}

function getRecentBoundaryWithinGrace(referenceDate = new Date()) {
  const boundary = getLatestScheduledBoundary(referenceDate);

  if (!boundary) {
    return null;
  }

  const delta = referenceDate.getTime() - boundary.getTime();
  return delta >= 0 && delta <= HYDRATE_BOUNDARY_GRACE_MS ? boundary : null;
}

function syncSchedule(referenceDate = new Date()) {
  state.intervalMs = FETCH_INTERVAL_MS;
  state.nextRefreshAt = getNextScheduledBoundary(referenceDate).toISOString();
}

function getComparisonModeLabel(windowMinutes) {
  return windowMinutes === 15 ? '过去 15 分钟榜' : '当前 5 分钟榜';
}

function roundNumber(value, digits = 3) {
  if (!Number.isFinite(value)) {
    return null;
  }

  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function averageNumbers(values) {
  const usable = values.filter((value) => Number.isFinite(value));
  if (usable.length === 0) {
    return null;
  }

  return usable.reduce((sum, value) => sum + value, 0) / usable.length;
}

function parseNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatVolumeLabel(value) {
  if (!Number.isFinite(value) || value <= 0) {
    return '0';
  }

  if (value >= 1e8) {
    return `${roundNumber(value / 1e8, 2)}亿`;
  }

  if (value >= 1e4) {
    return `${roundNumber(value / 1e4, 2)}万`;
  }

  return String(Math.round(value));
}

function resetCurrentDisplayState() {
  state.comparisonEndedAt = null;
  state.comparisonMode = '5m';
  state.comparisonModeLabel = '当前 5 分钟榜';
  state.comparisonReady = false;
  state.comparisonStartedAt = null;
  state.coverageCount = 0;
  state.fallers = [];
  state.groups = [];
  state.leaders = [];
  state.summary = [];
  state.windowMinutes = null;
}

function resetDailyMarketState(dayKey) {
  resetCurrentDisplayState();
  state.error = null;
  state.history = [];
  state.historyDay = dayKey;
  state.lastAttemptAt = null;
  state.lastSuccessAt = null;
  state.rotationInsights = [];
  state.rotationReports = [];
  state.status = 'idle';
  internalState.historyDay = dayKey;
  internalState.slotSnapshots = [];
}

function ensureTradingDayState(referenceDate = new Date()) {
  const dayKey = formatTradingDay(referenceDate);

  if (state.historyDay !== dayKey || internalState.historyDay !== dayKey) {
    resetDailyMarketState(dayKey);
  }
}

function inferBoardLabel(code) {
  if (/^60/.test(code)) {
    return '沪市主板';
  }

  if (/^00/.test(code)) {
    return '深市主板';
  }

  return '其他板块';
}

function isEligibleStockName(name = '') {
  return !ST_NAME_PATTERN.test(String(name || '').trim());
}

function normalizeRankStock(rawItem) {
  const code = String(rawItem['5'] || '').trim();
  const name = String(rawItem['55'] || '').trim();
  const price = parseNumber(rawItem['10']);
  const dailyChangePercent = parseNumber(rawItem['199112']);
  const turnoverRate = parseNumber(rawItem['1968584']);
  const volume = parseNumber(rawItem['13']);

  if (!code || !name || !Number.isFinite(price) || !SUPPORTED_CODE_PATTERN.test(code) || !isEligibleStockName(name)) {
    return null;
  }

  return {
    board: inferBoardLabel(code),
    code,
    dailyChangePercent: roundNumber(dailyChangePercent ?? 0, 3),
    name,
    price: roundNumber(price, 3),
    turnoverRate: roundNumber(turnoverRate ?? 0, 3),
    volume: volume ?? 0,
    volumeLabel: formatVolumeLabel(volume ?? 0),
  };
}

function parseRankPayload(rawText) {
  const text = String(rawText || '').trim();
  const start = text.indexOf('(');
  const end = text.lastIndexOf(')');

  if (start === -1 || end === -1 || end <= start + 1) {
    throw new Error('同花顺排行接口返回格式异常');
  }

  const items = JSON.parse(text.slice(start + 1, end));

  if (!Array.isArray(items)) {
    throw new Error('同花顺排行接口没有返回股票数组');
  }

  return items
    .map(normalizeRankStock)
    .filter(Boolean);
}

async function fetchText(url) {
  const response = await fetch(url, {
    headers: REQUEST_HEADERS,
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

async function fetchRankList(direction) {
  const url = `${RANK_API_BASE}/${direction}${RANK_BATCH_SIZE}.js`;
  const text = await fetchText(url);
  return parseRankPayload(text);
}

async function fetchMarketSnapshot() {
  const [descending, ascending] = await Promise.all([
    fetchRankList('d'),
    fetchRankList('a'),
  ]);

  const merged = new Map();

  descending.forEach((stock) => {
    merged.set(stock.code, stock);
  });

  ascending.forEach((stock) => {
    merged.set(stock.code, stock);
  });

  return {
    capturedAt: new Date().toISOString(),
    stocks: [...merged.values()],
  };
}

function decodeHtmlEntities(value) {
  return String(value || '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .trim();
}

function decodeConceptHtml(buffer) {
  try {
    return new TextDecoder('gbk').decode(buffer);
  } catch (error) {
    return Buffer.from(buffer).toString('utf8');
  }
}

async function fetchStockConcepts(code) {
  const response = await fetch(`${CONCEPT_PAGE_BASE}/${code}/concept.html`, {
    headers: REQUEST_HEADERS,
    redirect: 'follow',
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });

  const buffer = await response.arrayBuffer();

  if (!response.ok) {
    throw new Error(`概念页请求失败，HTTP ${response.status}`);
  }

  const html = decodeConceptHtml(buffer);
  const concepts = [...html.matchAll(/<a[^>]+href="javascript:void\(0\);"[^>]*>([^<]+)<\/a>/g)]
    .map((match) => decodeHtmlEntities(match[1]))
    .filter(Boolean)
    .filter((value) => !['展开', '全部展开', '添加概念', '编辑概念'].includes(value));

  return [...new Set(concepts)].slice(0, MAX_CONCEPTS_PER_STOCK);
}

function pruneConceptCache() {
  const now = Date.now();
  const entries = Object.entries(internalState.conceptCache)
    .filter(([, item]) => item && now - Date.parse(item.updatedAt || 0) <= CONCEPT_CACHE_TTL_MS)
    .sort((left, right) => Date.parse(right[1].updatedAt || 0) - Date.parse(left[1].updatedAt || 0))
    .slice(0, MAX_CONCEPT_CACHE_ITEMS);

  internalState.conceptCache = Object.fromEntries(entries);
}

async function getStockConcepts(code) {
  const cached = internalState.conceptCache[code];
  const cachedAt = cached?.updatedAt ? Date.parse(cached.updatedAt) : 0;

  if (cached && Date.now() - cachedAt <= CONCEPT_CACHE_TTL_MS) {
    return cached.concepts;
  }

  const concepts = await fetchStockConcepts(code);
  internalState.conceptCache[code] = {
    concepts,
    updatedAt: new Date().toISOString(),
  };
  pruneConceptCache();
  return concepts;
}

async function enrichLeaders(leaders) {
  const enriched = [];

  for (const leader of leaders) {
    try {
      const concepts = await getStockConcepts(leader.code);
      enriched.push({
        ...leader,
        concepts,
        primaryCategory: concepts[0] || leader.board,
      });
    } catch (error) {
      enriched.push({
        ...leader,
        concepts: [],
        primaryCategory: leader.board,
      });
    }
  }

  return enriched;
}

function buildLeaderGroups(leaders) {
  const conceptGroups = new Map();

  leaders.forEach((leader) => {
    leader.concepts.slice(0, 3).forEach((concept) => {
      if (!conceptGroups.has(concept)) {
        conceptGroups.set(concept, {
          label: concept,
          stocks: [],
        });
      }

      conceptGroups.get(concept).stocks.push({
        code: leader.code,
        name: leader.name,
        windowChangePercent: leader.windowChangePercent,
      });
    });
  });

  const repeatedConceptGroups = [...conceptGroups.values()]
    .filter((group) => group.stocks.length >= 2)
    .sort((left, right) => {
      if (right.stocks.length !== left.stocks.length) {
        return right.stocks.length - left.stocks.length;
      }

      return right.stocks[0].windowChangePercent - left.stocks[0].windowChangePercent;
    })
    .slice(0, MAX_GROUPS)
    .map((group) => ({
      count: group.stocks.length,
      label: group.label,
      stocks: group.stocks
        .sort((left, right) => right.windowChangePercent - left.windowChangePercent)
        .map((stock) => ({
          code: stock.code,
          name: stock.name,
          windowChangePercent: stock.windowChangePercent,
        })),
    }));

  const assignedCodes = new Set(
    repeatedConceptGroups.flatMap((group) => group.stocks.map((stock) => stock.code)),
  );

  const boardGroups = new Map();

  leaders
    .filter((leader) => !assignedCodes.has(leader.code))
    .forEach((leader) => {
      if (!boardGroups.has(leader.board)) {
        boardGroups.set(leader.board, []);
      }

      boardGroups.get(leader.board).push({
        code: leader.code,
        name: leader.name,
        windowChangePercent: leader.windowChangePercent,
      });
    });

  const fallbackGroups = [...boardGroups.entries()].map(([label, stocks]) => ({
    count: stocks.length,
    label,
    stocks: stocks.sort((left, right) => right.windowChangePercent - left.windowChangePercent),
  }));

  return [...repeatedConceptGroups, ...fallbackGroups].slice(0, MAX_GROUPS);
}

function buildSnapshotCache(stocks) {
  return Object.fromEntries(
    stocks.map((stock) => [stock.code, {
      price: stock.price,
    }]),
  );
}

function pruneSlotSnapshots() {
  internalState.slotSnapshots = internalState.slotSnapshots
    .sort((left, right) => Date.parse(left.slotEndedAt) - Date.parse(right.slotEndedAt))
    .slice(-SLOT_SNAPSHOT_LIMIT);
}

function findSlotSnapshot(slotEndedAt) {
  return internalState.slotSnapshots.find((item) => item.slotEndedAt === slotEndedAt) || null;
}

function upsertSlotSnapshot(snapshot) {
  internalState.slotSnapshots = internalState.slotSnapshots.filter(
    (item) => item.slotEndedAt !== snapshot.slotEndedAt,
  );

  internalState.slotSnapshots.push({
    capturedAt: snapshot.capturedAt,
    coverageCount: snapshot.stocks.length,
    slotEndedAt: snapshot.slotEndedAt,
    universeSnapshot: buildSnapshotCache(snapshot.stocks),
  });

  pruneSlotSnapshots();
}

function buildIntervalDetails(currentSnapshot, lookbackMinutes) {
  const referenceSlotDate = new Date(Date.parse(currentSnapshot.slotEndedAt) - (lookbackMinutes * 60 * 1000));
  const referenceSnapshot = findSlotSnapshot(referenceSlotDate.toISOString());

  if (!referenceSnapshot) {
    return {
      comparisonEndedAt: currentSnapshot.slotEndedAt,
      comparisonReady: false,
      comparisonReason: 'missing_reference_snapshot',
      comparisonStartedAt: referenceSlotDate.toISOString(),
      coverageCount: currentSnapshot.stocks.length,
      fallers: [],
      leaders: [],
      windowMinutes: lookbackMinutes,
    };
  }

  const rankedStocks = currentSnapshot.stocks
    .map((stock) => {
      const previous = referenceSnapshot.universeSnapshot[stock.code];

      if (!previous || !Number.isFinite(previous.price) || previous.price <= 0) {
        return null;
      }

      const windowChangePercent = ((stock.price - previous.price) / previous.price) * 100;

      return {
        ...stock,
        concepts: [],
        previousPrice: previous.price,
        primaryCategory: stock.board,
        windowChangePercent: roundNumber(windowChangePercent, 3),
      };
    })
    .filter(Boolean)
    .sort((left, right) => {
      if (right.windowChangePercent !== left.windowChangePercent) {
        return right.windowChangePercent - left.windowChangePercent;
      }

      return right.dailyChangePercent - left.dailyChangePercent;
    });
  const leaders = rankedStocks
    .slice(0, TOP_STOCK_COUNT);
  const fallers = [...rankedStocks]
    .sort((left, right) => {
      if (left.windowChangePercent !== right.windowChangePercent) {
        return left.windowChangePercent - right.windowChangePercent;
      }

      return left.dailyChangePercent - right.dailyChangePercent;
    })
    .slice(0, 5);

  return {
    comparisonEndedAt: currentSnapshot.slotEndedAt,
    comparisonReady: true,
    comparisonReason: 'ready',
    comparisonStartedAt: referenceSnapshot.slotEndedAt,
    coverageCount: currentSnapshot.stocks.length,
    fallers,
    leaders,
    windowMinutes: lookbackMinutes,
  };
}

function getLeaderThemeLabel(leader) {
  if (leader?.primaryCategory) {
    return leader.primaryCategory;
  }

  if (Array.isArray(leader?.concepts) && leader.concepts.length > 0) {
    return leader.concepts[0];
  }

  return leader?.board || '其他';
}

function buildIntervalThemeScores(leaders = [], fallers = []) {
  const categoryMap = new Map();

  const ensureCategory = (label) => {
    if (!categoryMap.has(label)) {
      categoryMap.set(label, {
        fallerCount: 0,
        fallerPressure: 0,
        label,
        leaderCount: 0,
        leaderStrength: 0,
        momentumScore: 0,
        netScore: 0,
      });
    }

    return categoryMap.get(label);
  };

  leaders.forEach((leader, index) => {
    const label = getLeaderThemeLabel(leader);
    const item = ensureCategory(label);
    const strength = Math.max(Number(leader.windowChangePercent) || 0, 0);
    const rankBonus = Math.max(TOP_STOCK_COUNT - index, 1) * 0.08;
    item.leaderCount += 1;
    item.leaderStrength += strength;
    item.momentumScore += strength + rankBonus;
  });

  fallers.forEach((leader, index) => {
    const label = getLeaderThemeLabel(leader);
    const item = ensureCategory(label);
    const pressure = Math.abs(Math.min(Number(leader.windowChangePercent) || 0, 0));
    const rankBonus = Math.max(5 - index, 1) * 0.06;
    item.fallerCount += 1;
    item.fallerPressure += pressure;
    item.momentumScore -= pressure + rankBonus;
  });

  return [...categoryMap.values()]
    .map((item) => ({
      ...item,
      fallerPressure: roundNumber(item.fallerPressure, 3),
      leaderStrength: roundNumber(item.leaderStrength, 3),
      momentumScore: roundNumber(item.momentumScore, 3),
      netScore: roundNumber(item.leaderStrength - item.fallerPressure, 3),
    }))
    .sort((left, right) => {
      if ((right.momentumScore || 0) !== (left.momentumScore || 0)) {
        return (right.momentumScore || 0) - (left.momentumScore || 0);
      }

      return (right.netScore || 0) - (left.netScore || 0);
    });
}

function mapLeaderForState(leader, index) {
  return {
    board: leader.board,
    code: leader.code,
    concepts: Array.isArray(leader.concepts) ? leader.concepts : [],
    dailyChangePercent: leader.dailyChangePercent,
    name: leader.name,
    previousPrice: leader.previousPrice,
    price: leader.price,
    primaryCategory: leader.primaryCategory,
    rank: index + 1,
    turnoverRate: leader.turnoverRate,
    volume: leader.volume,
    volumeLabel: leader.volumeLabel,
    windowChangePercent: leader.windowChangePercent,
  };
}

function mapHistoryLeader(leader, index) {
  return {
    board: leader.board,
    code: leader.code,
    concepts: Array.isArray(leader.concepts) ? leader.concepts.slice(0, 3) : [],
    name: leader.name,
    primaryCategory: leader.primaryCategory,
    rank: index + 1,
    windowChangePercent: leader.windowChangePercent,
  };
}

function buildHistoryEntry(details, leaders, fallers) {
  return {
    comparisonEndedAt: details.comparisonEndedAt,
    comparisonStartedAt: details.comparisonStartedAt,
    fallers: fallers.map(mapHistoryLeader),
    leaders: leaders.map(mapHistoryLeader),
    themeScores: buildIntervalThemeScores(leaders, fallers),
    topFaller: fallers.length > 0 ? mapHistoryLeader(fallers[0], 0) : null,
    topLeader: leaders.length > 0 ? mapHistoryLeader(leaders[0], 0) : null,
    windowMinutes: details.windowMinutes,
  };
}

function upsertHistoryEntry(entry) {
  state.history = state.history.filter((item) => item.comparisonEndedAt !== entry.comparisonEndedAt);
  state.history.push(entry);
  state.history.sort((left, right) => Date.parse(right.comparisonEndedAt) - Date.parse(left.comparisonEndedAt));
  state.history = state.history.slice(0, DAILY_HISTORY_LIMIT);
}

function containsUnsupportedLeaderCodes(leaders) {
  return leaders.some((leader) => !SUPPORTED_CODE_PATTERN.test(String(leader.code || '')));
}

function sanitizeRankedStocks(stocks) {
  return (Array.isArray(stocks) ? stocks : [])
    .filter((stock) => stock
      && SUPPORTED_CODE_PATTERN.test(String(stock.code || ''))
      && isEligibleStockName(stock.name))
    .map((stock, index) => ({
      ...stock,
      rank: index + 1,
    }));
}

function sanitizeGroupEntries(groups) {
  return (Array.isArray(groups) ? groups : [])
    .map((group) => ({
      ...group,
      count: sanitizeRankedStocks(group.stocks).length,
      stocks: sanitizeRankedStocks(group.stocks),
    }))
    .filter((group) => group.stocks.length > 0);
}

function sanitizeThemeScores(scores) {
  return (Array.isArray(scores) ? scores : [])
    .filter((score) => score && score.label)
    .map((score) => ({
      fallerCount: Number(score.fallerCount) || 0,
      fallerPressure: roundNumber(Number(score.fallerPressure) || 0, 3),
      label: String(score.label),
      leaderCount: Number(score.leaderCount) || 0,
      leaderStrength: roundNumber(Number(score.leaderStrength) || 0, 3),
      momentumScore: roundNumber(Number(score.momentumScore) || 0, 3),
      netScore: roundNumber(Number(score.netScore) || 0, 3),
    }));
}

function sanitizeHistoryEntries(entries) {
  return (Array.isArray(entries) ? entries : [])
    .map((entry) => {
      const leaders = sanitizeRankedStocks(entry.leaders);
      const fallers = sanitizeRankedStocks(entry.fallers);
      return {
        ...entry,
        fallers,
        leaders,
        themeScores: sanitizeThemeScores(entry.themeScores).length > 0
          ? sanitizeThemeScores(entry.themeScores)
          : buildIntervalThemeScores(leaders, fallers),
        topFaller: fallers.length > 0 ? { ...fallers[0] } : null,
        topLeader: leaders.length > 0 ? { ...leaders[0] } : null,
      };
    })
    .filter((entry) => entry.leaders.length > 0);
}

function sanitizeRotationReports(reports) {
  return (Array.isArray(reports) ? reports : [])
    .filter((report) => report && report.sessionKey && report.label)
    .map((report) => ({
      artifacts: report.artifacts ? { ...report.artifacts } : null,
      categories: (Array.isArray(report.categories) ? report.categories : [])
        .filter((category) => category && category.label)
        .map((category) => ({
          activityScore: roundNumber(Number(category.activityScore) || 0, 3),
          averageNetScore: roundNumber(Number(category.averageNetScore) || 0, 3),
          label: String(category.label),
          totalFallerPressure: roundNumber(Number(category.totalFallerPressure) || 0, 3),
          totalLeaderStrength: roundNumber(Number(category.totalLeaderStrength) || 0, 3),
          totalNetScore: roundNumber(Number(category.totalNetScore) || 0, 3),
        })),
      dominantTheme: report.dominantTheme || null,
      generatedAt: report.generatedAt || null,
      headline: report.headline || '',
      intervalCount: Number(report.intervalCount) || 0,
      label: String(report.label),
      series: (Array.isArray(report.series) ? report.series : []).map((item) => ({
        categoryScores: (Array.isArray(item.categoryScores) ? item.categoryScores : [])
          .filter((score) => score && score.label)
          .map((score) => ({
            label: String(score.label),
            netScore: roundNumber(Number(score.netScore) || 0, 3),
          })),
        comparisonEndedAt: item.comparisonEndedAt || null,
        comparisonStartedAt: item.comparisonStartedAt || null,
        dominantCategory: item.dominantCategory || null,
        dominantScore: roundNumber(Number(item.dominantScore) || 0, 3),
      })),
      sessionKey: String(report.sessionKey),
      summaryLines: (Array.isArray(report.summaryLines) ? report.summaryLines : []).map((line) => String(line)),
      svgMarkup: report.svgMarkup || '',
      windowEndedAt: report.windowEndedAt || null,
      windowStartedAt: report.windowStartedAt || null,
    }));
}

function sanitizeLoadedState() {
  state.fallers = sanitizeRankedStocks(state.fallers);
  state.groups = sanitizeGroupEntries(state.groups);
  state.history = sanitizeHistoryEntries(state.history);
  internalState.slotSnapshots = Array.isArray(internalState.slotSnapshots)
    ? internalState.slotSnapshots.filter((item) => item && item.slotEndedAt && item.universeSnapshot)
    : [];
  state.leaders = sanitizeRankedStocks(state.leaders);
  state.rotationReports = sanitizeRotationReports(state.rotationReports);
  state.rotationInsights = (Array.isArray(state.rotationInsights) ? state.rotationInsights : []).map((line) => String(line));

  if (containsUnsupportedLeaderCodes(state.leaders)
    || containsUnsupportedLeaderCodes(state.fallers)
    || state.history.some((entry) => containsUnsupportedLeaderCodes(entry.leaders || []))) {
    resetDailyMarketState(formatTradingDay(new Date()));
    return;
  }

  pruneSlotSnapshots();
}

function buildOutOfSessionSummary(hasDisplayResult, historyCount) {
  return [
    `${MARKET_SCOPE_LABEL}榜单只统计 ${SUPPORTED_CODE_LABEL} 开头股票，自动统计时段为 ${TRADING_SESSIONS_LABEL}。`,
    hasDisplayResult
      ? '当前处于非交易时段，页面展示的是最近一次有效主榜结果。'
      : '当前处于非交易时段，进入下一交易时段后会先建立新的 5 分钟基准快照。',
    `今日已保存 ${historyCount} 个 5 分钟区间榜单。`,
  ];
}

function buildInSessionWaitingSummary(historyCount) {
  return [
    `${MARKET_SCOPE_LABEL}榜单只统计 ${SUPPORTED_CODE_LABEL} 开头股票，自动统计时段为 ${TRADING_SESSIONS_LABEL}。`,
    '当前正在等待新的 5 分钟整点快照，生成基准后才会出现区间涨幅前十。',
    `今日已保存 ${historyCount} 个 5 分钟区间榜单。`,
  ];
}

function buildSummary(details, groups, historyCount, fiveMinuteFallers = []) {
  if (!details.comparisonReady) {
    return [
      `${MARKET_SCOPE_LABEL}榜单只统计 ${SUPPORTED_CODE_LABEL} 开头股票，自动统计时段为 ${TRADING_SESSIONS_LABEL}。`,
      details.windowMinutes === 15
        ? '当前 15 分钟节点缺少完整基准，系统会在下一个有效节点重新生成主榜。'
        : '当前快照仅用于建立新的 5 分钟基准，下一次有效快照后会生成区间涨幅前十。',
      `今日已保存 ${historyCount} 个 5 分钟区间榜单。`,
    ];
  }

  const lines = [];
  lines.push(`当前主榜：${getComparisonModeLabel(details.windowMinutes)}。整 15 分钟节点显示过去 15 分钟榜，其余节点显示当前 5 分钟榜。`);
  lines.push(`本轮覆盖了${MARKET_SCOPE_LABEL}${details.coverageCount} 只股票，今日已保存 ${historyCount} 个 5 分钟区间榜单。`);

  if (groups.length > 0) {
    const headlineGroups = groups
      .slice(0, 3)
      .map((group) => `${group.label}${group.count}只`)
      .join('，');
    lines.push(`涨幅前十更集中在这些分类：${headlineGroups}。`);
  } else {
    lines.push('涨幅前十没有形成明显的重复概念，分类上更分散。');
  }

  if (details.leaders.length > 0) {
    const topNames = details.leaders
      .slice(0, 3)
      .map((leader) => `${leader.name}${leader.windowChangePercent}%`)
      .join('，');
    lines.push(`当前主榜领涨前三分别是：${topNames}。`);
  }

  if (fiveMinuteFallers.length > 0) {
    const fastestDropNames = fiveMinuteFallers
      .slice(0, 3)
      .map((leader) => `${leader.name}${leader.windowChangePercent}%`)
      .join('，');
    lines.push(`当前 5 分钟回落最快的股票主要是：${fastestDropNames}。`);
  }

  lines.push('股票分类优先使用同花顺概念题材；若概念提取失败，则回退到主板分类。');
  return lines;
}

function appendRotationInsightSummary(lines, rotationInsights = []) {
  const nextLines = Array.isArray(lines) ? [...lines] : [];
  if (!Array.isArray(rotationInsights) || rotationInsights.length === 0) {
    return nextLines;
  }

  if (nextLines.some((line) => String(line).startsWith('热点迁移报告：'))) {
    return nextLines;
  }

  nextLines.push(`热点迁移报告：${rotationInsights.join('；')}`);
  return nextLines;
}

function getEntryThemeScores(entry) {
  if (Array.isArray(entry.themeScores) && entry.themeScores.length > 0) {
    return entry.themeScores;
  }

  return buildIntervalThemeScores(entry.leaders || [], entry.fallers || []);
}

function escapeSvgText(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function truncateLabel(value, maxLength = 8) {
  const text = String(value || '');
  return text.length > maxLength ? `${text.slice(0, maxLength)}…` : text;
}

function blendColor(start, end, ratio) {
  const clampRatio = Math.max(0, Math.min(1, ratio));
  const channels = start.map((channel, index) => Math.round(channel + ((end[index] - channel) * clampRatio)));
  return `rgb(${channels.join(',')})`;
}

function getRotationHeatColor(score, maxAbsScore) {
  if (!Number.isFinite(score) || maxAbsScore <= 0) {
    return 'rgba(255, 245, 239, 0.9)';
  }

  const intensity = Math.min(Math.abs(score) / maxAbsScore, 1);
  if (score >= 0) {
    return blendColor([252, 236, 230], [181, 58, 36], intensity);
  }

  return blendColor([232, 244, 236], [39, 126, 76], intensity);
}

function buildRotationSvg(report) {
  const categories = report.categories || [];
  const series = report.series || [];
  if (categories.length === 0 || series.length === 0) {
    return '';
  }

  const cellWidth = 58;
  const cellHeight = 34;
  const leftMargin = 138;
  const topMargin = 72;
  const footerHeight = 54;
  const width = leftMargin + (series.length * cellWidth) + 28;
  const height = topMargin + (categories.length * cellHeight) + footerHeight;
  const maxAbsScore = Math.max(
    ...series.flatMap((item) => (item.categoryScores || []).map((score) => Math.abs(score.netScore || 0))),
    0.5,
  );

  const cells = [];
  const xLabels = [];
  const yLabels = [];

  categories.forEach((category, rowIndex) => {
    const y = topMargin + (rowIndex * cellHeight);
    yLabels.push(`
      <text x="${leftMargin - 12}" y="${y + 21}" font-size="13" text-anchor="end" fill="#6e6157">${escapeSvgText(truncateLabel(category.label, 10))}</text>
    `);

    series.forEach((item, columnIndex) => {
      const x = leftMargin + (columnIndex * cellWidth);
      const score = (item.categoryScores || []).find((entry) => entry.label === category.label)?.netScore || 0;
      cells.push(`
        <rect x="${x}" y="${y}" width="${cellWidth - 8}" height="${cellHeight - 6}" rx="9" fill="${getRotationHeatColor(score, maxAbsScore)}" opacity="0.94" />
      `);

      if (rowIndex === 0) {
        xLabels.push(`
          <text x="${x + ((cellWidth - 8) / 2)}" y="${topMargin - 12}" font-size="11" text-anchor="middle" fill="#6e6157">${escapeSvgText(new Date(item.comparisonEndedAt).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', hour12: false }))}</text>
        `);
      }
    });
  });

  return [
    `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeSvgText(report.label)}">`,
    '<defs>',
    '  <linearGradient id="rotationBg" x1="0%" x2="100%" y1="0%" y2="100%">',
    '    <stop offset="0%" stop-color="#fff8f2" />',
    '    <stop offset="100%" stop-color="#f3e7d8" />',
    '  </linearGradient>',
    '</defs>',
    `<rect x="0" y="0" width="${width}" height="${height}" rx="26" fill="url(#rotationBg)" stroke="rgba(82, 52, 32, 0.12)" />`,
    `<text x="${leftMargin}" y="30" font-size="22" font-family="'Source Han Serif SC','Songti SC',serif" fill="#22160f">${escapeSvgText(report.label)}</text>`,
    `<text x="${leftMargin}" y="52" font-size="12" fill="#6e6157">${escapeSvgText(report.windowStartedAt)} - ${escapeSvgText(report.windowEndedAt)} · ${report.intervalCount} 个 5 分钟区间</text>`,
    cells.join(''),
    xLabels.join(''),
    yLabels.join(''),
    `<text x="${leftMargin}" y="${height - 18}" font-size="12" fill="#b33a24">红色=升温，绿色=退潮，颜色越深代表净强度越大</text>`,
    '</svg>',
  ].join('');
}

function buildRotationReportHeadline(label, dominantTheme, transition, streak) {
  if (dominantTheme && transition) {
    return `${label}以 ${dominantTheme} 为核心，主切换在 ${transition.from} -> ${transition.to}。`;
  }

  if (dominantTheme && streak) {
    return `${label}最强主线是 ${dominantTheme}，并且持续性最好。`;
  }

  return `${label}已生成。`;
}

function buildRotationReport(entries, definition, historyDay) {
  const eligibleEntries = [...entries]
    .filter((entry) => {
      const endedAt = new Date(entry.comparisonEndedAt);
      const totalMinutes = (endedAt.getHours() * 60) + endedAt.getMinutes();
      return (entry.windowMinutes || 5) === 5 && totalMinutes <= definition.cutoffMinutes;
    })
    .sort((left, right) => Date.parse(left.comparisonEndedAt) - Date.parse(right.comparisonEndedAt));

  if (eligibleEntries.length < ROTATION_MIN_INTERVALS) {
    return null;
  }

  const categoryAggregate = new Map();
  const series = eligibleEntries.map((entry) => {
    const scoreMap = new Map(getEntryThemeScores(entry).map((score) => [score.label, score]));
    scoreMap.forEach((score, label) => {
      if (!categoryAggregate.has(label)) {
        categoryAggregate.set(label, {
          activityScore: 0,
          label,
          totalFallerPressure: 0,
          totalLeaderStrength: 0,
          totalNetScore: 0,
        });
      }

      const aggregate = categoryAggregate.get(label);
      aggregate.activityScore += (score.leaderStrength || 0) + (score.fallerPressure || 0);
      aggregate.totalFallerPressure += score.fallerPressure || 0;
      aggregate.totalLeaderStrength += score.leaderStrength || 0;
      aggregate.totalNetScore += score.netScore || 0;
    });

    const dominantScore = [...scoreMap.values()]
      .sort((left, right) => {
        if ((right.momentumScore || 0) !== (left.momentumScore || 0)) {
          return (right.momentumScore || 0) - (left.momentumScore || 0);
        }

        return (right.netScore || 0) - (left.netScore || 0);
      })[0] || null;

    return {
      comparisonEndedAt: entry.comparisonEndedAt,
      comparisonStartedAt: entry.comparisonStartedAt,
      dominantCategory: dominantScore?.label || null,
      dominantScore: roundNumber(dominantScore?.momentumScore || dominantScore?.netScore || 0, 3),
      scoreMap,
    };
  });

  const categories = [...categoryAggregate.values()]
    .map((item) => ({
      activityScore: roundNumber(item.activityScore, 3),
      averageNetScore: roundNumber(item.totalNetScore / eligibleEntries.length, 3),
      label: item.label,
      totalFallerPressure: roundNumber(item.totalFallerPressure, 3),
      totalLeaderStrength: roundNumber(item.totalLeaderStrength, 3),
      totalNetScore: roundNumber(item.totalNetScore, 3),
    }))
    .sort((left, right) => {
      if ((right.activityScore || 0) !== (left.activityScore || 0)) {
        return (right.activityScore || 0) - (left.activityScore || 0);
      }

      return (right.totalNetScore || 0) - (left.totalNetScore || 0);
    })
    .slice(0, ROTATION_CATEGORY_LIMIT);

  const topCategoryLabels = new Set(categories.map((item) => item.label));
  const normalizedSeries = series.map((item) => ({
    categoryScores: categories.map((category) => ({
      label: category.label,
      netScore: roundNumber(item.scoreMap.get(category.label)?.netScore || 0, 3),
    })),
    comparisonEndedAt: item.comparisonEndedAt,
    comparisonStartedAt: item.comparisonStartedAt,
    dominantCategory: topCategoryLabels.has(item.dominantCategory) ? item.dominantCategory : (item.dominantCategory || null),
    dominantScore: item.dominantScore,
  }));

  const dominantThemes = categories.slice(0, 3);
  let longestStreak = null;
  let currentStreak = null;
  const transitions = [];

  normalizedSeries.forEach((item, index) => {
    if (item.dominantCategory) {
      if (currentStreak && currentStreak.label === item.dominantCategory) {
        currentStreak.length += 1;
        currentStreak.endedAt = item.comparisonEndedAt;
      } else {
        currentStreak = {
          endedAt: item.comparisonEndedAt,
          label: item.dominantCategory,
          length: 1,
          startedAt: item.comparisonStartedAt,
        };
      }

      if (!longestStreak || currentStreak.length > longestStreak.length) {
        longestStreak = { ...currentStreak };
      }
    }

    if (index > 0) {
      const previous = normalizedSeries[index - 1];
      if (previous.dominantCategory && item.dominantCategory && previous.dominantCategory !== item.dominantCategory) {
        transitions.push({
          at: item.comparisonEndedAt,
          from: previous.dominantCategory,
          strength: roundNumber((previous.dominantScore || 0) + (item.dominantScore || 0), 3),
          to: item.dominantCategory,
        });
      }
    }
  });

  const strongestTransition = transitions
    .sort((left, right) => (right.strength || 0) - (left.strength || 0))[0] || null;

  const splitIndex = Math.max(1, Math.floor(normalizedSeries.length / 2));
  const firstHalf = normalizedSeries.slice(0, splitIndex);
  const secondHalf = normalizedSeries.slice(splitIndex);
  let risingTheme = null;
  let fadingTheme = null;

  categories.forEach((category) => {
    const firstAverage = averageNumbers(firstHalf.map((item) => item.categoryScores.find((score) => score.label === category.label)?.netScore || 0)) || 0;
    const secondAverage = averageNumbers(secondHalf.map((item) => item.categoryScores.find((score) => score.label === category.label)?.netScore || 0)) || 0;
    const delta = roundNumber(secondAverage - firstAverage, 3);
    const payload = {
      delta,
      label: category.label,
    };

    if (!risingTheme || delta > risingTheme.delta) {
      risingTheme = payload;
    }

    if (!fadingTheme || delta < fadingTheme.delta) {
      fadingTheme = payload;
    }
  });

  const summaryLines = [];
  if (dominantThemes.length > 0) {
    summaryLines.push(`${definition.label}最强的三条主线分别是：${dominantThemes.map((item) => `${item.label}(${item.averageNetScore})`).join('、')}。`);
  }
  if (longestStreak) {
    summaryLines.push(`持续性最强的是 ${longestStreak.label}，连续主导 ${longestStreak.length} 个区间，时间覆盖 ${longestStreak.startedAt} 到 ${longestStreak.endedAt}。`);
  }
  if (strongestTransition) {
    summaryLines.push(`最明显的热点切换发生在 ${strongestTransition.at} 前后：${strongestTransition.from} 切向 ${strongestTransition.to}。`);
  }
  if (risingTheme && risingTheme.delta > 0) {
    summaryLines.push(`后半段升温最快的是 ${risingTheme.label}，相对前半段净强度提升 ${risingTheme.delta}。`);
  }
  if (fadingTheme && fadingTheme.delta < 0) {
    summaryLines.push(`后半段退潮最快的是 ${fadingTheme.label}，相对前半段净强度回落 ${Math.abs(fadingTheme.delta)}。`);
  }

  const report = {
    artifacts: {
      jsonPath: path.join(ROTATION_REPORT_DIR, `${historyDay}-${definition.key}.json`),
      svgPath: path.join(ROTATION_REPORT_DIR, `${historyDay}-${definition.key}.svg`),
    },
    categories,
    dominantTheme: dominantThemes[0]?.label || null,
    generatedAt: new Date().toISOString(),
    headline: buildRotationReportHeadline(definition.label, dominantThemes[0]?.label || null, strongestTransition, longestStreak),
    intervalCount: normalizedSeries.length,
    label: definition.label,
    series: normalizedSeries,
    sessionKey: definition.key,
    summaryLines,
    windowEndedAt: eligibleEntries[eligibleEntries.length - 1].comparisonEndedAt,
    windowStartedAt: eligibleEntries[0].comparisonStartedAt,
  };

  report.svgMarkup = buildRotationSvg(report);
  return report;
}

function buildRotationReports(historyEntries, historyDay) {
  return ROTATION_REPORT_DEFINITIONS
    .map((definition) => buildRotationReport(historyEntries, definition, historyDay))
    .filter(Boolean);
}

function buildRotationInsights(reports) {
  return reports.map((report) => report.headline).filter(Boolean);
}

async function persistRotationReports(reports) {
  if (!Array.isArray(reports) || reports.length === 0) {
    return;
  }

  await fs.mkdir(ROTATION_REPORT_DIR, { recursive: true });

  await Promise.all(reports.map(async (report) => {
    const jsonPayload = {
      ...report,
      svgMarkup: undefined,
    };

    await fs.writeFile(report.artifacts.jsonPath, JSON.stringify(jsonPayload, null, 2), 'utf8');
    await fs.writeFile(report.artifacts.svgPath, report.svgMarkup || '', 'utf8');
  }));
}

async function loadCache() {
  try {
    const rawCache = await fs.readFile(CACHE_FILE, 'utf8');
    const cached = JSON.parse(rawCache);

    if (!cached || typeof cached !== 'object') {
      return;
    }

    state.comparisonEndedAt = cached.comparisonEndedAt || null;
    state.comparisonMode = cached.comparisonMode || '5m';
    state.comparisonModeLabel = cached.comparisonModeLabel || getComparisonModeLabel(cached.windowMinutes || 5);
    state.comparisonReady = Boolean(cached.comparisonReady);
    state.comparisonStartedAt = cached.comparisonStartedAt || null;
    state.coverageCount = cached.coverageCount || 0;
    state.error = cached.error || null;
    state.fallers = Array.isArray(cached.fallers) ? cached.fallers : [];
    state.groups = Array.isArray(cached.groups) ? cached.groups : [];
    state.history = Array.isArray(cached.history) ? cached.history : [];
    state.historyDay = cached.historyDay || formatTradingDay(new Date(cached.lastSuccessAt || Date.now()));
    state.lastAttemptAt = cached.lastAttemptAt || null;
    state.lastSuccessAt = cached.lastSuccessAt || null;
    state.leaders = Array.isArray(cached.leaders) ? cached.leaders : [];
    state.nextRefreshAt = cached.nextRefreshAt || null;
    state.rotationInsights = Array.isArray(cached.rotationInsights) ? cached.rotationInsights : [];
    state.rotationReports = Array.isArray(cached.rotationReports) ? cached.rotationReports : [];
    state.status = cached.status || 'idle';
    state.summary = Array.isArray(cached.summary) ? cached.summary : [];
    state.windowMinutes = cached.windowMinutes ?? null;

    internalState.historyDay = cached.historyDay || state.historyDay;
    internalState.slotSnapshots = Array.isArray(cached.slotSnapshots) ? cached.slotSnapshots : [];
    internalState.conceptCache = cached.conceptCache || {};

    sanitizeLoadedState();
    pruneConceptCache();

    if (state.history.length > 0 && state.rotationReports.length === 0) {
      state.rotationReports = buildRotationReports(state.history, state.historyDay);
      state.rotationInsights = buildRotationInsights(state.rotationReports);
    }

    if (state.rotationReports.length > 0) {
      state.summary = appendRotationInsightSummary(state.summary, state.rotationInsights);
      await persistRotationReports(state.rotationReports);
    }
  } catch (error) {
    if (error.code !== 'ENOENT') {
      console.warn('Failed to load market leaders cache:', error.message);
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
        console.warn('Failed to hydrate market leaders cache:', error.message);
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
    conceptCache: internalState.conceptCache,
    slotSnapshots: internalState.slotSnapshots,
  }, null, 2), 'utf8');
}

async function refreshMarketLeaders(options = {}) {
  const {
    force = false,
    persistHistory = true,
    scheduledAt = null,
  } = options;

  await ensureCacheLoaded();

  if (inFlightRefresh) {
    return inFlightRefresh;
  }

  const now = new Date();
  ensureTradingDayState(now);
  syncSchedule(now);

  const slotEndedAtDate = scheduledAt ? new Date(scheduledAt) : getLatestScheduledBoundary(now);

  if (!slotEndedAtDate) {
    state.error = null;
    state.status = state.lastSuccessAt ? 'stale' : 'idle';
    state.summary = buildOutOfSessionSummary(state.comparisonReady || state.leaders.length > 0, state.history.length);
    return cloneState();
  }

  const slotEndedAt = slotEndedAtDate.toISOString();

  if (!scheduledAt && !isTradingSessionOpen(now)) {
    state.error = null;
    state.status = state.lastSuccessAt ? 'stale' : 'idle';
    state.summary = buildOutOfSessionSummary(state.comparisonReady || state.leaders.length > 0, state.history.length);
    return cloneState();
  }

  if (!scheduledAt && !force && state.comparisonEndedAt === slotEndedAt) {
    return cloneState();
  }

  if (!force && state.comparisonEndedAt === slotEndedAt) {
    return cloneState();
  }

  inFlightRefresh = (async () => {
    state.error = null;
    state.lastAttemptAt = new Date().toISOString();
    state.status = state.leaders.length > 0 || state.history.length > 0 ? 'refreshing' : 'loading';

    try {
      const marketSnapshot = await fetchMarketSnapshot();
      const currentSnapshot = {
        capturedAt: marketSnapshot.capturedAt,
        slotEndedAt,
        stocks: marketSnapshot.stocks,
      };
      const fiveMinuteDetails = buildIntervalDetails(currentSnapshot, 5);
      const fiveMinuteLeaders = fiveMinuteDetails.comparisonReady
        ? await enrichLeaders(fiveMinuteDetails.leaders)
        : [];
      const fiveMinuteFallers = fiveMinuteDetails.comparisonReady
        ? await enrichLeaders(fiveMinuteDetails.fallers || [])
        : [];

      if (persistHistory && fiveMinuteDetails.comparisonReady) {
        upsertHistoryEntry(buildHistoryEntry(fiveMinuteDetails, fiveMinuteLeaders, fiveMinuteFallers));
      }

      const displayLookbackMinutes = shouldDisplayFifteenMinuteLeaders(slotEndedAtDate) ? 15 : 5;
      const displayDetails = displayLookbackMinutes === 5
        ? fiveMinuteDetails
        : buildIntervalDetails(currentSnapshot, 15);
      const displayLeaders = displayLookbackMinutes === 5
        ? fiveMinuteLeaders
        : (displayDetails.comparisonReady ? await enrichLeaders(displayDetails.leaders) : []);
      const groups = buildLeaderGroups(displayLeaders);

      state.comparisonEndedAt = slotEndedAt;
      state.comparisonMode = displayLookbackMinutes === 15 ? '15m' : '5m';
      state.comparisonModeLabel = getComparisonModeLabel(displayLookbackMinutes);
      state.comparisonReady = displayDetails.comparisonReady;
      state.comparisonStartedAt = displayDetails.comparisonStartedAt;
      state.coverageCount = currentSnapshot.stocks.length;
      state.fallers = fiveMinuteFallers.map(mapLeaderForState);
      state.groups = groups;
      state.historyDay = formatTradingDay(slotEndedAtDate);
      state.lastSuccessAt = currentSnapshot.capturedAt;
      state.leaders = displayLeaders.map(mapLeaderForState);
      state.rotationReports = buildRotationReports(state.history, state.historyDay);
      state.rotationInsights = buildRotationInsights(state.rotationReports);
      state.status = 'ready';
      state.summary = appendRotationInsightSummary(
        buildSummary(displayDetails, groups, state.history.length, fiveMinuteFallers),
        state.rotationInsights,
      );
      state.windowMinutes = displayDetails.windowMinutes;

      internalState.historyDay = state.historyDay;
      upsertSlotSnapshot(currentSnapshot);
      syncSchedule(new Date(currentSnapshot.capturedAt));

      await persistRotationReports(state.rotationReports);
      await saveCache();
      return cloneState();
    } catch (error) {
      state.error = error.message;
      state.status = state.history.length > 0 || state.leaders.length > 0 ? 'stale' : 'error';
      syncSchedule(new Date());

      await saveCache();
      return cloneState();
    } finally {
      inFlightRefresh = null;
    }
  })();

  return inFlightRefresh;
}

async function getMarketLeadersSnapshot(options = {}) {
  const { hydrateIfEmpty = false } = options;

  await ensureCacheLoaded();

  const now = new Date();
  ensureTradingDayState(now);
  syncSchedule(now);

  if (!isTradingSessionOpen(now)) {
    state.error = null;
    state.status = state.lastSuccessAt ? 'stale' : 'idle';
    state.summary = buildOutOfSessionSummary(state.comparisonReady || state.leaders.length > 0, state.history.length);
    return cloneState();
  }

  if (hydrateIfEmpty && state.lastSuccessAt === null) {
    const recentBoundary = getRecentBoundaryWithinGrace(now);
    const lastAttemptTime = state.lastAttemptAt ? Date.parse(state.lastAttemptAt) : 0;
    const canRetry = Date.now() - lastAttemptTime > EMPTY_STATE_RETRY_MS;

    if (recentBoundary && !inFlightRefresh && canRetry) {
      await refreshMarketLeaders({
        force: true,
        persistHistory: true,
        scheduledAt: recentBoundary,
      });
    }
  }

  if (state.lastSuccessAt === null && state.summary.length === 0) {
    state.status = 'idle';
    state.summary = buildInSessionWaitingSummary(state.history.length);
  }

  state.summary = appendRotationInsightSummary(state.summary, state.rotationInsights);

  return cloneState();
}

function scheduleNextMarketLeadersRefresh() {
  if (!pollingStarted) {
    return;
  }

  if (nextRefreshTimer) {
    clearTimeout(nextRefreshTimer);
  }

  const now = new Date();
  ensureTradingDayState(now);
  syncSchedule(now);

  const nextBoundary = getNextScheduledBoundary(now);
  state.nextRefreshAt = nextBoundary.toISOString();
  const delay = Math.max(nextBoundary.getTime() - Date.now(), 1000);

  nextRefreshTimer = setTimeout(() => {
    refreshMarketLeaders({
      force: true,
      persistHistory: true,
      scheduledAt: nextBoundary,
    })
      .catch((error) => {
        console.warn('Scheduled market leaders refresh failed:', error.message);
      })
      .finally(() => {
        scheduleNextMarketLeadersRefresh();
      });
  }, delay);
}

function startMarketLeadersPolling() {
  if (pollingStarted) {
    return;
  }

  pollingStarted = true;

  ensureCacheLoaded()
    .then(() => {
      const now = new Date();
      ensureTradingDayState(now);
      syncSchedule(now);

      const recentBoundary = getRecentBoundaryWithinGrace(now);

      if (recentBoundary && state.comparisonEndedAt !== recentBoundary.toISOString()) {
        return refreshMarketLeaders({
          force: true,
          persistHistory: true,
          scheduledAt: recentBoundary,
        });
      }

      if (isTradingSessionOpen(now) && state.lastSuccessAt === null) {
        state.status = 'idle';
        state.summary = buildInSessionWaitingSummary(state.history.length);
        return null;
      }

      state.status = state.lastSuccessAt ? 'stale' : 'idle';
      state.summary = buildOutOfSessionSummary(state.comparisonReady || state.leaders.length > 0, state.history.length);
      return null;
    })
    .catch((error) => {
      console.warn('Initial market leaders refresh failed:', error.message);
    })
    .finally(() => {
      scheduleNextMarketLeadersRefresh();
    });
}

module.exports = {
  fetchMarketSnapshot,
  getMarketLeadersSnapshot,
  refreshMarketLeaders,
  startMarketLeadersPolling,
};

