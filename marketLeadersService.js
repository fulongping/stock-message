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
const SUPPORTED_CODE_PATTERN = /^(60|00)/;
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
  groups: [],
  history: [],
  historyDay: null,
  intervalMs: FETCH_INTERVAL_MS,
  lastAttemptAt: null,
  lastSuccessAt: null,
  leaders: [],
  nextRefreshAt: null,
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
    leaders: (entry.leaders || []).map((leader) => ({ ...leader })),
    topLeader: entry.topLeader ? { ...entry.topLeader } : null,
    windowMinutes: entry.windowMinutes,
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

function normalizeRankStock(rawItem) {
  const code = String(rawItem['5'] || '').trim();
  const name = String(rawItem['55'] || '').trim();
  const price = parseNumber(rawItem['10']);
  const dailyChangePercent = parseNumber(rawItem['199112']);
  const turnoverRate = parseNumber(rawItem['1968584']);
  const volume = parseNumber(rawItem['13']);

  if (!code || !name || !Number.isFinite(price) || !SUPPORTED_CODE_PATTERN.test(code)) {
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
      leaders: [],
      windowMinutes: lookbackMinutes,
    };
  }

  const leaders = currentSnapshot.stocks
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
    })
    .slice(0, TOP_STOCK_COUNT);

  return {
    comparisonEndedAt: currentSnapshot.slotEndedAt,
    comparisonReady: true,
    comparisonReason: 'ready',
    comparisonStartedAt: referenceSnapshot.slotEndedAt,
    coverageCount: currentSnapshot.stocks.length,
    leaders,
    windowMinutes: lookbackMinutes,
  };
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
    name: leader.name,
    primaryCategory: leader.primaryCategory,
    rank: index + 1,
    windowChangePercent: leader.windowChangePercent,
  };
}

function buildHistoryEntry(details, leaders) {
  return {
    comparisonEndedAt: details.comparisonEndedAt,
    comparisonStartedAt: details.comparisonStartedAt,
    leaders: leaders.map(mapHistoryLeader),
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

function sanitizeLoadedState() {
  state.history = Array.isArray(state.history) ? state.history.filter(Boolean) : [];
  internalState.slotSnapshots = Array.isArray(internalState.slotSnapshots)
    ? internalState.slotSnapshots.filter((item) => item && item.slotEndedAt && item.universeSnapshot)
    : [];

  if (containsUnsupportedLeaderCodes(state.leaders)
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

function buildSummary(details, groups, historyCount) {
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

  lines.push('股票分类优先使用同花顺概念题材；若概念提取失败，则回退到主板分类。');
  return lines;
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
    state.groups = Array.isArray(cached.groups) ? cached.groups : [];
    state.history = Array.isArray(cached.history) ? cached.history : [];
    state.historyDay = cached.historyDay || formatTradingDay(new Date(cached.lastSuccessAt || Date.now()));
    state.lastAttemptAt = cached.lastAttemptAt || null;
    state.lastSuccessAt = cached.lastSuccessAt || null;
    state.leaders = Array.isArray(cached.leaders) ? cached.leaders : [];
    state.nextRefreshAt = cached.nextRefreshAt || null;
    state.status = cached.status || 'idle';
    state.summary = Array.isArray(cached.summary) ? cached.summary : [];
    state.windowMinutes = cached.windowMinutes ?? null;

    internalState.historyDay = cached.historyDay || state.historyDay;
    internalState.slotSnapshots = Array.isArray(cached.slotSnapshots) ? cached.slotSnapshots : [];
    internalState.conceptCache = cached.conceptCache || {};

    sanitizeLoadedState();
    pruneConceptCache();
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

  if (!scheduledAt && state.comparisonEndedAt === slotEndedAt) {
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

      if (persistHistory && fiveMinuteDetails.comparisonReady) {
        upsertHistoryEntry(buildHistoryEntry(fiveMinuteDetails, fiveMinuteLeaders));
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
      state.groups = groups;
      state.historyDay = formatTradingDay(slotEndedAtDate);
      state.lastSuccessAt = currentSnapshot.capturedAt;
      state.leaders = displayLeaders.map(mapLeaderForState);
      state.status = 'ready';
      state.summary = buildSummary(displayDetails, groups, state.history.length);
      state.windowMinutes = displayDetails.windowMinutes;

      internalState.historyDay = state.historyDay;
      upsertSlotSnapshot(currentSnapshot);
      syncSchedule(new Date(currentSnapshot.capturedAt));

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

