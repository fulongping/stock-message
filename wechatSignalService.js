const fs = require('fs/promises');
const path = require('path');

const POLL_INTERVAL_MS = 10 * 60 * 1000;
const EMPTY_STATE_RETRY_MS = 60 * 1000;
const MAX_FILES = 200;
const MAX_MESSAGES = 100;
const MAX_RECENT_ITEMS = 12;
const MAX_ARTICLES = 6;

const TARGET_CHAT_NAMES = ['子明和他的朋友们', '49060485253@chatroom'];
const TARGET_PUBLICATION_NAMES = ['子明解读'];
const FOCUSED_SPEAKER_ALIASES = {
  '顾子明': ['顾子明'],
  '子明群-子路': ['子明群-子路', '子路'],
  '间歇泉': ['间歇泉'],
};
const TARGET_SPEAKERS = Object.keys(FOCUSED_SPEAKER_ALIASES);

const IMPORT_DIR = process.env.WECHAT_IMPORT_DIR
  ? path.resolve(process.env.WECHAT_IMPORT_DIR)
  : path.join(__dirname, 'data', 'wechat-import');
const CACHE_FILE = path.join(__dirname, 'data', 'wechat-signal-cache.json');

const KEYWORDS = {
  tech: [
    'ai',
    'aigc',
    'agent',
    'cpo',
    'chip',
    'cpu',
    'gpu',
    'server',
    '算力',
    '芯片',
    '半导体',
    '英伟达',
    '机器人',
    '人形机器人',
    '自动驾驶',
    '软件',
    '云计算',
    '服务器',
    '大模型',
    '国产替代',
    '光模块',
    '液冷',
    '智驾',
    '数据中心',
    '先进制程',
    '端侧',
  ],
  war: [
    '战争',
    '军工',
    '中东',
    '伊朗',
    '以色列',
    '俄乌',
    '油价',
    '红海',
    '航运',
    '导弹',
    '军贸',
    '冲突',
    '制裁',
    '地缘',
    '原油',
    '安全线',
    '防务',
    '军费',
    '武器',
    '海峡',
    '航母',
    '停火',
    '黄金',
    '避险',
  ],
};

const state = {
  analyzedMessageCount: 0,
  discovery: null,
  error: null,
  focusedSpeakers: [],
  importDir: IMPORT_DIR,
  importFileCount: 0,
  intervalMs: POLL_INTERVAL_MS,
  lastAttemptAt: null,
  lastSuccessAt: null,
  nextRefreshAt: null,
  overallSummary: [],
  recentArticles: [],
  recentMessages: [],
  signal: null,
  status: 'idle',
  warnings: [],
};

let pollingStarted = false;
let inFlightRefresh = null;

function cloneState() {
  return JSON.parse(JSON.stringify(state));
}

function sanitizeText(value) {
  return String(value || '')
    .replace(/\r/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/[ \t]{2,}/g, ' ')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .trim();
}

function normalizeKey(value) {
  return sanitizeText(value)
    .toLowerCase()
    .replace(/\s+/g, '')
    .replace(/[：:()\[\]（）"'`·,，.!！？?、\-_/\\]/g, '');
}

function includesAny(value, candidates) {
  const normalizedValue = normalizeKey(value);
  return candidates.some((candidate) => normalizedValue.includes(normalizeKey(candidate)));
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeTimestamp(value, fallbackTimestamp) {
  if (!value) {
    return fallbackTimestamp;
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString();
  }

  if (typeof value === 'number') {
    const maybeMillis = value > 1e12 ? value : value * 1000;
    const date = new Date(maybeMillis);
    return Number.isNaN(date.getTime()) ? fallbackTimestamp : date.toISOString();
  }

  const normalized = String(value)
    .trim()
    .replace(/\./g, '-')
    .replace(/\//g, '-');
  const date = new Date(normalized);
  return Number.isNaN(date.getTime()) ? fallbackTimestamp : date.toISOString();
}

function inferSourceName(filePath) {
  if (includesAny(filePath, TARGET_PUBLICATION_NAMES)) {
    return '子明解读';
  }

  if (includesAny(filePath, TARGET_CHAT_NAMES)) {
    return '子明和他的朋友们';
  }

  return '';
}

function detectSourceType({ chatName, sourceName, sender }) {
  const joined = [chatName, sourceName, sender].filter(Boolean).join(' ');

  if (includesAny(joined, TARGET_PUBLICATION_NAMES)) {
    return 'article';
  }

  if (includesAny(joined, TARGET_CHAT_NAMES)) {
    return 'group';
  }

  return 'unknown';
}

function normalizeRecord(rawRecord, context) {
  const fileSourceName = inferSourceName(context.filePath);
  const sourceName = sanitizeText(
    rawRecord.sourceName
      || rawRecord.source
      || rawRecord.publication
      || rawRecord.account
      || context.sourceName
      || fileSourceName
  );
  const chatName = sanitizeText(
    rawRecord.chatName
      || rawRecord.chat
      || rawRecord.room
      || rawRecord.group
      || context.chatName
      || fileSourceName
  );
  const sender = sanitizeText(
    rawRecord.sender
      || rawRecord.author
      || rawRecord.nickname
      || rawRecord.name
      || rawRecord.from
      || context.sender
  );
  const title = sanitizeText(rawRecord.title || rawRecord.subject || '');
  const content = sanitizeText(
    rawRecord.content
      || rawRecord.text
      || rawRecord.message
      || rawRecord.body
      || rawRecord.summary
      || rawRecord.desc
      || ''
  );

  if (!title && !content) {
    return null;
  }

  const timestamp = normalizeTimestamp(
    rawRecord.timestamp
      || rawRecord.time
      || rawRecord.datetime
      || rawRecord.date
      || rawRecord.createdAt
      || rawRecord.created_at,
    context.fallbackTimestamp
  );

  return {
    chatName,
    content,
    filePath: context.filePath,
    sender,
    sourceName,
    sourceType: detectSourceType({ chatName, sender, sourceName }),
    timestamp,
    title,
  };
}

function parseStructuredJson(rawValue, context) {
  if (Array.isArray(rawValue)) {
    return rawValue
      .flatMap((item) => parseStructuredJson(item, context));
  }

  if (!rawValue || typeof rawValue !== 'object') {
    return [];
  }

  if (Array.isArray(rawValue.messages)) {
    return rawValue.messages
      .map((item) => normalizeRecord(item, {
        ...context,
        chatName: rawValue.chatName || rawValue.chat || rawValue.group,
        sourceName: rawValue.sourceName || rawValue.source,
      }))
      .filter(Boolean);
  }

  if (Array.isArray(rawValue.articles)) {
    return rawValue.articles
      .map((item) => normalizeRecord(item, {
        ...context,
        sourceName: rawValue.sourceName || rawValue.source || rawValue.publication,
      }))
      .filter(Boolean);
  }

  const normalized = normalizeRecord(rawValue, context);
  return normalized ? [normalized] : [];
}

function parseChatLikeText(content, context) {
  const lines = content.split(/\r?\n/);
  const messages = [];
  let currentMessage = null;

  const patterns = [
    /^\[?(\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)\]?\s+([^:：\t]{1,40})[:：]\s*(.+)$/,
    /^([^:：\t]{1,40})\t(\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)\t(.+)$/,
    /^(\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)\t([^:：\t]{1,40})\t(.+)$/,
    /^(\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)\s+([^:：]{1,40})[:：]\s*(.+)$/,
  ];

  for (const rawLine of lines) {
    const line = rawLine.trim();

    if (!line) {
      continue;
    }

    let matched = null;

    for (const pattern of patterns) {
      const result = line.match(pattern);
      if (result) {
        matched = result;
        break;
      }
    }

    if (matched) {
      if (currentMessage) {
        messages.push(currentMessage);
      }

      const sender = /\d{4}/.test(matched[1]) ? matched[2] : matched[1];
      const timestamp = /\d{4}/.test(matched[1]) ? matched[1] : matched[2];
      currentMessage = normalizeRecord({
        content: matched[3],
        sender,
        timestamp,
      }, context);
      continue;
    }

    if (currentMessage) {
      currentMessage.content = sanitizeText(`${currentMessage.content}\n${line}`);
    }
  }

  if (currentMessage) {
    messages.push(currentMessage);
  }

  return messages.filter(Boolean);
}

function parseArticleLikeText(content, context) {
  const titleFromHtml = content.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1];
  const headingFromHtml = content.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i)?.[1];
  const title = sanitizeText(
    titleFromHtml
      || headingFromHtml
      || context.fileName.replace(path.extname(context.fileName), '')
  );
  const body = sanitizeText(content);

  if (!title && !body) {
    return [];
  }

  return [
    normalizeRecord({
      content: body,
      sourceName: context.sourceName || inferSourceName(context.filePath),
      title,
    }, context),
  ].filter(Boolean);
}

async function parseImportFile(filePath, stat) {
  const fileName = path.basename(filePath);
  const extension = path.extname(filePath).toLowerCase();
  const context = {
    fallbackTimestamp: stat.mtime.toISOString(),
    fileName,
    filePath,
    sourceName: inferSourceName(filePath),
  };
  const rawContent = await fs.readFile(filePath, 'utf8');

  if (!rawContent.trim()) {
    return [];
  }

  if (['.json', '.jsonl', '.ndjson'].includes(extension)) {
    if (extension === '.json') {
      return parseStructuredJson(JSON.parse(rawContent), context);
    }

    return rawContent
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .flatMap((line) => parseStructuredJson(JSON.parse(line), context));
  }

  const chatMessages = parseChatLikeText(rawContent, context);
  if (chatMessages.length > 0) {
    return chatMessages;
  }

  return parseArticleLikeText(rawContent, context);
}

async function collectImportFiles(rootDir) {
  const files = [];

  async function walk(currentDir) {
    let entries;
    try {
      entries = await fs.readdir(currentDir, { withFileTypes: true });
    } catch (error) {
      if (error.code === 'ENOENT') {
        return;
      }
      throw error;
    }

    for (const entry of entries) {
      const fullPath = path.join(currentDir, entry.name);

      if (entry.isDirectory()) {
        await walk(fullPath);
        continue;
      }

      if (!entry.isFile()) {
        continue;
      }

      const extension = path.extname(entry.name).toLowerCase();
      if (!['.json', '.jsonl', '.ndjson', '.txt', '.md', '.html', '.htm'].includes(extension)) {
        continue;
      }

      const stat = await fs.stat(fullPath);
      files.push({ filePath: fullPath, stat });
    }
  }

  await walk(rootDir);
  files.sort((left, right) => right.stat.mtimeMs - left.stat.mtimeMs);
  return files.slice(0, MAX_FILES);
}

function shortSnippet(value, maxLength = 110) {
  const text = sanitizeText(value).replace(/\n+/g, ' ');

  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, maxLength - 1)}…`;
}

function countKeywords(value) {
  const text = sanitizeText(value).toLowerCase();
  const techHits = {};
  const warHits = {};
  let techScore = 0;
  let warScore = 0;

  for (const keyword of KEYWORDS.tech) {
    const matches = text.match(new RegExp(escapeRegExp(keyword.toLowerCase()), 'g'));
    const count = matches ? matches.length : 0;

    if (count > 0) {
      techHits[keyword] = count;
      techScore += count;
    }
  }

  for (const keyword of KEYWORDS.war) {
    const matches = text.match(new RegExp(escapeRegExp(keyword.toLowerCase()), 'g'));
    const count = matches ? matches.length : 0;

    if (count > 0) {
      warHits[keyword] = count;
      warScore += count;
    }
  }

  return {
    techHits,
    techScore,
    warHits,
    warScore,
  };
}

function addKeywordCounts(targetMap, sourceCounts, weight) {
  Object.entries(sourceCounts).forEach(([keyword, count]) => {
    targetMap.set(keyword, (targetMap.get(keyword) || 0) + (count * weight));
  });
}

function rankKeywords(sourceMap, limit = 5) {
  return [...sourceMap.entries()]
    .sort((left, right) => right[1] - left[1])
    .slice(0, limit)
    .map(([keyword, count]) => ({ keyword, count }));
}

function formatKeywordLabel(topKeywords) {
  if (!topKeywords || topKeywords.length === 0) {
    return '暂无明显高频词';
  }

  return topKeywords.map((item) => `${item.keyword}×${item.count}`).join(' / ');
}

function resolveSpeakerName(sender) {
  const normalizedSender = normalizeKey(sender);

  for (const speaker of TARGET_SPEAKERS) {
    const aliases = FOCUSED_SPEAKER_ALIASES[speaker];
    if (aliases.some((alias) => normalizedSender.includes(normalizeKey(alias)))) {
      return speaker;
    }
  }

  return '';
}

function getRecordTimestamp(record) {
  return Date.parse(record.timestamp) || 0;
}

function isTargetGroupRecord(record) {
  return includesAny([record.chatName, record.sourceName, record.filePath].join(' '), TARGET_CHAT_NAMES);
}

function isTargetArticleRecord(record) {
  return includesAny([record.sourceName, record.chatName, record.filePath].join(' '), TARGET_PUBLICATION_NAMES);
}

function classifyBias(techScore, warScore, messageCount, articleCount) {
  const totalScore = techScore + warScore;
  const difference = techScore - warScore;

  if (messageCount === 0 && articleCount === 0) {
    return { bias: '信号不足', confidence: 'low' };
  }

  if (totalScore < 3) {
    return { bias: '信号不足', confidence: 'low' };
  }

  if (Math.abs(difference) <= 2) {
    return { bias: '拉锯中', confidence: totalScore >= 8 ? 'medium' : 'low' };
  }

  const balance = Math.abs(difference) / totalScore;
  const confidence = balance >= 0.45 && totalScore >= 8
    ? 'high'
    : balance >= 0.25
      ? 'medium'
      : 'low';

  return {
    bias: difference > 0 ? '偏科技向' : '偏战争线',
    confidence,
  };
}

function summarizeSpeaker(speaker, records) {
  if (records.length === 0) {
    return {
      bias: '信号不足',
      lastTimestamp: null,
      messageCount: 0,
      snippets: [],
      speaker,
      summary: '最近 100 条群消息里没有识别到该发言人的有效内容。',
      techScore: 0,
      warScore: 0,
    };
  }

  const techKeywords = new Map();
  const warKeywords = new Map();
  let techScore = 0;
  let warScore = 0;

  records.forEach((record) => {
    const counts = countKeywords(`${record.title} ${record.content}`);
    techScore += counts.techScore;
    warScore += counts.warScore;
    addKeywordCounts(techKeywords, counts.techHits, 1);
    addKeywordCounts(warKeywords, counts.warHits, 1);
  });

  const bias = classifyBias(techScore, warScore, records.length, 0).bias;
  const dominantKeywords = bias === '偏战争线'
    ? rankKeywords(warKeywords, 3)
    : rankKeywords(techKeywords, 3);
  const keywordText = dominantKeywords.length > 0
    ? dominantKeywords.map((item) => item.keyword).join(' / ')
    : '没有形成稳定关键词聚焦';

  return {
    bias,
    lastTimestamp: records[0].timestamp,
    messageCount: records.length,
    snippets: records.slice(0, 3).map((record) => shortSnippet(record.content)),
    speaker,
    summary: `最近 ${records.length} 条发言${bias === '信号不足' ? '方向还不够清晰' : bias}，高频内容更接近 ${keywordText}。`,
    techScore,
    warScore,
  };
}

async function detectEncryptedWeChatStore() {
  const scannedRoots = [];
  const dbCandidates = [];

  const rootCandidates = [
    path.join('D:', 'software', 'weixin', 'xwechat_files'),
    process.env.APPDATA ? path.join(process.env.APPDATA, 'Tencent', 'xwechat') : '',
  ].filter(Boolean);

  for (const root of rootCandidates) {
    try {
      const stat = await fs.stat(root);
      if (!stat.isDirectory()) {
        continue;
      }

      scannedRoots.push(root);

      if (root.endsWith('xwechat_files')) {
        const accounts = await fs.readdir(root, { withFileTypes: true });
        for (const entry of accounts) {
          if (!entry.isDirectory()) {
            continue;
          }

          const accountRoot = path.join(root, entry.name, 'db_storage', 'message');
          dbCandidates.push(path.join(accountRoot, 'message_0.db'));
          dbCandidates.push(path.join(accountRoot, 'biz_message_0.db'));
        }
      }
    } catch (error) {
      if (error.code !== 'ENOENT') {
        scannedRoots.push(root);
      }
    }
  }

  const checkedFiles = [];
  for (const candidate of dbCandidates) {
    try {
      const handle = await fs.open(candidate, 'r');
      const buffer = Buffer.alloc(16);
      await handle.read(buffer, 0, 16, 0);
      await handle.close();

      checkedFiles.push(candidate);
      const header = buffer.toString('utf8');

      if (!header.startsWith('SQLite format 3')) {
        return {
          checkedFiles,
          encryptedStoreDetected: true,
          liveAccessSupported: false,
          note: '检测到新版微信本地消息库，但数据库不是明文 SQLite，当前工具无法直接读取实时消息，只能分析导入文本。',
          scannedRoots,
        };
      }

      return {
        checkedFiles,
        encryptedStoreDetected: false,
        liveAccessSupported: true,
        note: '检测到明文 SQLite 消息库，可以继续扩展实时读取逻辑，但当前版本仍以导入文本分析为主。',
        scannedRoots,
      };
    } catch (error) {
      if (error.code !== 'ENOENT') {
        checkedFiles.push(candidate);
      }
    }
  }

  return {
    checkedFiles,
    encryptedStoreDetected: false,
    liveAccessSupported: false,
    note: '当前没有发现可直接读取的微信明文数据库，本工具默认只分析导入到指定目录的文本或 JSON 文件。',
    scannedRoots,
  };
}

function buildSignal(groupMessages, articles) {
  const techKeywords = new Map();
  const warKeywords = new Map();
  let techScore = 0;
  let warScore = 0;
  let focusedMessageCount = 0;

  [...groupMessages, ...articles].forEach((record) => {
    const speaker = resolveSpeakerName(record.sender);
    const weight = speaker ? 2 : 1;
    const counts = countKeywords(`${record.title} ${record.content}`);

    techScore += counts.techScore * weight;
    warScore += counts.warScore * weight;
    addKeywordCounts(techKeywords, counts.techHits, weight);
    addKeywordCounts(warKeywords, counts.warHits, weight);

    if (speaker) {
      focusedMessageCount += 1;
    }
  });

  const classification = classifyBias(techScore, warScore, groupMessages.length, articles.length);

  return {
    articleCount: articles.length,
    bias: classification.bias,
    confidence: classification.confidence,
    focusedMessageCount,
    messageCount: groupMessages.length,
    techScore,
    topKeywords: {
      tech: rankKeywords(techKeywords, 5),
      war: rankKeywords(warKeywords, 5),
    },
    warScore,
  };
}

function buildOverallSummary(groupMessages, articles, signal, focusedSpeakers) {
  if (groupMessages.length === 0 && articles.length === 0) {
    return [
      '导入目录里还没有识别到“子明和他的朋友们”群聊消息或“子明解读”文章。',
      '把最近 100 条群消息和公众号正文导入到 data/wechat-import 后，系统会在下一次轮询时自动分析。',
      '当前判断不会直接读取微信实时数据库，因为本机已检测到新版加密存储。',
    ];
  }

  const lines = [];
  lines.push(`最近分析了 ${groupMessages.length} 条群消息和 ${articles.length} 篇文章，综合判断为“${signal.bias}”。`);
  lines.push(`科技关键词得分 ${signal.techScore}，战争线关键词得分 ${signal.warScore}。`);

  const activeSpeakers = focusedSpeakers.filter((speaker) => speaker.messageCount > 0);
  if (activeSpeakers.length > 0) {
    const speakerText = activeSpeakers
      .map((speaker) => `${speaker.speaker}${speaker.messageCount}条`)
      .join('，');
    lines.push(`重点发言人里，本轮有明确内容输出的是：${speakerText}。`);
  } else {
    lines.push('最近 100 条群消息里，三位重点发言人的可识别发言较少，当前结论更多依赖群整体讨论和公众号内容。');
  }

  const keywordBias = signal.bias === '偏战争线'
    ? formatKeywordLabel(signal.topKeywords.war.slice(0, 3))
    : formatKeywordLabel(signal.topKeywords.tech.slice(0, 3));
  lines.push(`高频词更集中在：${keywordBias}。`);

  if (articles.length > 0) {
    lines.push(`最近一篇公众号文章是《${articles[0].title || '未命名文章'}》，其内容已并入判断。`);
  }

  return lines.slice(0, 5);
}

async function saveCache() {
  await fs.mkdir(path.dirname(CACHE_FILE), { recursive: true });
  await fs.writeFile(CACHE_FILE, JSON.stringify(cloneState(), null, 2), 'utf8');
}

async function loadCache() {
  try {
    const rawCache = await fs.readFile(CACHE_FILE, 'utf8');
    const cachedState = JSON.parse(rawCache);

    if (!cachedState || typeof cachedState !== 'object') {
      return;
    }

    state.analyzedMessageCount = cachedState.analyzedMessageCount || 0;
    state.discovery = cachedState.discovery || null;
    state.error = cachedState.error || null;
    state.focusedSpeakers = Array.isArray(cachedState.focusedSpeakers) ? cachedState.focusedSpeakers : [];
    state.importFileCount = cachedState.importFileCount || 0;
    state.lastAttemptAt = cachedState.lastAttemptAt || null;
    state.lastSuccessAt = cachedState.lastSuccessAt || null;
    state.nextRefreshAt = cachedState.nextRefreshAt || null;
    state.overallSummary = Array.isArray(cachedState.overallSummary) ? cachedState.overallSummary : [];
    state.recentArticles = Array.isArray(cachedState.recentArticles) ? cachedState.recentArticles : [];
    state.recentMessages = Array.isArray(cachedState.recentMessages) ? cachedState.recentMessages : [];
    state.signal = cachedState.signal || null;
    state.status = cachedState.status || 'idle';
    state.warnings = Array.isArray(cachedState.warnings) ? cachedState.warnings : [];
  } catch (error) {
    if (error.code !== 'ENOENT') {
      console.warn('Failed to load WeChat signal cache:', error.message);
    }
  }
}

async function refreshWeChatSignal(options = {}) {
  const { force = false } = options;

  if (inFlightRefresh) {
    return inFlightRefresh;
  }

  const now = Date.now();
  const lastSuccessTime = state.lastSuccessAt ? Date.parse(state.lastSuccessAt) : 0;
  const isFresh = state.lastSuccessAt && (now - lastSuccessTime < POLL_INTERVAL_MS);

  if (!force && isFresh) {
    return cloneState();
  }

  inFlightRefresh = (async () => {
    state.error = null;
    state.lastAttemptAt = new Date().toISOString();
    state.status = state.signal ? 'refreshing' : 'loading';
    state.warnings = [];

    try {
      await fs.mkdir(IMPORT_DIR, { recursive: true });
      const discovery = await detectEncryptedWeChatStore();
      const files = await collectImportFiles(IMPORT_DIR);
      const parsedRecords = [];
      const parseWarnings = [];

      for (const { filePath, stat } of files) {
        try {
          const records = await parseImportFile(filePath, stat);
          parsedRecords.push(...records);
        } catch (error) {
          parseWarnings.push(`${path.basename(filePath)} 解析失败：${error.message}`);
        }
      }

      const relevantGroupMessages = parsedRecords
        .filter(isTargetGroupRecord)
        .sort((left, right) => getRecordTimestamp(right) - getRecordTimestamp(left))
        .slice(0, MAX_MESSAGES);

      const relevantArticles = parsedRecords
        .filter(isTargetArticleRecord)
        .sort((left, right) => getRecordTimestamp(right) - getRecordTimestamp(left))
        .slice(0, MAX_ARTICLES);

      const focusedSpeakers = TARGET_SPEAKERS.map((speaker) => summarizeSpeaker(
        speaker,
        relevantGroupMessages.filter((record) => resolveSpeakerName(record.sender) === speaker),
      ));

      const signal = buildSignal(relevantGroupMessages, relevantArticles);
      const warnings = [];

      if (discovery?.note) {
        warnings.push(discovery.note);
      }

      warnings.push(...parseWarnings.slice(0, 3));

      state.analyzedMessageCount = relevantGroupMessages.length;
      state.discovery = discovery;
      state.focusedSpeakers = focusedSpeakers;
      state.importFileCount = files.length;
      state.lastSuccessAt = new Date().toISOString();
      state.nextRefreshAt = new Date(Date.now() + POLL_INTERVAL_MS).toISOString();
      state.overallSummary = buildOverallSummary(
        relevantGroupMessages,
        relevantArticles,
        signal,
        focusedSpeakers,
      );
      state.recentArticles = relevantArticles.slice(0, MAX_ARTICLES).map((article) => ({
        bias: classifyBias(
          countKeywords(`${article.title} ${article.content}`).techScore,
          countKeywords(`${article.title} ${article.content}`).warScore,
          0,
          1,
        ).bias,
        snippet: shortSnippet(article.content, 150),
        sourceName: article.sourceName || '子明解读',
        timestamp: article.timestamp,
        title: article.title || '未命名文章',
      }));
      state.recentMessages = relevantGroupMessages.slice(0, MAX_RECENT_ITEMS).map((message) => {
        const counts = countKeywords(`${message.title} ${message.content}`);
        return {
          bias: classifyBias(counts.techScore, counts.warScore, 1, 0).bias,
          chatName: message.chatName || '子明和他的朋友们',
          isFocusedSpeaker: Boolean(resolveSpeakerName(message.sender)),
          sender: message.sender || '未知发言人',
          snippet: shortSnippet(message.content, 130),
          timestamp: message.timestamp,
        };
      });
      state.signal = signal;
      state.status = 'ready';
      state.warnings = warnings;

      await saveCache();
      return cloneState();
    } catch (error) {
      state.error = error.message;
      state.nextRefreshAt = new Date(Date.now() + POLL_INTERVAL_MS).toISOString();
      state.status = state.signal ? 'stale' : 'error';

      await saveCache();
      return cloneState();
    } finally {
      inFlightRefresh = null;
    }
  })();

  return inFlightRefresh;
}

async function getWeChatSignalSnapshot(options = {}) {
  const { hydrateIfEmpty = false } = options;

  if (hydrateIfEmpty && !state.lastSuccessAt) {
    const lastAttemptTime = state.lastAttemptAt ? Date.parse(state.lastAttemptAt) : 0;
    const canRetry = Date.now() - lastAttemptTime > EMPTY_STATE_RETRY_MS;

    if (!inFlightRefresh && canRetry) {
      await refreshWeChatSignal({ force: true });
    }
  }

  return cloneState();
}

function startWeChatSignalPolling() {
  if (pollingStarted) {
    return;
  }

  pollingStarted = true;

  loadCache()
    .then(() => refreshWeChatSignal({ force: true }))
    .catch((error) => {
      console.warn('Initial WeChat signal refresh failed:', error.message);
    });

  setInterval(() => {
    refreshWeChatSignal({ force: true }).catch((error) => {
      console.warn('Scheduled WeChat signal refresh failed:', error.message);
    });
  }, POLL_INTERVAL_MS);
}

module.exports = {
  getWeChatSignalSnapshot,
  refreshWeChatSignal,
  startWeChatSignalPolling,
};
