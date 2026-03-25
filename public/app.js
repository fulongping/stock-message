const hotElements = {
  countdownText: document.getElementById('hotCountdownText'),
  errorPanel: document.getElementById('hotErrorPanel'),
  lastAttemptText: document.getElementById('hotLastAttemptText'),
  lastSuccessText: document.getElementById('hotLastSuccessText'),
  refreshButton: document.getElementById('refreshHotButton'),
  sourceLink: document.getElementById('sourceLink'),
  statusBadge: document.getElementById('hotStatusBadge'),
  themesGrid: document.getElementById('themesGrid'),
};

const marketElements = {
  countdownText: document.getElementById('marketCountdownText'),
  coverageText: document.getElementById('marketCoverageText'),
  endText: document.getElementById('marketEndText'),
  errorPanel: document.getElementById('marketErrorPanel'),
  groupsGrid: document.getElementById('marketGroupsGrid'),
  historyCountText: document.getElementById('marketHistoryCountText'),
  historyGrid: document.getElementById('marketHistoryGrid'),
  leadersGrid: document.getElementById('marketLeadersGrid'),
  modeText: document.getElementById('marketModeText'),
  startText: document.getElementById('marketStartText'),
  statusBadge: document.getElementById('marketStatusBadge'),
  summaryList: document.getElementById('marketSummaryList'),
  windowText: document.getElementById('marketWindowText'),
};

const patternElements = {
  backtestAvgText: document.getElementById('patternBacktestAvgText'),
  backtestBasisText: document.getElementById('patternBacktestBasisText'),
  backtestCumText: document.getElementById('patternBacktestCumText'),
  backtestDayCountText: document.getElementById('patternBacktestDayCountText'),
  backtestGrid: document.getElementById('patternBacktestGrid'),
  backtestTradeCountText: document.getElementById('patternBacktestTradeCountText'),
  backtestWinRateText: document.getElementById('patternBacktestWinRateText'),
  countdownText: document.getElementById('patternCountdownText'),
  dayText: document.getElementById('patternDayText'),
  errorPanel: document.getElementById('patternErrorPanel'),
  matchCountText: document.getElementById('patternMatchCountText'),
  lastSuccessText: document.getElementById('patternLastSuccessText'),
  picksGrid: document.getElementById('patternPicksGrid'),
  statusBadge: document.getElementById('patternStatusBadge'),
  summaryList: document.getElementById('patternSummaryList'),
  themeCountText: document.getElementById('patternThemeCountText'),
  themesGrid: document.getElementById('patternThemesGrid'),
  warningPanel: document.getElementById('patternWarningPanel'),
};

const signalElements = {
  analyzedCountText: document.getElementById('analyzedCountText'),
  articlesList: document.getElementById('recentArticlesList'),
  biasText: document.getElementById('biasText'),
  confidenceText: document.getElementById('confidenceText'),
  countdownText: document.getElementById('signalCountdownText'),
  errorPanel: document.getElementById('signalErrorPanel'),
  importDirText: document.getElementById('importDirText'),
  importFileCountText: document.getElementById('importFileCountText'),
  lastAttemptText: document.getElementById('signalLastAttemptText'),
  lastSuccessText: document.getElementById('signalLastSuccessText'),
  messagesList: document.getElementById('recentMessagesList'),
  refreshButton: document.getElementById('refreshSignalButton'),
  speakerGrid: document.getElementById('speakerGrid'),
  statusBadge: document.getElementById('signalStatusBadge'),
  summaryList: document.getElementById('summaryList'),
  techKeywordsText: document.getElementById('techKeywordsText'),
  techScoreText: document.getElementById('techScoreText'),
  warKeywordsText: document.getElementById('warKeywordsText'),
  warScoreText: document.getElementById('warScoreText'),
  warningPanel: document.getElementById('warningPanel'),
};

let latestHotSnapshot = null;
let latestMarketSnapshot = null;
let latestPatternSnapshot = null;
let latestSignalSnapshot = null;

function formatDateTime(value) {
  if (!value) {
    return '--';
  }

  return new Intl.DateTimeFormat('zh-CN', {
    dateStyle: 'medium',
    timeStyle: 'medium',
  }).format(new Date(value));
}

function formatClock(value) {
  if (!value) {
    return '--';
  }

  return new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit',
    hour12: false,
    minute: '2-digit',
  }).format(new Date(value));
}

function formatCountdown(targetTime) {
  if (!targetTime) {
    return '等待下一次计划刷新时间';
  }

  const diff = new Date(targetTime).getTime() - Date.now();

  if (diff <= 0) {
    return '计划刷新时间已到，等待服务端下一轮更新';
  }

  const totalSeconds = Math.floor(diff / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;

  return `距离下一次自动刷新还有 ${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
}

function getStatusText(status) {
  switch (status) {
    case 'ready':
      return '已就绪';
    case 'refreshing':
      return '刷新中';
    case 'stale':
      return '使用缓存';
    case 'error':
      return '分析失败';
    case 'loading':
      return '初始化中';
    default:
      return '待启动';
  }
}

function getConfidenceText(confidence) {
  switch (confidence) {
    case 'high':
      return '高';
    case 'medium':
      return '中';
    case 'low':
      return '低';
    default:
      return '--';
  }
}

function getBiasTone(bias) {
  switch (bias) {
    case '偏科技向':
      return 'tech';
    case '偏战争线':
      return 'war';
    case '拉锯中':
      return 'mixed';
    default:
      return 'neutral';
  }
}

function formatKeywords(items) {
  if (!items || items.length === 0) {
    return '暂无明显高频词';
  }

  return items.map((item) => `${item.keyword}×${item.count}`).join(' / ');
}

function formatPercent(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '--';
  }

  return `${Number(value).toFixed(digits)}%`;
}

function createEmptyBlock(message) {
  const block = document.createElement('div');
  block.className = 'placeholder-card';
  block.textContent = message;
  return block;
}

function createBiasBadge(text, className = 'inline-bias') {
  const badge = document.createElement('span');
  badge.className = `${className} ${getBiasTone(text)}`;
  badge.textContent = text || '信号不足';
  return badge;
}

function createTag(text) {
  const tag = document.createElement('span');
  tag.className = 'tag-pill';
  tag.textContent = text;
  return tag;
}

function createChangeBadge(value) {
  const badge = document.createElement('span');
  const numeric = value === null || value === undefined ? Number.NaN : Number(value);
  const tone = numeric > 0 ? 'up' : numeric < 0 ? 'down' : 'flat';
  badge.className = `change-badge ${tone}`;
  badge.textContent = formatPercent(numeric, 2);
  return badge;
}

function setPanelMessage(panel, messages, itemClassName = 'placeholder-card') {
  panel.innerHTML = '';

  if (!messages || messages.length === 0) {
    panel.classList.add('hidden');
    return;
  }

  panel.classList.remove('hidden');
  messages.forEach((message) => {
    const line = document.createElement('p');
    line.className = itemClassName;
    line.textContent = message;
    panel.appendChild(line);
  });
}

function renderThemes(themes) {
  hotElements.themesGrid.innerHTML = '';

  if (!themes || themes.length === 0) {
    hotElements.themesGrid.appendChild(createEmptyBlock('暂时还没有抓到可展示的主题。'));
    return;
  }

  themes.forEach((theme) => {
    const card = document.createElement('article');
    card.className = 'theme-card';

    const rank = document.createElement('div');
    rank.className = 'theme-rank';
    rank.textContent = String(theme.id).padStart(2, '0');

    const body = document.createElement('div');
    const label = document.createElement('p');
    label.className = 'theme-label';
    label.textContent = '今日热度主题';

    const name = document.createElement('h3');
    name.className = 'theme-name';
    name.textContent = theme.name;

    body.append(label, name);
    card.append(rank, body);
    hotElements.themesGrid.appendChild(card);
  });
}

function renderHotError(snapshot) {
  if (!snapshot.error) {
    hotElements.errorPanel.classList.add('hidden');
    hotElements.errorPanel.textContent = '';
    return;
  }

  hotElements.errorPanel.classList.remove('hidden');
  hotElements.errorPanel.textContent = `最近一次抓取异常：${snapshot.error}`;
}

function renderHotSnapshot(snapshot) {
  latestHotSnapshot = snapshot;
  hotElements.statusBadge.textContent = getStatusText(snapshot.status);
  hotElements.statusBadge.dataset.status = snapshot.status;
  hotElements.countdownText.textContent = formatCountdown(snapshot.nextRefreshAt);
  hotElements.lastSuccessText.textContent = formatDateTime(snapshot.lastSuccessAt);
  hotElements.lastAttemptText.textContent = formatDateTime(snapshot.lastAttemptAt);
  hotElements.sourceLink.href = snapshot.sourceUrl || hotElements.sourceLink.href;
  renderThemes(snapshot.themes);
  renderHotError(snapshot);
}

function renderMarketSummary(lines) {
  marketElements.summaryList.innerHTML = '';

  if (!lines || lines.length === 0) {
    const item = document.createElement('li');
    item.textContent = '暂无区间摘要。';
    marketElements.summaryList.appendChild(item);
    return;
  }

  lines.forEach((line) => {
    const item = document.createElement('li');
    item.textContent = line;
    marketElements.summaryList.appendChild(item);
  });
}

function renderMarketGroups(snapshot) {
  marketElements.groupsGrid.innerHTML = '';

  if (!snapshot.comparisonReady) {
    marketElements.groupsGrid.appendChild(createEmptyBlock('等待新的 5 分钟整点快照后再生成概念分类。'));
    return;
  }

  if (!snapshot.groups || snapshot.groups.length === 0) {
    marketElements.groupsGrid.appendChild(createEmptyBlock('本轮没有形成明显的重复概念分类。'));
    return;
  }

  snapshot.groups.forEach((group) => {
    const card = document.createElement('article');
    card.className = 'leader-group-card';

    const top = document.createElement('div');
    top.className = 'leader-group-top';

    const title = document.createElement('strong');
    title.textContent = group.label;

    const count = document.createElement('span');
    count.className = 'leader-group-count';
    count.textContent = `${group.count} 只`;

    top.append(title, count);

    const stocks = document.createElement('div');
    stocks.className = 'leader-group-stocks';

    group.stocks.forEach((stock) => {
      const chip = document.createElement('span');
      chip.className = 'leader-stock-chip';
      chip.textContent = `${stock.name} ${formatPercent(stock.windowChangePercent, 2)}`;
      stocks.appendChild(chip);
    });

    card.append(top, stocks);
    marketElements.groupsGrid.appendChild(card);
  });
}

function renderMarketLeaders(snapshot) {
  marketElements.leadersGrid.innerHTML = '';

  if (!snapshot.comparisonReady) {
    marketElements.leadersGrid.appendChild(createEmptyBlock('已经拿到当前基准快照，下一轮 5 分钟整点后这里会显示 60 / 00 主板区间涨幅前十。'));
    return;
  }

  if (!snapshot.leaders || snapshot.leaders.length === 0) {
    marketElements.leadersGrid.appendChild(createEmptyBlock('本轮没有可展示的区间涨幅前十。'));
    return;
  }

  snapshot.leaders.forEach((leader) => {
    const card = document.createElement('article');
    card.className = 'stock-card';

    const top = document.createElement('div');
    top.className = 'stock-top';

    const rank = document.createElement('span');
    rank.className = 'stock-rank';
    rank.textContent = String(leader.rank).padStart(2, '0');

    const heading = document.createElement('div');
    heading.className = 'stock-heading';

    const name = document.createElement('h4');
    name.textContent = leader.name;

    const code = document.createElement('p');
    code.className = 'stock-code';
    code.textContent = `${leader.code} · ${leader.board}`;

    heading.append(name, code);
    top.append(rank, heading, createChangeBadge(leader.windowChangePercent));

    const metrics = document.createElement('div');
    metrics.className = 'stock-metrics';

    [
      `区间涨幅 ${formatPercent(leader.windowChangePercent, 2)}`,
      `当前价格 ${leader.price ?? '--'}`,
      `当日涨跌 ${formatPercent(leader.dailyChangePercent, 2)}`,
      `换手率 ${formatPercent(leader.turnoverRate, 2)}`,
      `成交量 ${leader.volumeLabel}`,
    ].forEach((text) => {
      const line = document.createElement('p');
      line.className = 'metric-line';
      line.textContent = text;
      metrics.appendChild(line);
    });

    const tags = document.createElement('div');
    tags.className = 'tag-list';
    const tagSource = leader.concepts && leader.concepts.length > 0
      ? leader.concepts.slice(0, 4)
      : [leader.board];

    tagSource.forEach((tag) => {
      tags.appendChild(createTag(tag));
    });

    card.append(top, metrics, tags);
    marketElements.leadersGrid.appendChild(card);
  });
}

function renderMarketHistory(snapshot) {
  marketElements.historyGrid.innerHTML = '';

  if (!snapshot.history || snapshot.history.length === 0) {
    marketElements.historyGrid.appendChild(createEmptyBlock('当天还没有生成可回看的 5 分钟历史榜单。'));
    return;
  }

  snapshot.history.forEach((entry) => {
    const card = document.createElement('article');
    card.className = 'leader-group-card';

    const top = document.createElement('div');
    top.className = 'leader-group-top';

    const title = document.createElement('strong');
    title.textContent = `${formatClock(entry.comparisonStartedAt)} - ${formatClock(entry.comparisonEndedAt)}`;

    const count = document.createElement('span');
    count.className = 'leader-group-count';
    count.textContent = '5 分钟前十';

    top.append(title, count);

    const subtitle = document.createElement('p');
    subtitle.className = 'panel-caption';
    subtitle.textContent = entry.topLeader
      ? `领涨：${entry.topLeader.name} ${formatPercent(entry.topLeader.windowChangePercent, 2)}`
      : '本区间暂无有效结果';

    const stocks = document.createElement('div');
    stocks.className = 'leader-group-stocks';

    (entry.leaders || []).forEach((leader) => {
      const chip = document.createElement('span');
      chip.className = 'leader-stock-chip';
      chip.textContent = `${String(leader.rank).padStart(2, '0')} ${leader.name} ${formatPercent(leader.windowChangePercent, 2)}`;
      stocks.appendChild(chip);
    });

    card.append(top, subtitle, stocks);
    marketElements.historyGrid.appendChild(card);
  });
}

function renderMarketError(snapshot) {
  if (!snapshot.error) {
    marketElements.errorPanel.classList.add('hidden');
    marketElements.errorPanel.textContent = '';
    return;
  }

  marketElements.errorPanel.classList.remove('hidden');
  marketElements.errorPanel.textContent = `最近一次区间涨幅统计失败：${snapshot.error}`;
}

function renderMarketSnapshot(snapshot) {
  latestMarketSnapshot = snapshot;
  marketElements.statusBadge.textContent = getStatusText(snapshot.status);
  marketElements.statusBadge.dataset.status = snapshot.status;
  marketElements.countdownText.textContent = formatCountdown(snapshot.nextRefreshAt);
  marketElements.coverageText.textContent = String(snapshot.coverageCount || 0);
  marketElements.modeText.textContent = `当前主榜：${snapshot.comparisonModeLabel || '当前 5 分钟榜'}`;
  marketElements.historyCountText.textContent = `${snapshot.history?.length || 0} 个区间`;
  marketElements.windowText.textContent = snapshot.comparisonReady
    ? `${snapshot.windowMinutes ?? '--'} 分钟`
    : '等待下一轮 5 分钟快照';
  marketElements.startText.textContent = formatDateTime(snapshot.comparisonStartedAt);
  marketElements.endText.textContent = formatDateTime(snapshot.comparisonEndedAt || snapshot.lastSuccessAt);

  renderMarketSummary(snapshot.summary);
  renderMarketGroups(snapshot);
  renderMarketLeaders(snapshot);
  renderMarketHistory(snapshot);
  renderMarketError(snapshot);
}

function renderPatternSummary(lines) {
  patternElements.summaryList.innerHTML = '';

  if (!lines || lines.length === 0) {
    const item = document.createElement('li');
    item.textContent = '暂无收盘复盘摘要。';
    patternElements.summaryList.appendChild(item);
    return;
  }

  lines.forEach((line) => {
    const item = document.createElement('li');
    item.textContent = line;
    patternElements.summaryList.appendChild(item);
  });
}

function renderPatternThemes(snapshot) {
  patternElements.themesGrid.innerHTML = '';

  if (!snapshot.topThemes || snapshot.topThemes.length === 0) {
    patternElements.themesGrid.appendChild(createEmptyBlock('收盘后这里会展示前五热门主题与映射到的概念结果。'));
    return;
  }

  snapshot.topThemes.forEach((theme) => {
    const card = document.createElement('article');
    card.className = 'leader-group-card';

    const top = document.createElement('div');
    top.className = 'leader-group-top';

    const title = document.createElement('strong');
    title.textContent = `${String(theme.id).padStart(2, '0')} ${theme.name}`;

    const count = document.createElement('span');
    count.className = 'leader-group-count';
    count.textContent = theme.matchedConcept
      ? `${theme.sampledCandidateCount || 0} 只候选`
      : '待匹配';

    top.append(title, count);

    const meta = document.createElement('p');
    meta.className = 'theme-meta';
    meta.textContent = theme.matchedConcept
      ? `概念成分股 ${theme.constituentCount || 0} 只`
      : '未在概念库里找到精确匹配题材';

    card.append(top, meta);
    patternElements.themesGrid.appendChild(card);
  });
}

function renderPatternPicks(snapshot) {
  patternElements.picksGrid.innerHTML = '';

  if (!snapshot.picks || snapshot.picks.length === 0) {
    patternElements.picksGrid.appendChild(createEmptyBlock('收盘复盘完成后，这里会展示符合模式的 5 只股票。'));
    return;
  }

  snapshot.picks.forEach((pick) => {
    const card = document.createElement('article');
    card.className = 'stock-card';

    const top = document.createElement('div');
    top.className = 'stock-top';

    const rank = document.createElement('span');
    rank.className = 'stock-rank';
    rank.textContent = String(pick.rank).padStart(2, '0');

    const heading = document.createElement('div');
    heading.className = 'stock-heading';

    const name = document.createElement('h4');
    name.textContent = pick.name;

    const code = document.createElement('p');
    code.className = 'stock-code';
    code.textContent = `${pick.code} · 收盘复盘`;

    heading.append(name, code);
    top.append(rank, heading, createChangeBadge(pick.dailyChangePercent));

    const metrics = document.createElement('div');
    metrics.className = 'stock-metrics';

    [
      `收盘涨跌 ${formatPercent(pick.dailyChangePercent, 2)}`,
      `收盘价 ${pick.close ?? '--'}`,
      `MA5 ${pick.ma5 ?? '--'}`,
      `距 MA5 ${formatPercent(pick.aboveMa5Percent, 2)}`,
      `趋势推进 ${pick.runDays || 0} 天`,
      `震荡换手 ${pick.pullbackDays ?? 0} 天`,
      `10日量能抬升 ${formatPercent(pick.volumeCenterLiftPercent ?? pick.volumeExpandPercent, 1)}`,
      `MA5 斜率 ${formatPercent(pick.maSlopePercent, 2)}`,
      `量能支撑 ${pick.volumeSupportDays ?? 0} 天`,
      `换手率 ${formatPercent(pick.turnoverRate, 2)}`,
    ].forEach((text) => {
      const line = document.createElement('p');
      line.className = 'metric-line';
      line.textContent = text;
      metrics.appendChild(line);
    });

    const tags = document.createElement('div');
    tags.className = 'tag-list';
    (pick.matchedThemes || []).forEach((theme) => {
      tags.appendChild(createTag(theme));
    });

    const reasons = document.createElement('ul');
    reasons.className = 'pick-reasons';
    (pick.reasons || []).forEach((reason) => {
      const item = document.createElement('li');
      item.textContent = reason;
      reasons.appendChild(item);
    });

    card.append(top, metrics, tags, reasons);
    patternElements.picksGrid.appendChild(card);
  });
}

function renderPatternError(snapshot) {
  if (!snapshot.error) {
    patternElements.errorPanel.classList.add('hidden');
    patternElements.errorPanel.textContent = '';
    return;
  }

  patternElements.errorPanel.classList.remove('hidden');
  patternElements.errorPanel.textContent = `最近一次收盘复盘失败：${snapshot.error}`;
}

function renderPatternBacktest(snapshot) {
  const backtest = snapshot.backtest || {};
  patternElements.backtestDayCountText.textContent = String(backtest.signalDayCount || 0);
  patternElements.backtestTradeCountText.textContent = String(backtest.totalTrades || 0);
  patternElements.backtestAvgText.textContent = formatPercent(backtest.averageReturnPercent, 2);
  patternElements.backtestCumText.textContent = formatPercent(backtest.cumulativeReturnPercent, 2);
  patternElements.backtestWinRateText.textContent = formatPercent(backtest.dayWinRatePercent, 2);

  let basisText = backtest.basis || '按当前筛选池回放：信号日收盘选股，下一交易日开盘买入，第三交易日收盘卖出';
  if (backtest.available) {
    basisText = `${basisText} 日胜率 ${formatPercent(backtest.dayWinRatePercent, 2)}，单笔胜率 ${formatPercent(backtest.tradeWinRatePercent, 2)}。`;
  }
  patternElements.backtestBasisText.textContent = basisText;

  patternElements.backtestGrid.innerHTML = '';
  if (!backtest.available || !backtest.days || backtest.days.length === 0) {
    patternElements.backtestGrid.appendChild(createEmptyBlock('收盘复盘完成后，这里会展示最近 10 个信号日的 T+1 开盘买 / T+2 收盘卖回测。'));
    return;
  }

  [...backtest.days].reverse().forEach((day) => {
    const card = document.createElement('article');
    card.className = 'stock-card backtest-card';

    const top = document.createElement('div');
    top.className = 'stock-top';

    const heading = document.createElement('div');
    heading.className = 'stock-heading';

    const title = document.createElement('h4');
    title.textContent = day.signalDate || '--';

    const meta = document.createElement('p');
    meta.className = 'stock-code';
    meta.textContent = `${day.entryDate || '--'} 开盘买入 · ${day.exitDate || '--'} 收盘卖出`;

    heading.append(title, meta);
    top.append(heading, createChangeBadge(day.portfolioReturnPercent));

    const metrics = document.createElement('div');
    metrics.className = 'stock-metrics';
    [
      `组合收益 ${formatPercent(day.portfolioReturnPercent, 2)}`,
      `入选股票 ${day.pickCount || 0} 只`,
      `严格候选 ${day.strictCount || 0} 只`,
      `可结算交易 ${day.tradeCount || 0} 笔`,
    ].forEach((text) => {
      const line = document.createElement('p');
      line.className = 'metric-line';
      line.textContent = text;
      metrics.appendChild(line);
    });

    const picks = document.createElement('div');
    picks.className = 'tag-list';
    (day.picks || []).forEach((pick) => {
      const chip = document.createElement('span');
      chip.className = `tag-pill trade-chip ${pick.returnPercent > 0 ? 'up' : pick.returnPercent < 0 ? 'down' : 'flat'}`;
      chip.textContent = `${String(pick.rank || '--').padStart(2, '0')} ${pick.name} ${formatPercent(pick.returnPercent, 2)}`;
      picks.appendChild(chip);
    });

    if (!day.picks || day.picks.length === 0) {
      picks.appendChild(createEmptyBlock('当天没有形成可结算的回测交易。'));
    }

    card.append(top, metrics, picks);
    patternElements.backtestGrid.appendChild(card);
  });
}

function renderPatternSnapshot(snapshot) {
  latestPatternSnapshot = snapshot;
  patternElements.statusBadge.textContent = getStatusText(snapshot.status);
  patternElements.statusBadge.dataset.status = snapshot.status;
  patternElements.countdownText.textContent = formatCountdown(snapshot.nextRefreshAt);
  patternElements.dayText.textContent = snapshot.day || '--';
  patternElements.themeCountText.textContent = String(snapshot.topThemes?.length || 0);
  patternElements.matchCountText.textContent = String(snapshot.filterCounts?.strictMatchCount || 0);
  patternElements.lastSuccessText.textContent = formatDateTime(snapshot.lastSuccessAt);

  renderPatternSummary(snapshot.summary);
  renderPatternThemes(snapshot);
  renderPatternBacktest(snapshot);
  renderPatternPicks(snapshot);
  setPanelMessage(patternElements.warningPanel, snapshot.warnings, 'warning-line');
  renderPatternError(snapshot);
}
function renderSummary(lines) {
  signalElements.summaryList.innerHTML = '';

  if (!lines || lines.length === 0) {
    const item = document.createElement('li');
    item.textContent = '暂无综合判断。';
    signalElements.summaryList.appendChild(item);
    return;
  }

  lines.forEach((line) => {
    const item = document.createElement('li');
    item.textContent = line;
    signalElements.summaryList.appendChild(item);
  });
}

function renderSpeakerGrid(speakers) {
  signalElements.speakerGrid.innerHTML = '';

  if (!speakers || speakers.length === 0) {
    signalElements.speakerGrid.appendChild(createEmptyBlock('还没有可展示的重点发言人信息。'));
    return;
  }

  speakers.forEach((speaker) => {
    const card = document.createElement('article');
    card.className = 'speaker-card';

    const top = document.createElement('div');
    top.className = 'speaker-top';

    const title = document.createElement('h4');
    title.textContent = speaker.speaker;

    top.append(title, createBiasBadge(speaker.bias));

    const meta = document.createElement('p');
    meta.className = 'speaker-meta';
    meta.textContent = `最近发言 ${speaker.messageCount || 0} 条 · 最近时间 ${formatDateTime(speaker.lastTimestamp)}`;

    const summary = document.createElement('p');
    summary.className = 'speaker-summary';
    summary.textContent = speaker.summary;

    const snippets = document.createElement('div');
    snippets.className = 'snippet-list';

    if (!speaker.snippets || speaker.snippets.length === 0) {
      snippets.appendChild(createEmptyBlock('暂无可展示的发言摘录。'));
    } else {
      speaker.snippets.forEach((snippet) => {
        const line = document.createElement('p');
        line.className = 'snippet-item';
        line.textContent = snippet;
        snippets.appendChild(line);
      });
    }

    card.append(top, meta, summary, snippets);
    signalElements.speakerGrid.appendChild(card);
  });
}

function renderMessageList(messages, container, emptyText, type) {
  container.innerHTML = '';

  if (!messages || messages.length === 0) {
    container.appendChild(createEmptyBlock(emptyText));
    return;
  }

  messages.forEach((message) => {
    const item = document.createElement('article');
    item.className = `timeline-item ${message.isFocusedSpeaker ? 'focused' : ''}`;

    const top = document.createElement('div');
    top.className = 'timeline-top';

    const title = document.createElement('strong');
    title.textContent = type === 'article' ? (message.title || '未命名文章') : (message.sender || '未知发言人');

    const meta = document.createElement('span');
    meta.className = 'timeline-time';
    meta.textContent = formatDateTime(message.timestamp);

    top.append(title, meta, createBiasBadge(message.bias, 'inline-bias small'));

    const source = document.createElement('p');
    source.className = 'timeline-source';
    source.textContent = type === 'article'
      ? (message.sourceName || '子明解读')
      : (message.chatName || '子明和他的朋友们');

    const text = document.createElement('p');
    text.className = 'timeline-text';
    text.textContent = message.snippet;

    item.append(top, source, text);
    container.appendChild(item);
  });
}

function renderWarnings(warnings) {
  setPanelMessage(signalElements.warningPanel, warnings, 'warning-line');
}

function renderSignalError(snapshot) {
  if (!snapshot.error) {
    signalElements.errorPanel.classList.add('hidden');
    signalElements.errorPanel.textContent = '';
    return;
  }

  signalElements.errorPanel.classList.remove('hidden');
  signalElements.errorPanel.textContent = `最近一次分析失败：${snapshot.error}`;
}

function renderSignalSnapshot(snapshot) {
  latestSignalSnapshot = snapshot;

  const signal = snapshot.signal || {};
  const bias = signal.bias || '信号不足';

  signalElements.statusBadge.textContent = getStatusText(snapshot.status);
  signalElements.statusBadge.dataset.status = snapshot.status;
  signalElements.countdownText.textContent = formatCountdown(snapshot.nextRefreshAt);
  signalElements.biasText.textContent = bias;
  signalElements.biasText.className = `bias-badge ${getBiasTone(bias)}`;
  signalElements.confidenceText.textContent = `置信度：${getConfidenceText(signal.confidence)}`;
  signalElements.techScoreText.textContent = String(signal.techScore || 0);
  signalElements.warScoreText.textContent = String(signal.warScore || 0);
  signalElements.techKeywordsText.textContent = formatKeywords(signal.topKeywords?.tech);
  signalElements.warKeywordsText.textContent = formatKeywords(signal.topKeywords?.war);
  signalElements.importDirText.textContent = snapshot.importDir || '--';
  signalElements.importFileCountText.textContent = String(snapshot.importFileCount || 0);
  signalElements.lastSuccessText.textContent = formatDateTime(snapshot.lastSuccessAt);
  signalElements.lastAttemptText.textContent = formatDateTime(snapshot.lastAttemptAt);
  signalElements.analyzedCountText.textContent = `${snapshot.analyzedMessageCount || 0} 条群消息`;

  renderSummary(snapshot.overallSummary);
  renderSpeakerGrid(snapshot.focusedSpeakers);
  renderMessageList(snapshot.recentMessages, signalElements.messagesList, '暂无可展示的群消息。', 'message');
  renderMessageList(snapshot.recentArticles, signalElements.articlesList, '暂无可展示的公众号文章。', 'article');
  renderWarnings(snapshot.warnings);
  renderSignalError(snapshot);
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    cache: 'no-store',
    ...options,
  });

  if (!response.ok) {
    throw new Error(`请求失败：HTTP ${response.status}`);
  }

  return response.json();
}

async function loadHotSnapshot() {
  const snapshot = await requestJson('/api/hot-themes');
  renderHotSnapshot(snapshot);
}

async function loadMarketSnapshot() {
  const snapshot = await requestJson('/api/market-leaders');
  renderMarketSnapshot(snapshot);
}

async function loadPatternSnapshot() {
  const snapshot = await requestJson('/api/pattern-picks');
  renderPatternSnapshot(snapshot);
}
async function refreshHotNow() {
  hotElements.refreshButton.disabled = true;
  hotElements.refreshButton.textContent = '刷新中…';

  const [hotResult, marketResult, patternResult] = await Promise.allSettled([
    requestJson('/api/hot-themes/refresh', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    }),
    requestJson('/api/market-leaders/refresh', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    }),
    requestJson('/api/pattern-picks/refresh', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    }),
  ]);

  if (hotResult.status === 'fulfilled') {
    renderHotSnapshot(hotResult.value);
  } else {
    hotElements.errorPanel.classList.remove('hidden');
    hotElements.errorPanel.textContent = `主题刷新失败：${hotResult.reason.message}`;
  }

  if (marketResult.status === 'fulfilled') {
    renderMarketSnapshot(marketResult.value);
  } else {
    marketElements.errorPanel.classList.remove('hidden');
    marketElements.errorPanel.textContent = `区间涨幅刷新失败：${marketResult.reason.message}`;
  }

  if (patternResult.status === 'fulfilled') {
    renderPatternSnapshot(patternResult.value);
  } else {
    patternElements.errorPanel.classList.remove('hidden');
    patternElements.errorPanel.textContent = `收盘复盘刷新失败：${patternResult.reason.message}`;
  }

  hotElements.refreshButton.disabled = false;
  hotElements.refreshButton.textContent = '刷新同花顺';
}

async function loadSignalSnapshot() {
  const snapshot = await requestJson('/api/wechat-signal');
  renderSignalSnapshot(snapshot);
}

async function refreshSignalNow() {
  signalElements.refreshButton.disabled = true;
  signalElements.refreshButton.textContent = '分析中…';

  try {
    const snapshot = await requestJson('/api/wechat-signal/refresh', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    renderSignalSnapshot(snapshot);
  } catch (error) {
    signalElements.errorPanel.classList.remove('hidden');
    signalElements.errorPanel.textContent = `重新分析失败：${error.message}`;
  } finally {
    signalElements.refreshButton.disabled = false;
    signalElements.refreshButton.textContent = '重新分析';
  }
}

function updateCountdowns() {
  if (latestHotSnapshot) {
    hotElements.countdownText.textContent = formatCountdown(latestHotSnapshot.nextRefreshAt);
  }

  if (latestMarketSnapshot) {
    marketElements.countdownText.textContent = formatCountdown(latestMarketSnapshot.nextRefreshAt);
  }

  if (latestPatternSnapshot) {
    patternElements.countdownText.textContent = formatCountdown(latestPatternSnapshot.nextRefreshAt);
  }

  if (latestSignalSnapshot) {
    signalElements.countdownText.textContent = formatCountdown(latestSignalSnapshot.nextRefreshAt);
  }
}

hotElements.refreshButton.addEventListener('click', () => {
  refreshHotNow().catch((error) => {
    hotElements.errorPanel.classList.remove('hidden');
    hotElements.errorPanel.textContent = `同花顺刷新失败：${error.message}`;
  });
});

signalElements.refreshButton.addEventListener('click', refreshSignalNow);

loadHotSnapshot().catch((error) => {
  hotElements.errorPanel.classList.remove('hidden');
  hotElements.errorPanel.textContent = `初始化失败：${error.message}`;
});

loadMarketSnapshot().catch((error) => {
  marketElements.errorPanel.classList.remove('hidden');
  marketElements.errorPanel.textContent = `初始化失败：${error.message}`;
});

loadPatternSnapshot().catch((error) => {
  patternElements.errorPanel.classList.remove('hidden');
  patternElements.errorPanel.textContent = `初始化失败：${error.message}`;
});

loadSignalSnapshot().catch((error) => {
  signalElements.errorPanel.classList.remove('hidden');
  signalElements.errorPanel.textContent = `初始化失败：${error.message}`;
});

setInterval(updateCountdowns, 1000);
setInterval(() => {
  loadHotSnapshot().catch((error) => {
    hotElements.errorPanel.classList.remove('hidden');
    hotElements.errorPanel.textContent = `状态更新失败：${error.message}`;
  });

  loadMarketSnapshot().catch((error) => {
    marketElements.errorPanel.classList.remove('hidden');
    marketElements.errorPanel.textContent = `状态更新失败：${error.message}`;
  });

  loadPatternSnapshot().catch((error) => {
    patternElements.errorPanel.classList.remove('hidden');
    patternElements.errorPanel.textContent = `状态更新失败：${error.message}`;
  });

  loadSignalSnapshot().catch((error) => {
    signalElements.errorPanel.classList.remove('hidden');
    signalElements.errorPanel.textContent = `状态更新失败：${error.message}`;
  });
}, 60 * 1000);

