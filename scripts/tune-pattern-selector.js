const fs = require('fs');
const path = require('path');
const Module = require('module');

const TOP_PICK_COUNT = 5;
const DEFAULT_VARIANT = {
  burstWindow: 3,
  maxAboveMa5Percent: 8,
  maxBottomLiftPercent: 50,
  maxNearBottomLiftPercent: 62,
  maxNearTrendReturnPercent: 38,
  maxPullbackDays: 3,
  maxStableAboveMa5Percent: 9,
  maxStableBottomLiftPercent: 55,
  maxStableTrendReturnPercent: 28,
  maxTrendReturnPercent: 30,
  minBottomLiftPercent: 8,
  minNearBottomLiftPercent: 5,
  minNearTrendReturnPercent: 3,
  minShortVolumeLiftPercent: 0,
  minStableBottomLiftPercent: 12,
  minStableTrendReturnPercent: 8,
  minTrendReturnPercent: 5,
  minVolumeCenterLiftPercent: 3,
  minVolumeSupportDays: 3,
  minStableVolumeSupportDays: 4,
  name: 'default',
  nearVolumeCenterFloorPercent: -2,
  preferredAboveMa5Percent: 3.5,
  preferredRunDays: 6,
  preferredRunReturnPercent: 14,
  stableMaSlopeMaxPercent: 10,
  stableMaSlopeMinPercent: 1.5,
  strictMaSlopeMaxPercent: 10,
  strictMaSlopeMinPercent: 0.5,
  supportThresholdRatio: 1.03,
  supportWindow: 6,
  trendBaseWindow: 20,
  trendWindow: 10,
  volumeLongWindow: 12,
  volumeShortWindow: 6,
};

function loadServiceInternals() {
  const servicePath = path.resolve(__dirname, '..', 'patternSelectorService.js');
  const source = `${fs.readFileSync(servicePath, 'utf8')}
module.exports.__internals = {
  KLINE_CONCURRENCY,
  MA_TOLERANCE_RATIO,
  MIN_BAR_COUNT,
  addMa5,
  average,
  buildCandidatePool,
  countNearOrAboveMa5,
  ensureCacheLoaded,
  ensureTradingDayState,
  fetchDailyBars,
  fetchMarketSnapshot,
  getDailyChangePercent,
  getTopThemes,
  getMaxConsecutiveBelowMa5,
  mapWithConcurrency,
  pruneCaches,
  roundNumber,
  state,
  syncSchedule,
};`;
  const loadedModule = new Module(servicePath, module);
  loadedModule.filename = servicePath;
  loadedModule.paths = Module._nodeModulePaths(path.dirname(servicePath));
  loadedModule._compile(source, servicePath);
  return loadedModule.exports.__internals;
}

function roundNumber(value, digits = 2) {
  if (!Number.isFinite(value)) {
    return null;
  }

  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function buildVolumeMetrics(bars, config, helpers) {
  const shortBars = bars.slice(-config.volumeShortWindow);
  const longBars = bars.slice(-config.volumeLongWindow);
  const supportBars = bars.slice(-config.supportWindow);
  const burstBars = shortBars.slice(-Math.min(config.burstWindow, shortBars.length));
  const volumeShortAvg = helpers.average(shortBars.map((bar) => bar.volume));
  const volumeLongAvg = helpers.average(longBars.map((bar) => bar.volume));
  const volumeBurstAvg = helpers.average(burstBars.map((bar) => bar.volume));
  const volumeCenterLiftPercent = Number.isFinite(volumeShortAvg) && Number.isFinite(volumeLongAvg) && volumeLongAvg > 0
    ? ((volumeShortAvg - volumeLongAvg) / volumeLongAvg) * 100
    : null;
  const shortVolumeLiftPercent = Number.isFinite(volumeBurstAvg) && Number.isFinite(volumeShortAvg) && volumeShortAvg > 0
    ? ((volumeBurstAvg - volumeShortAvg) / volumeShortAvg) * 100
    : null;
  const volumeSupportDays = Number.isFinite(volumeLongAvg)
    ? supportBars.filter((bar) => bar.volume >= volumeLongAvg * config.supportThresholdRatio).length
    : 0;

  return {
    shortVolumeLiftPercent,
    volumeBurstAvg,
    volumeCenterLiftPercent,
    volumeLongAvg,
    volumeShortAvg,
    volumeSupportDays,
  };
}

function buildTrendMetrics(bars, config) {
  const recentBars = bars.slice(-config.trendWindow);
  const baseBars = bars.slice(-config.trendBaseWindow);
  let advanceDays = recentBars.length > 0 ? 1 : 0;
  let pullbackDays = 0;

  for (let index = 1; index < recentBars.length; index += 1) {
    if (recentBars[index].close >= recentBars[index - 1].close * 0.995) {
      advanceDays += 1;
    } else {
      pullbackDays += 1;
    }
  }

  const startBar = recentBars[0];
  const lastBar = recentBars[recentBars.length - 1];
  const baseLow = baseBars.length > 0 ? Math.min(...baseBars.map((bar) => bar.low)) : null;
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

function evaluateCandidate(candidate, config, helpers) {
  if (!Array.isArray(candidate.bars) || candidate.bars.length < helpers.MIN_BAR_COUNT) {
    return {
      code: candidate.code,
      error: 'bar_count_insufficient',
      name: candidate.name,
      passed: false,
    };
  }

  const bars = helpers.addMa5(candidate.bars);
  const recentBars = bars.slice(-config.trendWindow).filter((item) => Number.isFinite(item.ma5));
  const lastBar = bars[bars.length - 1];
  const maReferenceBar = bars[bars.length - 5] || lastBar;
  const aboveMa5Percent = Number.isFinite(lastBar.ma5) && lastBar.ma5 !== 0
    ? ((lastBar.close - lastBar.ma5) / lastBar.ma5) * 100
    : null;
  const maSlopePercent = Number.isFinite(lastBar.ma5) && Number.isFinite(maReferenceBar.ma5) && maReferenceBar.ma5 !== 0
    ? ((lastBar.ma5 - maReferenceBar.ma5) / Math.abs(maReferenceBar.ma5)) * 100
    : 0;
  const maxBelowMa5Streak = helpers.getMaxConsecutiveBelowMa5(recentBars);
  const recentAboveCount = helpers.countNearOrAboveMa5(recentBars);
  const volumeMetrics = buildVolumeMetrics(bars, config, helpers);
  const trendMetrics = buildTrendMetrics(bars, config);
  const passMa = recentBars.length >= 8
    && maxBelowMa5Streak < 2
    && recentAboveCount >= Math.max(recentBars.length - 3, 6)
    && maSlopePercent > config.strictMaSlopeMinPercent
    && maSlopePercent <= config.strictMaSlopeMaxPercent
    && lastBar.close >= lastBar.ma5 * (1 - helpers.MA_TOLERANCE_RATIO);
  const passVolume = Number.isFinite(volumeMetrics.volumeCenterLiftPercent)
    && volumeMetrics.volumeCenterLiftPercent >= config.minVolumeCenterLiftPercent
    && volumeMetrics.volumeSupportDays >= config.minVolumeSupportDays
    && Number.isFinite(volumeMetrics.shortVolumeLiftPercent)
    && volumeMetrics.shortVolumeLiftPercent >= config.minShortVolumeLiftPercent;
  const strictStructure = Number.isFinite(trendMetrics.trendReturnPercent)
    && trendMetrics.trendReturnPercent >= config.minTrendReturnPercent
    && trendMetrics.trendReturnPercent <= config.maxTrendReturnPercent
    && Number.isFinite(trendMetrics.bottomLiftPercent)
    && trendMetrics.bottomLiftPercent >= config.minBottomLiftPercent
    && trendMetrics.bottomLiftPercent <= config.maxBottomLiftPercent
    && trendMetrics.pullbackDays <= config.maxPullbackDays;
  const stableTrendRide = recentBars.length >= 8
    && maxBelowMa5Streak === 0
    && recentAboveCount >= Math.max(recentBars.length - 1, 8)
    && Number.isFinite(aboveMa5Percent)
    && aboveMa5Percent >= 0
    && aboveMa5Percent <= config.maxStableAboveMa5Percent
    && maSlopePercent >= config.stableMaSlopeMinPercent
    && maSlopePercent <= config.stableMaSlopeMaxPercent
    && Number.isFinite(trendMetrics.trendReturnPercent)
    && trendMetrics.trendReturnPercent >= config.minStableTrendReturnPercent
    && trendMetrics.trendReturnPercent <= config.maxStableTrendReturnPercent
    && Number.isFinite(trendMetrics.bottomLiftPercent)
    && trendMetrics.bottomLiftPercent >= config.minStableBottomLiftPercent
    && trendMetrics.bottomLiftPercent <= config.maxStableBottomLiftPercent
    && trendMetrics.pullbackDays <= config.maxPullbackDays + 1
    && volumeMetrics.volumeSupportDays >= config.minStableVolumeSupportDays;
  const passDeviation = Number.isFinite(aboveMa5Percent)
    && aboveMa5Percent >= -1
    && (aboveMa5Percent <= config.maxAboveMa5Percent || stableTrendRide);
  const nearTrendCandidate = recentBars.length >= 8
    && maxBelowMa5Streak < 2
    && recentAboveCount >= Math.max(recentBars.length - 3, 6)
    && maSlopePercent > 0
    && maSlopePercent <= 12
    && Number.isFinite(aboveMa5Percent)
    && aboveMa5Percent >= -2
    && aboveMa5Percent <= 9
    && Number.isFinite(trendMetrics.trendReturnPercent)
    && trendMetrics.trendReturnPercent >= config.minNearTrendReturnPercent
    && trendMetrics.trendReturnPercent <= config.maxNearTrendReturnPercent
    && Number.isFinite(trendMetrics.bottomLiftPercent)
    && trendMetrics.bottomLiftPercent >= config.minNearBottomLiftPercent
    && trendMetrics.bottomLiftPercent <= config.maxNearBottomLiftPercent
    && trendMetrics.pullbackDays <= config.maxPullbackDays + 1
    && ((Number.isFinite(volumeMetrics.volumeCenterLiftPercent) && volumeMetrics.volumeCenterLiftPercent >= config.nearVolumeCenterFloorPercent)
      || volumeMetrics.volumeSupportDays >= Math.max(config.minVolumeSupportDays - 1, 2));
  const passed = passMa && passVolume && passDeviation && (strictStructure || stableTrendRide);
  const dailyChangePercent = Number.isFinite(candidate.dailyChangePercent)
    ? candidate.dailyChangePercent
    : helpers.getDailyChangePercent(lastBar);
  const deviationPenalty = Number.isFinite(aboveMa5Percent)
    ? Math.abs(aboveMa5Percent - config.preferredAboveMa5Percent)
    : 20;
  const runPenalty = Number.isFinite(trendMetrics.advanceDays)
    ? Math.abs(trendMetrics.advanceDays - config.preferredRunDays)
    : 5;
  const runReturnPenalty = Number.isFinite(trendMetrics.trendReturnPercent)
    ? Math.abs(trendMetrics.trendReturnPercent - config.preferredRunReturnPercent)
    : 15;
  const score = ((candidate.themeRankScore || 0) * 8)
    + (recentAboveCount * 4)
    + ((volumeMetrics.volumeSupportDays || 0) * 3)
    + Math.min(trendMetrics.trendReturnPercent || 0, 25)
    + (volumeMetrics.volumeCenterLiftPercent || 0)
    + (volumeMetrics.shortVolumeLiftPercent || 0)
    + (dailyChangePercent || 0)
    - (deviationPenalty * 2)
    - (runPenalty * 1.5)
    - (runReturnPenalty * 0.5)
    - (Math.max((trendMetrics.bottomLiftPercent || 0) - config.maxBottomLiftPercent, 0) * 1.2)
    - (Math.max(maSlopePercent - 8, 0) * 1.5);

  return {
    aboveMa5Percent: roundNumber(aboveMa5Percent, 2),
    bottomLiftPercent: roundNumber(trendMetrics.bottomLiftPercent, 2),
    code: candidate.code,
    close: roundNumber(lastBar.close, 3),
    dailyChangePercent: roundNumber(dailyChangePercent, 2),
    ma5: roundNumber(lastBar.ma5, 3),
    maSlopePercent: roundNumber(maSlopePercent, 2),
    matchedThemes: candidate.matchedThemes,
    maxBelowMa5Streak,
    name: candidate.name,
    nearTrendCandidate,
    passed,
    pullbackDays: trendMetrics.pullbackDays,
    recentAboveCount,
    recentWindowLength: recentBars.length,
    runDays: trendMetrics.advanceDays || 0,
    runReturnPercent: roundNumber(trendMetrics.trendReturnPercent || 0, 2),
    score: roundNumber(score, 2),
    stableTrendRide,
    themeRankScore: candidate.themeRankScore || 0,
    turnoverRate: roundNumber(candidate.turnoverRate, 2),
    volumeCenterLiftPercent: roundNumber(volumeMetrics.volumeCenterLiftPercent, 1),
    volumeLongAvg: roundNumber(volumeMetrics.volumeLongAvg, 0),
    volumeShortAvg: roundNumber(volumeMetrics.volumeShortAvg, 0),
    volumeSupportDays: volumeMetrics.volumeSupportDays,
  };
}

function compareDescending(left, right, key) {
  return (right[key] || 0) - (left[key] || 0);
}

function buildFreshnessPenalty(item, config) {
  const runDays = item.runDays || 0;
  const runReturn = item.runReturnPercent || 0;
  return Math.abs(runDays - config.preferredRunDays) * 2
    + Math.abs(runReturn - config.preferredRunReturnPercent) * 0.3
    + Math.abs((item.aboveMa5Percent || 0) - config.preferredAboveMa5Percent) * 1.5;
}

function sortStrictMatches(left, right, config) {
  if ((right.themeRankScore || 0) !== (left.themeRankScore || 0)) {
    return compareDescending(left, right, 'themeRankScore');
  }

  const leftPenalty = buildFreshnessPenalty(left, config);
  const rightPenalty = buildFreshnessPenalty(right, config);
  if (leftPenalty !== rightPenalty) {
    return leftPenalty - rightPenalty;
  }

  if ((right.volumeSupportDays || 0) !== (left.volumeSupportDays || 0)) {
    return compareDescending(left, right, 'volumeSupportDays');
  }

  return compareDescending(left, right, 'score');
}

function sortFallbackMatches(left, right) {
  if ((right.themeRankScore || 0) !== (left.themeRankScore || 0)) {
    return compareDescending(left, right, 'themeRankScore');
  }

  if ((right.recentAboveCount || 0) !== (left.recentAboveCount || 0)) {
    return compareDescending(left, right, 'recentAboveCount');
  }

  return compareDescending(left, right, 'score');
}

function sortReserveMatches(left, right, config) {
  if ((right.themeRankScore || 0) !== (left.themeRankScore || 0)) {
    return compareDescending(left, right, 'themeRankScore');
  }

  const leftDeviation = Math.abs((left.aboveMa5Percent ?? 99) - config.preferredAboveMa5Percent);
  const rightDeviation = Math.abs((right.aboveMa5Percent ?? 99) - config.preferredAboveMa5Percent);
  if (leftDeviation !== rightDeviation) {
    return leftDeviation - rightDeviation;
  }

  return compareDescending(left, right, 'score');
}

function selectPicks(evaluationResults, config) {
  const strictMatches = evaluationResults
    .filter((item) => item && item.passed)
    .sort((left, right) => sortStrictMatches(left, right, config));
  const strictPicks = strictMatches
    .slice(0, TOP_PICK_COUNT)
    .map((item) => ({
      ...item,
      selectionMode: 'strict',
    }));
  const selectedCodes = new Set(strictPicks.map((item) => item.code));
  const fallbackMatches = evaluationResults
    .filter((item) => item && !item.error && !selectedCodes.has(item.code) && item.nearTrendCandidate)
    .sort(sortFallbackMatches);
  const fallbackPicks = fallbackMatches
    .slice(0, Math.max(0, TOP_PICK_COUNT - strictPicks.length))
    .map((item) => ({
      ...item,
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
    .sort((left, right) => sortReserveMatches(left, right, config));
  const reservePicks = reserveMatches
    .slice(0, Math.max(0, TOP_PICK_COUNT - strictPicks.length - fallbackPicks.length))
    .map((item) => ({
      ...item,
      selectionMode: 'reserve',
    }));

  return {
    picks: [...strictPicks, ...fallbackPicks, ...reservePicks]
      .slice(0, TOP_PICK_COUNT)
      .map((item, index) => ({
        ...item,
        rank: index + 1,
      })),
    strictMatches,
  };
}

function average(values) {
  if (!Array.isArray(values) || values.length === 0) {
    return null;
  }

  const total = values.reduce((sum, value) => sum + value, 0);
  return total / values.length;
}

function buildBacktest(candidateRecords, config, helpers) {
  const usableRecords = candidateRecords.filter((item) => item && !item.error && Array.isArray(item.bars) && item.bars.length >= helpers.MIN_BAR_COUNT + 2);
  if (usableRecords.length === 0) {
    return null;
  }

  const referenceBars = usableRecords
    .map((item) => item.bars)
    .sort((left, right) => right.length - left.length)[0];
  const signalBars = referenceBars.slice(-12, -2);
  const days = signalBars.map((signalBar) => {
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

      return evaluateCandidate({
        ...record.candidate,
        dailyChangePercent: null,
        turnoverRate: null,
        bars: record.bars.slice(0, signalIndex + 1),
      }, config, helpers);
    });
    const selection = selectPicks(historicalResults, config);
    const trades = selection.picks
      .map((pick) => {
        const record = usableRecords.find((item) => item.candidate.code === pick.code);
        const signalIndex = record?.bars.findIndex((bar) => bar.date === signalDate) ?? -1;
        const entryBar = signalIndex >= 0 ? record.bars[signalIndex + 1] : null;
        const exitBar = signalIndex >= 0 ? record.bars[signalIndex + 2] : null;
        if (!entryBar || !exitBar || !Number.isFinite(entryBar.open) || !Number.isFinite(exitBar.close) || entryBar.open <= 0) {
          return null;
        }

        return {
          code: pick.code,
          name: pick.name,
          returnPercent: roundNumber(((exitBar.close - entryBar.open) / entryBar.open) * 100, 2),
          selectionMode: pick.selectionMode,
        };
      })
      .filter(Boolean);
    const portfolioReturnPercent = average(trades.map((item) => item.returnPercent));
    return {
      pickCount: selection.picks.length,
      portfolioReturnPercent: roundNumber(portfolioReturnPercent, 2),
      signalDate,
      strictCount: selection.picks.filter((item) => item.selectionMode === 'strict').length,
      tradeCount: trades.length,
      trades,
    };
  });
  const settledDays = days.filter((item) => Number.isFinite(item.portfolioReturnPercent));
  const averageReturnPercent = average(settledDays.map((item) => item.portfolioReturnPercent));
  const cumulativeReturnPercent = settledDays.reduce((accumulator, item) => accumulator * (1 + (item.portfolioReturnPercent / 100)), 1);
  const positiveDays = settledDays.filter((item) => item.portfolioReturnPercent > 0).length;
  const totalTrades = settledDays.reduce((sum, item) => sum + item.tradeCount, 0);
  const winningTrades = settledDays.reduce((sum, item) => sum + item.trades.filter((trade) => trade.returnPercent > 0).length, 0);
  const strictPickAverage = average(settledDays.map((item) => item.strictCount));

  return {
    averageReturnPercent: roundNumber(averageReturnPercent, 2),
    cumulativeReturnPercent: roundNumber((cumulativeReturnPercent - 1) * 100, 2),
    dayWinRatePercent: settledDays.length > 0 ? roundNumber((positiveDays / settledDays.length) * 100, 2) : null,
    signalDayCount: settledDays.length,
    strictPickAverage: roundNumber(strictPickAverage, 2),
    totalTrades,
    tradeWinRatePercent: totalTrades > 0 ? roundNumber((winningTrades / totalTrades) * 100, 2) : null,
  };
}

function buildVariants() {
  const variants = [
    {
      ...DEFAULT_VARIANT,
      burstWindow: 5,
      maxAboveMa5Percent: 8.5,
      maxBottomLiftPercent: 55,
      maxNearBottomLiftPercent: 70,
      maxNearTrendReturnPercent: 45,
      maxStableAboveMa5Percent: 10,
      maxStableBottomLiftPercent: 60,
      maxStableTrendReturnPercent: 30,
      maxTrendReturnPercent: 40,
      minShortVolumeLiftPercent: -999,
      minVolumeCenterLiftPercent: 5,
      minVolumeSupportDays: 4,
      minStableVolumeSupportDays: 6,
      name: 'baseline',
      preferredAboveMa5Percent: 5,
      preferredRunDays: 8,
      preferredRunReturnPercent: 20,
      supportThresholdRatio: 1.02,
      supportWindow: 10,
      volumeLongWindow: 20,
      volumeShortWindow: 10,
    },
  ];

  const shortWindows = [5, 6, 7];
  const longWindows = [10, 12, 14];
  const minCenterLifts = [2, 3, 4];
  const minSupportDays = [3, 4];
  const minShortLifts = [0, 2];
  const maxTrendReturns = [26, 30];
  const maxBottomLifts = [42, 48];
  const preferredRunDays = [5, 6, 7];

  shortWindows.forEach((volumeShortWindow) => {
    longWindows.forEach((volumeLongWindow) => {
      if (volumeLongWindow <= volumeShortWindow) {
        return;
      }

      minCenterLifts.forEach((minVolumeCenterLiftPercent) => {
        minSupportDays.forEach((minVolumeSupportDays) => {
          minShortLifts.forEach((minShortVolumeLiftPercent) => {
            maxTrendReturns.forEach((maxTrendReturnPercent) => {
              maxBottomLifts.forEach((maxBottomLiftPercent) => {
                preferredRunDays.forEach((preferredRunDay) => {
                  variants.push({
                    ...DEFAULT_VARIANT,
                    maxBottomLiftPercent,
                    maxNearBottomLiftPercent: maxBottomLiftPercent + 12,
                    maxNearTrendReturnPercent: maxTrendReturnPercent + 8,
                    maxStableBottomLiftPercent: maxBottomLiftPercent + 7,
                    maxStableTrendReturnPercent: Math.min(maxTrendReturnPercent, 28),
                    maxTrendReturnPercent,
                    minShortVolumeLiftPercent,
                    minStableVolumeSupportDays: Math.min(minVolumeSupportDays + 1, volumeShortWindow),
                    minVolumeCenterLiftPercent,
                    minVolumeSupportDays,
                    name: `v${volumeShortWindow}-${volumeLongWindow}_lift${minVolumeCenterLiftPercent}_sup${minVolumeSupportDays}_burst${minShortVolumeLiftPercent}_run${maxTrendReturnPercent}_bot${maxBottomLiftPercent}_pref${preferredRunDay}`,
                    preferredRunDays: preferredRunDay,
                    preferredRunReturnPercent: Math.min(maxTrendReturnPercent - 8, 16),
                    supportThresholdRatio: volumeShortWindow <= 5 ? 1.02 : 1.03,
                    supportWindow: volumeShortWindow,
                    volumeLongWindow,
                    volumeShortWindow,
                  });
                });
              });
            });
          });
        });
      });
    });
  });

  return variants;
}

async function buildCandidateRecords(internals) {
  await internals.ensureCacheLoaded();
  internals.ensureTradingDayState(new Date());
  internals.pruneCaches(internals.state.day);
  internals.syncSchedule(new Date());
  const [topThemes, marketSnapshot] = await Promise.all([
    internals.getTopThemes(),
    internals.fetchMarketSnapshot(),
  ]);
  const candidateContext = await internals.buildCandidatePool(topThemes, marketSnapshot);
  const candidateRecords = await internals.mapWithConcurrency(
    candidateContext.candidates,
    internals.KLINE_CONCURRENCY,
    async (candidate) => {
      try {
        const bars = await internals.fetchDailyBars(candidate.code, internals.state.day);
        return {
          bars,
          candidate,
        };
      } catch (error) {
        return {
          bars: [],
          candidate,
          error: error.message,
        };
      }
    },
  );

  return {
    candidateContext,
    candidateRecords,
    topThemes,
  };
}

async function main() {
  const internals = loadServiceInternals();
  const { candidateContext, candidateRecords, topThemes } = await buildCandidateRecords(internals);
  const variants = buildVariants();
  const summaries = variants
    .map((variant) => {
      const backtest = buildBacktest(candidateRecords, variant, internals);
      return {
        ...variant,
        backtest,
      };
    })
    .filter((item) => item.backtest && Number.isFinite(item.backtest.dayWinRatePercent))
    .sort((left, right) => {
      if (right.backtest.dayWinRatePercent !== left.backtest.dayWinRatePercent) {
        return right.backtest.dayWinRatePercent - left.backtest.dayWinRatePercent;
      }

      if (right.backtest.tradeWinRatePercent !== left.backtest.tradeWinRatePercent) {
        return right.backtest.tradeWinRatePercent - left.backtest.tradeWinRatePercent;
      }

      if (right.backtest.averageReturnPercent !== left.backtest.averageReturnPercent) {
        return right.backtest.averageReturnPercent - left.backtest.averageReturnPercent;
      }

      return right.backtest.cumulativeReturnPercent - left.backtest.cumulativeReturnPercent;
    });

  const output = {
    generatedAt: new Date().toISOString(),
    summary: {
      candidateCount: candidateContext.candidateCount,
      themeMemberCount: candidateContext.themeMemberCount,
      topThemes: topThemes.map((item) => item.name),
      usableRecordCount: candidateRecords.filter((item) => !item.error && item.bars.length >= internals.MIN_BAR_COUNT + 2).length,
      variantCount: variants.length,
    },
    topResults: summaries.slice(0, 20).map((item) => ({
      backtest: item.backtest,
      config: {
        maxBottomLiftPercent: item.maxBottomLiftPercent,
        maxTrendReturnPercent: item.maxTrendReturnPercent,
        minShortVolumeLiftPercent: item.minShortVolumeLiftPercent,
        minVolumeCenterLiftPercent: item.minVolumeCenterLiftPercent,
        minVolumeSupportDays: item.minVolumeSupportDays,
        preferredRunDays: item.preferredRunDays,
        supportThresholdRatio: item.supportThresholdRatio,
        supportWindow: item.supportWindow,
        volumeLongWindow: item.volumeLongWindow,
        volumeShortWindow: item.volumeShortWindow,
      },
      name: item.name,
    })),
  };
  const outputPath = path.resolve(__dirname, '..', 'data', 'pattern-tuning-results.json');
  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));

  console.log(`Top themes: ${output.summary.topThemes.join('、')}`);
  console.log(`Candidates: ${output.summary.candidateCount}, usableRecords: ${output.summary.usableRecordCount}, variants: ${output.summary.variantCount}`);
  output.topResults.slice(0, 10).forEach((item, index) => {
    console.log(
      `${index + 1}. ${item.name} | dayWin=${item.backtest.dayWinRatePercent}% | tradeWin=${item.backtest.tradeWinRatePercent}% | avg=${item.backtest.averageReturnPercent}% | cum=${item.backtest.cumulativeReturnPercent}% | strictAvg=${item.backtest.strictPickAverage}`,
    );
  });
  console.log(`Saved tuning results to ${outputPath}`);
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exit(1);
});
