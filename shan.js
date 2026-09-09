'use strict';

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { SignerClient } = require('@specialjp/lighter-sdk');

const levelsPath = path.join(__dirname, 'Levels.json');
const statePath = path.join(__dirname, 'state.json');

/*
  Put these files on your HTTPS website.
  Examples:
  https://example.com/lighter-bot/status.json
  https://example.com/lighter-bot/levels.json
*/
const REMOTE_STATUS_URL = 'https://superhost.eu.org/bot/shan/status.json';
const REMOTE_LEVELS_URL = 'https://superhost.eu.org/bot/shan/levels.json';

const REMOTE_POLL_MS = 15_000;
const FETCH_TIMEOUT_MS = 8_000;

let levels = readLocalLevels();

const config = {
  url: 'https://mainnet.zklighter.elliot.ai',
  privateKey: 'ae8c1de9a111a6734a50a068735ef2df84e8f5ee791d46d6074adebf7d52fd490d19135b883a9005',
  accountIndex: 281474976493344,
  apiKeyIndex: 254
};

const wsUrl = 'wss://mainnet.zklighter.elliot.ai/stream';

let client = null;
let ws = null;

let pingTimer = null;
let reconnectTimer = null;
let pollTimer = null;
let priceLogTimer = null;
let remotePollTimer = null;

let livePrice = null;
let prevPrice = null;

let busy = false;
let pendingOrder = false;
let remotePollBusy = false;

/*
  running = trading allowed
  paused = no buy/sell orders, tracking continues
  stopped = no trading; state.json removed; waits for remote "run"
*/
let botMode = 'running';

let remoteLevelsHash = null;
let lastRemoteCommand = null;

let state = {
  initialized: false,
  gridOpened: false,
  direction: 'long',
  gridType: 'arithmetic',
  marketIndex: levels.marketIndex,
  lowerPrice: levels.lowerPrice,
  upperPrice: levels.upperPrice,
  grids: levels.grids,
  quantityPerGrid: levels.quantityPerGrid,
  buyCount: 0,
  sellCount: 0,
  positionSize: 0,
  avgEntryPrice: 0,
  lastGridIndex: null,
  filledGrids: [],
  lastPrice: null,
  gridBook: {}
};

function fmt(n) {
  return Number(n || 0).toFixed(8);
}

function readJsonFile(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function readLocalLevels() {
  const parsed = readJsonFile(levelsPath);
  validateLevels(parsed);
  return parsed;
}

function atomicWriteJson(filePath, data) {
  const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tempPath, JSON.stringify(data, null, 2));
  fs.renameSync(tempPath, filePath);
}

function loadState() {
  try {
    if (!fs.existsSync(statePath)) return;

    const saved = readJsonFile(statePath);
    state = { ...state, ...saved };

    if (!state.gridBook || typeof state.gridBook !== 'object') {
      state.gridBook = {};
    }

    if (!Array.isArray(state.filledGrids)) {
      state.filledGrids = [];
    }
  } catch (err) {
    console.log('Could not load state.json:', err.message);
  }
}

function saveState() {
  try {
    atomicWriteJson(statePath, state);
  } catch (err) {
    console.log('Could not save state.json:', err.message);
  }
}

function removeStateFile() {
  try {
    if (fs.existsSync(statePath)) fs.unlinkSync(statePath);
  } catch (err) {
    console.log('Could not remove state.json:', err.message);
  }
}

function freshStateFromLevels() {
  return {
    initialized: false,
    gridOpened: false,
    direction: levels.direction,
    gridType: levels.gridType,
    marketIndex: levels.marketIndex,
    lowerPrice: levels.lowerPrice,
    upperPrice: levels.upperPrice,
    grids: levels.grids,
    quantityPerGrid: levels.quantityPerGrid,
    buyCount: 0,
    sellCount: 0,
    positionSize: 0,
    avgEntryPrice: 0,
    lastGridIndex: null,
    filledGrids: [],
    lastPrice: livePrice,
    gridBook: {}
  };
}

function validateLevels(x) {
  if (!x || typeof x !== 'object') {
    throw new Error('levels.json must be a JSON object');
  }

  const requiredNumbers = [
    'marketIndex',
    'lowerPrice',
    'upperPrice',
    'grids',
    'quantityPerGrid',
    'buyWorstPrice',
    'sellWorstPrice'
  ];

  for (const key of requiredNumbers) {
    if (!Number.isFinite(Number(x[key]))) {
      throw new Error(`levels.json invalid or missing numeric field: ${key}`);
    }
  }

  if (Number(x.lowerPrice) <= 0) {
    throw new Error('lowerPrice must be greater than 0');
  }

  if (Number(x.upperPrice) <= Number(x.lowerPrice)) {
    throw new Error('upperPrice must be greater than lowerPrice');
  }

  if (!Number.isInteger(Number(x.grids)) || Number(x.grids) < 1) {
    throw new Error('grids must be an integer greater than 0');
  }

  if (Number(x.quantityPerGrid) <= 0) {
    throw new Error('quantityPerGrid must be greater than 0');
  }

  if (!['long'].includes(x.direction)) {
    throw new Error('Only direction "long" is currently supported');
  }

  if (!['arithmetic', 'geometric'].includes(x.gridType)) {
    throw new Error('gridType must be arithmetic or geometric');
  }
}

function sameJson(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}

function hashJson(obj) {
  return crypto
    .createHash('sha256')
    .update(JSON.stringify(obj))
    .digest('hex');
}

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      accept: 'application/json',
      'cache-control': 'no-cache'
    },
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
  });

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }

  return res.json();
}

function normalizeCommand(status) {
  const command = String(status?.command || '').trim().toLowerCase();

  if (!['run', 'pause', 'stop'].includes(command)) {
    throw new Error('status.json command must be run, pause, or stop');
  }

  return command;
}

function getPriceFromMsg(data) {
  const p =
    data?.market_stats?.mark_price ??
    data?.market_stats?.index_price ??
    data?.market_stats?.last_trade_price ??
    data?.data?.market_stats?.mark_price ??
    data?.data?.mark_price ??
    data?.mark_price ??
    data?.index_price ??
    data?.last_trade_price;

  const n = Number(p);
  return Number.isFinite(n) ? n : null;
}

function buildGrids() {
  const { lowerPrice, upperPrice, grids, gridType } = levels;
  const out = [];

  if (grids <= 0) return out;

  if (gridType === 'geometric') {
    const ratio = Math.pow(upperPrice / lowerPrice, 1 / grids);

    for (let i = 0; i <= grids; i++) {
      out.push(lowerPrice * Math.pow(ratio, i));
    }
  } else {
    const step = (upperPrice - lowerPrice) / grids;

    for (let i = 0; i <= grids; i++) {
      out.push(lowerPrice + step * i);
    }
  }

  return out;
}

function getGridIndex(price, gridsArr) {
  if (price <= gridsArr[0]) return 0;
  if (price >= gridsArr[gridsArr.length - 1]) return gridsArr.length - 1;

  for (let i = 0; i < gridsArr.length - 1; i++) {
    if (price >= gridsArr[i] && price < gridsArr[i + 1]) {
      return i;
    }
  }

  return gridsArr.length - 1;
}

function crossedDown(prev, curr, level) {
  return prev != null && prev > level && curr <= level;
}

function crossedUp(prev, curr, level) {
  return prev != null && prev < level && curr >= level;
}

function defaultGridState() {
  return {
    status: 'EMPTY',
    entryPrice: null,
    exitPrice: null,
    qty: 0,
    lastTouched: null,
    openOrderTs: null,
    closeOrderTs: null
  };
}

function ensureGridBook(gridsArr) {
  for (let i = 0; i < gridsArr.length - 1; i++) {
    const key = String(i);

    if (!state.gridBook[key]) {
      state.gridBook[key] = defaultGridState();
    }
  }
}

function getGridState(i) {
  return state.gridBook[String(i)] || defaultGridState();
}

function setGridState(i, patch) {
  const key = String(i);

  if (!state.gridBook[key]) {
    state.gridBook[key] = defaultGridState();
  }

  state.gridBook[key] = {
    ...state.gridBook[key],
    ...patch,
    lastTouched: Date.now()
  };
}

async function initClient() {
  if (!config.privateKey) {
    throw new Error(
      'LIGHTER_PRIVATE_KEY is missing. Set it before running the bot.'
    );
  }

  client = new SignerClient(config);
  await client.initialize();
  await client.ensureWasmClient();

  console.log('Signer client ready');
}

function clearWsTimers() {
  if (pingTimer) clearInterval(pingTimer);
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (pollTimer) clearInterval(pollTimer);
  if (priceLogTimer) clearInterval(priceLogTimer);

  pingTimer = null;
  reconnectTimer = null;
  pollTimer = null;
  priceLogTimer = null;
}

function stopRemotePolling() {
  if (remotePollTimer) clearInterval(remotePollTimer);
  remotePollTimer = null;
}

function startRemotePolling() {
  stopRemotePolling();

  remotePollTimer = setInterval(() => {
    pollRemoteControl().catch(() => {});
  }, REMOTE_POLL_MS);

  pollRemoteControl().catch(() => {});
}

function connectWs() {
  clearWsTimers();

  ws = new WebSocket(wsUrl);

  ws.on('open', () => {
    console.log('WebSocket connected');

    ws.send(JSON.stringify({
      type: 'subscribe',
      channel: `market_stats/${levels.marketIndex}`
    }));

    pingTimer = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'ping' }));
      }
    }, 25_000);

    pollTimer = setInterval(() => {
      tradeLogic().catch((err) => {
        console.log('tradeLogic error:', err.message);
      });
    }, 300);

    priceLogTimer = setInterval(() => {
      if (livePrice != null) {
        console.log(
          `MODE=${botMode} PRICE=${fmt(livePrice)} ` +
          `pos=${fmt(state.positionSize)} buys=${state.buyCount} ` +
          `sells=${state.sellCount} lastGrid=${state.lastGridIndex} ` +
          `filled=${JSON.stringify(state.filledGrids)}`
        );
      }
    }, Math.max(1, Number(levels.priceLogSeconds || 2)) * 1000);
  });

  ws.on('message', async (msg) => {
    try {
      const data = JSON.parse(msg.toString());
      const price = getPriceFromMsg(data);

      if (price == null) return;

      prevPrice = livePrice;
      livePrice = price;

      if (botMode !== 'stopped') {
        state.lastPrice = livePrice;

        if (!state.initialized) {
          state.initialized = true;
          state.gridOpened = true;
          saveState();
        }
      }

      await tradeLogic();
    } catch (err) {
      console.log('WS message error:', err.message);
    }
  });

  ws.on('close', () => {
    console.log('WebSocket closed. Reconnecting in 2s...');
    clearWsTimers();
    reconnectTimer = setTimeout(connectWs, 2000);
  });

  ws.on('error', (err) => {
    console.log('WebSocket error:', err.message);
  });
}

async function applyRemoteLevels(remoteLevels) {
  validateLevels(remoteLevels);

  const oldLevels = levels;
  const changed = !sameJson(oldLevels, remoteLevels);

  if (!changed) {
    return false;
  }

  /*
    State should normally have been removed by "stop".
    Do not replace active levels during a run because existing OPEN grids
    could no longer match the new ladder.
  */
  if (botMode !== 'stopped') {
    console.log(
      'Remote levels differ, but active levels are preserved until stop -> run.'
    );
    return false;
  }

  atomicWriteJson(levelsPath, remoteLevels);
  levels = remoteLevels;

  console.log(
    `Remote levels applied: ${levels.symbol} market=${levels.marketIndex} ` +
    `range=${levels.lowerPrice}-${levels.upperPrice} grids=${levels.grids}`
  );

  return true;
}

async function enterStoppedMode() {
  if (botMode === 'stopped') return;

  botMode = 'stopped';
  busy = false;

  /*
    Safety warning:
    This removes local tracking only. It does NOT close live exchange positions.
  */
  state = freshStateFromLevels();
  removeStateFile();

  console.log(
    'BOT STOPPED: trading disabled and state.json removed. ' +
    'Any exchange position remains open; verify/reconcile it before RUN.'
  );
}

async function enterPausedMode() {
  if (botMode === 'stopped') {
    console.log('Ignoring PAUSE because bot is stopped. Use RUN to create a new state.');
    return;
  }

  botMode = 'paused';
  console.log('BOT PAUSED: no new market orders will be submitted.');
}

async function enterRunningMode(remoteLevels) {
  if (botMode === 'stopped') {
    await applyRemoteLevels(remoteLevels);

    /*
      Fresh ladder is created only after a remote STOP -> RUN cycle.
      positionSize intentionally starts at zero because state was deleted.
      Do not use STOP while holding a position unless you have reconciled it.
    */
    state = freshStateFromLevels();
    const gridsArr = buildGrids();

    ensureGridBook(gridsArr);
    state.initialized = livePrice != null;
    state.gridOpened = livePrice != null;

    if (livePrice != null) {
      state.lastGridIndex = getGridIndex(livePrice, gridsArr);
      state.lastPrice = livePrice;
    }

    saveState();
    prevPrice = livePrice;

    console.log(
      `BOT RUNNING with fresh ladder: ${gridsArr.length - 1} grid intervals.`
    );
  } else {
    /*
      If levels were changed on the website while paused/running,
      retain current local levels until the explicit STOP -> RUN workflow.
    */
    const remoteHash = hashJson(remoteLevels);

    if (remoteLevelsHash && remoteHash !== remoteLevelsHash) {
      console.log(
        'Remote levels changed. They will apply after stop -> run; current ladder is unchanged.'
      );
    }
  }

  botMode = 'running';
  console.log('BOT RUNNING: order placement enabled.');
}

async function pollRemoteControl() {
  if (remotePollBusy) return;
  remotePollBusy = true;

  try {
    const [remoteStatus, remoteLevels] = await Promise.all([
      fetchJson(`${REMOTE_STATUS_URL}?t=${Date.now()}`),
      fetchJson(`${REMOTE_LEVELS_URL}?t=${Date.now()}`)
    ]);

    const command = normalizeCommand(remoteStatus);
    validateLevels(remoteLevels);

    const remoteHash = hashJson(remoteLevels);

    if (command === 'stop') {
      await enterStoppedMode();
    } else if (command === 'pause') {
      await enterPausedMode();
    } else if (command === 'run') {
      await enterRunningMode(remoteLevels);
    }

    remoteLevelsHash = remoteHash;

    if (command !== lastRemoteCommand) {
      console.log(`Remote command received: ${command.toUpperCase()}`);
      lastRemoteCommand = command;
    }
  } catch (err) {
    /*
      Network/site failure must fail closed:
      retain the prior local mode and do not submit a command based
      on incomplete or invalid remote data.
    */
    console.log(`Remote control poll failed: ${err.message}`);
  } finally {
    remotePollBusy = false;
  }
}

async function placeBuy(levelIndex) {
  if (botMode !== 'running' || pendingOrder) return false;

  pendingOrder = true;

  try {
    const clientOrderIndex = Date.now();

    const [tx, txHash, err] = await client.createMarketOrder({
      marketIndex: levels.marketIndex,
      clientOrderIndex,
      baseAmount: levels.quantityPerGrid,
      avgExecutionPrice: levels.buyWorstPrice,
      isAsk: false
    });

    if (err) {
      console.log(`BUY rejected at grid ${levelIndex}:`, err);
      return false;
    }

    state.buyCount += 1;
    state.positionSize += levels.quantityPerGrid;

    const previousSize = state.positionSize - levels.quantityPerGrid;
    const previousValue = state.avgEntryPrice * previousSize;

    state.avgEntryPrice =
      (previousValue + livePrice * levels.quantityPerGrid) /
      state.positionSize;

    state.lastGridIndex = levelIndex;
    state.filledGrids = Array.from(
      new Set([...state.filledGrids, levelIndex])
    );

    setGridState(levelIndex, {
      status: 'OPEN',
      entryPrice: livePrice,
      exitPrice: null,
      qty: levels.quantityPerGrid,
      openOrderTs: Date.now(),
      closeOrderTs: null
    });

    saveState();

    console.log(
      `BUY grid=${levelIndex} price=${fmt(livePrice)} tx=${txHash}`
    );

    return true;
  } finally {
    pendingOrder = false;
  }
}

async function placeSell(levelIndex) {
  if (
    botMode !== 'running' ||
    pendingOrder ||
    state.positionSize <= 0
  ) {
    return false;
  }

  pendingOrder = true;

  try {
    const grid = getGridState(levelIndex);
    const qty = Number(grid.qty || levels.quantityPerGrid);

    if (qty <= 0) {
      console.log(`SELL skipped at grid ${levelIndex}: invalid grid quantity`);
      return false;
    }

    const clientOrderIndex = Date.now();

    const [tx, txHash, err] = await client.createMarketOrder({
      marketIndex: levels.marketIndex,
      clientOrderIndex,
      baseAmount: qty,
      avgExecutionPrice: levels.sellWorstPrice,
      isAsk: true
    });

    if (err) {
      console.log(`SELL rejected at grid ${levelIndex}:`, err);
      return false;
    }

    state.sellCount += 1;
    state.positionSize = Math.max(0, state.positionSize - qty);

    if (state.positionSize === 0) {
      state.avgEntryPrice = 0;
    }

    state.filledGrids = state.filledGrids.filter((x) => x !== levelIndex);

    setGridState(levelIndex, {
      status: 'CLOSED',
      exitPrice: livePrice,
      closeOrderTs: Date.now()
    });

    saveState();

    console.log(
      `SELL grid=${levelIndex} price=${fmt(livePrice)} tx=${txHash}`
    );

    return true;
  } finally {
    pendingOrder = false;
  }
}

async function tradeLogic() {
  /*
    This is the critical guard: PAUSE and STOP cannot place buy/sell orders.
  */
  if (
    botMode !== 'running' ||
    busy ||
    livePrice == null ||
    pendingOrder
  ) {
    return;
  }

  busy = true;

  try {
    const gridsArr = buildGrids();

    if (!gridsArr.length) return;

    ensureGridBook(gridsArr);

    const currentIndex = getGridIndex(livePrice, gridsArr);
    state.lastGridIndex = currentIndex;

    if (!state.initialized) {
      state.initialized = true;
      state.gridOpened = true;
      saveState();
      return;
    }

    if (levels.direction !== 'long') return;

    for (let i = 0; i < gridsArr.length - 1; i++) {
      if (botMode !== 'running') break;

      const grid = getGridState(i);
      const entryLevel = gridsArr[i];
      const exitLevel = gridsArr[i + 1];

      if (grid.status === 'OPEN') {
        if (crossedUp(prevPrice, livePrice, exitLevel)) {
          const ok = await placeSell(i);
          if (!ok) break;
        }

        continue;
      }

      if (
        grid.status === 'EMPTY' &&
        crossedDown(prevPrice, livePrice, entryLevel)
      ) {
        const ok = await placeBuy(i);
        if (!ok) break;

        continue;
      }

      if (
        grid.status === 'CLOSED' &&
        crossedDown(prevPrice, livePrice, entryLevel)
      ) {
        setGridState(i, {
          status: 'EMPTY',
          entryPrice: null,
          exitPrice: null,
          qty: 0
        });

        saveState();
      }
    }
  } catch (err) {
    console.log('tradeLogic failed:', err.message);
  } finally {
    busy = false;
  }
}

process.on('SIGINT', () => {
  console.log('Shutting down...');

  clearWsTimers();
  stopRemotePolling();

  try {
    if (ws) ws.close();
  } catch {}

  if (botMode !== 'stopped') {
    saveState();
  }

  process.exit(0);
});

process.on('SIGTERM', () => {
  process.emit('SIGINT');
});

(async () => {
  levels = readLocalLevels();
  loadState();

  await initClient();

  connectWs();
  startRemotePolling();

  console.log(
    `Remote control polling every ${REMOTE_POLL_MS / 1000} seconds.`
  );
})().catch((err) => {
  console.error('Fatal:', err.message);
  process.exit(1);
});