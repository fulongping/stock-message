const http = require('http');
const fs = require('fs/promises');
const path = require('path');
const {
  getSnapshot,
  refreshThemes,
  startPolling,
} = require('./src/hotThemesService');
const {
  getWeChatSignalSnapshot,
  refreshWeChatSignal,
  startWeChatSignalPolling,
} = require('./wechatSignalService');
const {
  getMarketLeadersSnapshot,
  refreshMarketLeaders,
  startMarketLeadersPolling,
} = require('./marketLeadersService');
const {
  getPatternPicksSnapshot,
  refreshPatternPicks,
  startPatternPicksPolling,
} = require('./patternSelectorService');

const HOST = '127.0.0.1';
const PORT = Number(process.env.PORT || 3000);
const PUBLIC_DIR = path.join(__dirname, 'public');

const MIME_TYPES = {
  '.css': 'text/css; charset=utf-8',
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
};

function writeJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    'Cache-Control': 'no-store',
    'Content-Type': 'application/json; charset=utf-8',
  });
  res.end(JSON.stringify(payload));
}

async function serveStatic(req, res, pathname) {
  const normalizedPath = pathname === '/' ? '/index.html' : pathname;
  const safePath = normalizedPath.replace(/^\/+/, '');
  const filePath = path.resolve(PUBLIC_DIR, safePath);

  if (!filePath.startsWith(PUBLIC_DIR)) {
    writeJson(res, 403, { error: 'Forbidden' });
    return;
  }

  try {
    const file = await fs.readFile(filePath);
    const extname = path.extname(filePath);
    res.writeHead(200, {
      'Cache-Control': 'no-store',
      'Content-Type': MIME_TYPES[extname] || 'application/octet-stream',
    });

    if (req.method === 'HEAD') {
      res.end();
      return;
    }

    res.end(file);
  } catch (error) {
    if (error.code === 'ENOENT') {
      writeJson(res, 404, { error: 'Not found' });
      return;
    }

    console.error('Static file error:', error);
    writeJson(res, 500, { error: 'Static file error' });
  }
}

async function handleApi(req, res, pathname) {
  if (pathname === '/api/hot-themes' && req.method === 'GET') {
    const snapshot = await getSnapshot({ hydrateIfEmpty: true });
    writeJson(res, 200, snapshot);
    return;
  }

  if (pathname === '/api/hot-themes/refresh' && req.method === 'POST') {
    const snapshot = await refreshThemes({ force: true });
    writeJson(res, 200, snapshot);
    return;
  }

  if (pathname === '/api/market-leaders' && req.method === 'GET') {
    const snapshot = await getMarketLeadersSnapshot({ hydrateIfEmpty: true });
    writeJson(res, 200, snapshot);
    return;
  }

  if (pathname === '/api/market-leaders/refresh' && req.method === 'POST') {
    const snapshot = await refreshMarketLeaders({ force: true });
    writeJson(res, 200, snapshot);
    return;
  }

  if (pathname === '/api/pattern-picks' && req.method === 'GET') {
    const snapshot = await getPatternPicksSnapshot({ hydrateIfEmpty: true });
    writeJson(res, 200, snapshot);
    return;
  }

  if (pathname === '/api/pattern-picks/refresh' && req.method === 'POST') {
    const snapshot = await refreshPatternPicks({ force: true });
    writeJson(res, 200, snapshot);
    return;
  }

  if (pathname === '/api/wechat-signal' && req.method === 'GET') {
    const snapshot = await getWeChatSignalSnapshot({ hydrateIfEmpty: true });
    writeJson(res, 200, snapshot);
    return;
  }

  if (pathname === '/api/wechat-signal/refresh' && req.method === 'POST') {
    const snapshot = await refreshWeChatSignal({ force: true });
    writeJson(res, 200, snapshot);
    return;
  }

  writeJson(res, 404, { error: 'Unknown API route' });
}

function createServer() {
  return http.createServer(async (req, res) => {
    try {
      const requestUrl = new URL(req.url, `http://${HOST}:${PORT}`);

      if (requestUrl.pathname.startsWith('/api/')) {
        await handleApi(req, res, requestUrl.pathname);
        return;
      }

      if (!['GET', 'HEAD'].includes(req.method)) {
        writeJson(res, 405, { error: 'Method not allowed' });
        return;
      }

      await serveStatic(req, res, requestUrl.pathname);
    } catch (error) {
      console.error('Server error:', error);
      writeJson(res, 500, { error: 'Internal server error' });
    }
  });
}

async function startServer(port = PORT) {
  startPolling();
  startMarketLeadersPolling();
  startPatternPicksPolling();
  startWeChatSignalPolling();
  const server = createServer();

  return new Promise((resolve) => {
    server.listen(port, HOST, () => {
      console.log(`Hot themes monitor running at http://${HOST}:${port}`);
      resolve(server);
    });
  });
}

if (require.main === module) {
  startServer().catch((error) => {
    console.error('Failed to start server:', error);
    process.exitCode = 1;
  });
}

module.exports = {
  createServer,
  startServer,
};
