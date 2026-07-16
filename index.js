import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import cors from 'cors';
import multer from 'multer';
import pino from 'pino';
import PQueue from 'p-queue';
import dotenv from 'dotenv';
import rateLimit from 'express-rate-limit';
// ✅ FIX: use named import for makeWASocket as well
import {
  makeWASocket,
  useMultiFileAuthState,
  makeCacheableSignalKeyStore,
  DisconnectReason,
  Browsers,
  fetchLatestBaileysVersion,
} from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';

// ── Load environment ──────────────────────────────────────────
dotenv.config();
const requiredEnv = ['PORT'];
const missingEnv = requiredEnv.filter(k => !process.env[k]);
if (missingEnv.length) throw new Error(`Missing env vars: ${missingEnv.join(', ')}`);

// ── Configuration ─────────────────────────────────────────────
const config = {
  port: parseInt(process.env.PORT, 10) || 21224,
  logLevel: process.env.LOG_LEVEL || 'info',
  sessionStore: process.env.SESSION_STORE || 'file',
  redisUrl: process.env.REDIS_URL,
  reconnectBaseDelay: parseInt(process.env.RECONNECT_BASE_DELAY, 10) || 1000,
  msgConcurrency: parseInt(process.env.MSG_CONCURRENCY, 10) || 5,
  healthPort: parseInt(process.env.HEALTH_PORT, 10) || parseInt(process.env.PORT, 10) || 21224,
  version: process.env.VERSION || '4.0.0',
};

// ── Logger ────────────────────────────────────────────────────
const logger = pino({
  level: config.logLevel,
  formatters: { level: (label) => ({ level: label }) },
  timestamp: pino.stdTimeFunctions.isoTime,
});

// ── Redis (optional) ──────────────────────────────────────────
let redis = null;
let redisConnected = false;
if (config.sessionStore === 'redis') {
  try {
    const { Redis } = await import('ioredis');
    redis = new Redis(config.redisUrl, {
      maxRetriesPerRequest: 3,
      retryDelayOnFailover: 500,
      enableReadyCheck: true,
    });
    redis.on('connect', () => { redisConnected = true; logger.info('Redis connected'); });
    redis.on('error', (err) => { redisConnected = false; logger.warn({ err: err.message }, 'Redis error'); });
    redis.on('close', () => { redisConnected = false; });
    await new Promise((resolve) => {
      if (redis.status === 'ready') { redisConnected = true; resolve(); }
      else { redis.once('ready', () => { redisConnected = true; resolve(); }); }
    });
  } catch (err) {
    logger.warn('Redis not available, falling back to file storage');
    config.sessionStore = 'file';
  }
}

// ── Paths and dirs ────────────────────────────────────────────
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const SESSION_FILE = path.resolve(__dirname, 'running_sessions.json');
const sessionDir = path.resolve(__dirname, 'session');
const uploadDir = path.resolve(__dirname, 'uploads');
const publicDir = path.resolve(__dirname, 'public');
[sessionDir, uploadDir, publicDir].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// ── Session persistence helpers ─────────────────────────────
async function saveSessions(userSessions) {
  try {
    const data = JSON.stringify(userSessions, null, 2);
    if (config.sessionStore === 'redis' && redisConnected) {
      await redis.set('whatsapp:sessions', data, 'EX', 60 * 60 * 24 * 30);
    } else {
      const tmp = SESSION_FILE + '.tmp';
      await fs.promises.writeFile(tmp, data, 'utf8');
      await fs.promises.rename(tmp, SESSION_FILE);
    }
  } catch (e) {
    logger.error({ err: e }, 'Failed to save sessions');
  }
}

async function loadSessions() {
  try {
    let data = null;
    if (config.sessionStore === 'redis' && redisConnected) {
      data = await redis.get('whatsapp:sessions');
    } else {
      if (!fs.existsSync(SESSION_FILE)) return {};
      data = await fs.promises.readFile(SESSION_FILE, 'utf8');
    }
    if (data) {
      const parsed = JSON.parse(data);
      logger.info({ count: Object.keys(parsed).length }, 'Loaded sessions');
      return parsed;
    }
    return {};
  } catch (e) {
    logger.error({ err: e }, 'Failed to load sessions');
    return {};
  }
}

function removeDir(dirPath) {
  try {
    if (fs.existsSync(dirPath)) fs.rmSync(dirPath, { recursive: true, force: true });
  } catch (_) {}
}

async function closeRedis() {
  if (redis) {
    try { await redis.quit(); } catch (_) {}
  }
}

// ── Connection Manager ──────────────────────────────────────
const activeSockets = {};
const reconnectTimers = {};
const reconnectAttempts = {};
const stopFlags = {};
const senderQueues = {};
const sessionStats = {};
const connectionLocks = new Map();
let readiness = false;

function isSocketConnected(sock) {
  if (!sock) return false;
  if (sock.user) return true;
  if (sock.ws && sock.ws.readyState === 1) return true;
  if (sock.authState?.creds?.registered) return true;
  return false;
}

function updateReadiness() {
  const connected = Object.values(activeSockets).some(sock => isSocketConnected(sock));
  if (readiness !== connected) {
    readiness = connected;
    logger.info({ readiness }, 'Readiness updated');
  }
}

function getSocket(uniqueKey) { return activeSockets[uniqueKey]; }
function getStopFlags(uniqueKey) { return stopFlags[uniqueKey]; }
function setStopFlag(uniqueKey, flag) { stopFlags[uniqueKey] = flag; }
function getSenderQueue(uniqueKey) { return senderQueues[uniqueKey]; }
function setSenderQueue(uniqueKey, queue) { senderQueues[uniqueKey] = queue; }

function clearReconnectTimer(uniqueKey) {
  if (reconnectTimers[uniqueKey]) {
    clearTimeout(reconnectTimers[uniqueKey]);
    delete reconnectTimers[uniqueKey];
  }
}

function cleanupSending(uniqueKey) {
  if (stopFlags[uniqueKey]) {
    clearTimeout(stopFlags[uniqueKey].timeout);
    delete stopFlags[uniqueKey];
  }
  if (senderQueues[uniqueKey]) {
    senderQueues[uniqueKey].pause();
    senderQueues[uniqueKey].clear();
    delete senderQueues[uniqueKey];
  }
  clearReconnectTimer(uniqueKey);
}

async function closeSocketSafely(sock) {
  if (!sock) return;
  try {
    sock.ev.removeAllListeners();
    if (sock.ws && sock.ws.readyState === 1) {
      sock.ws.close();
      logger.debug('WebSocket closed');
    } else if (typeof sock.end === 'function') {
      await sock.end();
      logger.debug('Socket ended via end()');
    } else {
      logger.warn('No method to close socket');
    }
  } catch (err) {
    logger.warn({ err: err.message }, 'Error closing socket');
  }
}

async function cleanupSocket(uniqueKey) {
  const sock = activeSockets[uniqueKey];
  if (sock) {
    await closeSocketSafely(sock);
    logger.info({ uniqueKey }, 'Socket closed and listeners removed');
  }
}

async function cleanupSession(uniqueKey) {
  cleanupSending(uniqueKey);
  await cleanupSocket(uniqueKey);
  delete activeSockets[uniqueKey];
  delete reconnectAttempts[uniqueKey];
  delete sessionStats[uniqueKey];
  updateReadiness();
}

function removeActiveSocket(uniqueKey) {
  delete activeSockets[uniqueKey];
  updateReadiness();
}

// ── Connect with mutex ──────────────────────────────────────
async function connectAndLogin(phoneNumber, uniqueKey, sendPairingCode = null) {
  const lockKey = phoneNumber;
  if (connectionLocks.has(lockKey)) {
    logger.warn({ phoneNumber }, 'Connection attempt already in progress, waiting...');
    await connectionLocks.get(lockKey);
    if (activeSockets[uniqueKey]) {
      return activeSockets[uniqueKey];
    }
  }

  const lockPromise = (async () => {
    try {
      return await connectAndLoginInternal(phoneNumber, uniqueKey, sendPairingCode);
    } finally {
      connectionLocks.delete(lockKey);
    }
  })();
  connectionLocks.set(lockKey, lockPromise);
  return await lockPromise;
}

async function connectAndLoginInternal(phoneNumber, uniqueKey, sendPairingCode) {
  const sessionPath = path.join(sessionDir, uniqueKey);
  let pairingCodeSent = false;
  let responded = false;

  if (activeSockets[uniqueKey]) {
    logger.info({ uniqueKey }, 'Cleaning up existing socket before reconnect');
    await cleanupSocket(uniqueKey);
    delete activeSockets[uniqueKey];
  }

  clearReconnectTimer(uniqueKey);

  const startConnection = async () => {
    try {
      logger.info({ phoneNumber, uniqueKey }, 'Connecting...');

      if (!fs.existsSync(sessionPath)) fs.mkdirSync(sessionPath, { recursive: true });

      const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
      const { version } = await fetchLatestBaileysVersion();

      const sock = makeWASocket({
        version,
        logger: pino({ level: 'silent' }),
        browser: Browsers.windows('Firefox'),
        auth: {
          creds: state.creds,
          keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'silent' })),
        },
        printQRInTerminal: false,
        generateHighQualityLinkPreview: true,
        markOnlineOnConnect: true,
        getMessage: async () => undefined,
        keepAliveIntervalMs: 30000,
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: undefined,
        retryRequestDelayMs: 250,
      });

      activeSockets[uniqueKey] = sock;

      const isRegistered = sock.authState?.creds?.registered ?? false;
      if (!isRegistered && !pairingCodeSent) {
        await new Promise(r => setTimeout(r, 2000));
        try {
          const cleaned = phoneNumber.replace(/[^0-9]/g, '');
          logger.info({ phoneNumber: cleaned }, 'Requesting pairing code');
          const code = await sock.requestPairingCode(cleaned);
          const pairingCode = code?.match(/.{1,4}/g)?.join('-') || code;
          pairingCodeSent = true;
          if (sendPairingCode && !responded) {
            responded = true;
            sendPairingCode(pairingCode, false);
          } else if (!sendPairingCode) {
            await fs.promises.writeFile(
              path.join(__dirname, `pairing_code_${uniqueKey}.txt`),
              `Phone: ${phoneNumber}\nCode: ${pairingCode}\nTime: ${new Date()}`
            );
            logger.info({ uniqueKey }, 'Pairing code saved to file');
          }
        } catch (err) {
          logger.error({ uniqueKey, err: err.message }, 'Pairing error');
          if (sendPairingCode && !responded) {
            responded = true;
            sendPairingCode(null, false, err.message);
          }
        }
      } else if (isRegistered) {
        logger.info({ uniqueKey }, 'Session already registered');
        if (!pairingCodeSent && sendPairingCode && !responded) {
          pairingCodeSent = true;
          responded = true;
          sendPairingCode(null, true);
        }
      }

      sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === 'open') {
          logger.info({ uniqueKey }, '✅ Connected');
          reconnectAttempts[uniqueKey] = 0;
          updateReadiness();

          if (!pairingCodeSent && sendPairingCode && !responded) {
            pairingCodeSent = true;
            responded = true;
            sendPairingCode(null, true);
          }

          const sess = userSessions[uniqueKey];
          if (sess) {
            if (sess.messaging && sess.messages) {
              startSending(uniqueKey, sess.target, sess.messages, {
                type: 'text',
                prefix: sess.hatersName || '',
                speed: sess.speed,
              });
            } else if (sess.photoing && sess.photoItems) {
              startSending(uniqueKey, sess.target, sess.photoItems, {
                type: 'photo',
                caption: sess.caption || '',
                speed: sess.speed,
              });
            } else if (sess.stickering && sess.stickerPath) {
              startSending(uniqueKey, sess.target, [sess.stickerPath], {
                type: 'sticker',
                speed: sess.speed,
              });
            }
          }
        }

        if (connection === 'close') {
          const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
          logger.warn({ uniqueKey, reason }, 'Disconnected');
          updateReadiness();

          if (reason === DisconnectReason.badSession) {
            logger.warn({ uniqueKey }, 'Bad session – deleting folder');
            removeDir(sessionPath);
            cleanupSending(uniqueKey);
            delete activeSockets[uniqueKey];
          } else if (reason === DisconnectReason.connectionReplaced) {
            logger.warn({ uniqueKey }, 'Connection replaced – cleaning up');
            await cleanupSession(uniqueKey);
          } else if (reason === DisconnectReason.loggedOut || reason === 401) {
            logger.warn({ uniqueKey }, 'Logged out – deleting session folder');
            cleanupSending(uniqueKey);
            await cleanupSocket(uniqueKey);
            delete activeSockets[uniqueKey];
            removeDir(sessionPath);
          }

          if (!stopFlags[uniqueKey]?.stopped) {
            const currentAttempt = (reconnectAttempts[uniqueKey] || 0) + 1;
            reconnectAttempts[uniqueKey] = currentAttempt;
            const baseDelay = config.reconnectBaseDelay * Math.pow(2, Math.min(currentAttempt, 10));
            const jitter = Math.random() * 0.3 * baseDelay;
            const delay = Math.min(baseDelay + jitter, 60000);
            logger.info({ uniqueKey, attempt: currentAttempt, delay }, 'Scheduling reconnect');
            clearReconnectTimer(uniqueKey);
            reconnectTimers[uniqueKey] = setTimeout(() => {
              connectAndLogin(phoneNumber, uniqueKey, null);
            }, delay);
          }
        }
      });

      sock.ev.on('creds.update', saveCreds);
      sock.ev.on('messages.upsert', () => {});

      return sock;

    } catch (error) {
      logger.error({ uniqueKey, err: error.message }, 'Connection error');
      if (!pairingCodeSent && sendPairingCode && !responded) {
        responded = true;
        sendPairingCode(null, false, error.message);
      }
      if (!stopFlags[uniqueKey]?.stopped) {
        const currentAttempt = (reconnectAttempts[uniqueKey] || 0) + 1;
        reconnectAttempts[uniqueKey] = currentAttempt;
        const delay = Math.min(5000 * Math.pow(2, Math.min(currentAttempt, 10)), 60000);
        clearReconnectTimer(uniqueKey);
        reconnectTimers[uniqueKey] = setTimeout(() => {
          startConnection();
        }, delay);
      }
      throw error;
    }
  };

  return await startConnection();
}

function getActiveSockets() { return activeSockets; }
function getReconnectTimers() { return reconnectTimers; }

function clearAllReconnectTimers() {
  for (const key of Object.keys(reconnectTimers)) {
    clearTimeout(reconnectTimers[key]);
    delete reconnectTimers[key];
  }
}

async function closeAllSockets() {
  const promises = [];
  for (const key of Object.keys(activeSockets)) {
    const sock = activeSockets[key];
    if (sock) {
      promises.push(closeSocketSafely(sock));
    }
  }
  await Promise.allSettled(promises);
  for (const key of Object.keys(activeSockets)) {
    delete activeSockets[key];
  }
  updateReadiness();
}

// ── Sender with per-session queue ──────────────────────────
const globalSendQueue = new PQueue({ concurrency: config.msgConcurrency });

function getSessionStats(uniqueKey) {
  return sessionStats[uniqueKey] || { sent: 0, failed: 0, lastMessage: '', type: 'msg' };
}
function clearSessionStats(uniqueKey) {
  delete sessionStats[uniqueKey];
}

function startSending(uniqueKey, target, items, options = {}) {
  const { type = 'text', caption = '', speed = 5 } = options;
  const sock = getSocket(uniqueKey);
  if (!sock || !isSocketConnected(sock)) {
    logger.warn({ uniqueKey }, 'No connected socket, cannot start sending');
    return;
  }

  cleanupSending(uniqueKey);

  if (!sessionStats[uniqueKey]) {
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '', type };
  }
  sessionStats[uniqueKey].type = type;

  let currentIndex = 0;
  let stopped = false;
  const stopFlag = { stopped: false };
  setStopFlag(uniqueKey, stopFlag);

  const perSessionQueue = new PQueue({ concurrency: 1 });
  setSenderQueue(uniqueKey, perSessionQueue);

  const sendOne = async () => {
    if (stopped || stopFlag.stopped) return;
    if (!getSocket(uniqueKey) || !isSocketConnected(getSocket(uniqueKey))) {
      logger.warn({ uniqueKey }, 'Socket lost during sending, stopping');
      return;
    }
    if (currentIndex >= items.length) currentIndex = 0;
    const item = items[currentIndex];
    const sock = getSocket(uniqueKey);
    let chatId;
    if (target.includes('@g.us') || target.includes('@s.whatsapp.net')) {
      chatId = target;
    } else {
      chatId = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    }

    try {
      let sendPromise;
      switch (type) {
        case 'text': {
          const text = options.prefix ? `${options.prefix} ${item}` : item;
          sendPromise = sock.sendMessage(chatId, { text }, { timeout: 15000 });
          break;
        }
        case 'photo': {
          let imagePayload;
          if (item.startsWith('http://') || item.startsWith('https://')) {
            imagePayload = { url: item };
          } else {
            const filePath = path.resolve(__dirname, item);
            if (!fs.existsSync(filePath)) throw new Error(`File not found: ${filePath}`);
            imagePayload = await fs.promises.readFile(filePath);
          }
          sendPromise = sock.sendMessage(chatId, { image: imagePayload, caption }, { timeout: 20000 });
          break;
        }
        case 'sticker': {
          const filePath = path.resolve(__dirname, item);
          if (!fs.existsSync(filePath)) throw new Error(`Sticker file missing: ${filePath}`);
          const stickerBuffer = await fs.promises.readFile(filePath);
          sendPromise = sock.sendMessage(chatId, { sticker: stickerBuffer }, { timeout: 15000 });
          break;
        }
        default:
          throw new Error(`Unsupported type: ${type}`);
      }

      await globalSendQueue.add(() => sendPromise);

      sessionStats[uniqueKey].sent++;
      sessionStats[uniqueKey].lastMessage = item.substring(0, 60);
      logger.info({ uniqueKey, type, sent: sessionStats[uniqueKey].sent, chatId }, 'Message sent');
    } catch (err) {
      sessionStats[uniqueKey].failed++;
      logger.error({ uniqueKey, type, err: err.message }, 'Send failed');
    }

    currentIndex++;
    if (currentIndex >= items.length) {
      logger.info({ uniqueKey }, 'All items sent, restarting loop');
      currentIndex = 0;
    }
  };

  const scheduleNext = () => {
    if (stopped || stopFlag.stopped) {
      return;
    }
    if (!getSocket(uniqueKey) || !isSocketConnected(getSocket(uniqueKey))) {
      logger.warn({ uniqueKey }, 'Socket lost, stopping sender');
      return;
    }
    perSessionQueue.add(sendOne).then(() => {
      if (!stopped && !stopFlag.stopped) {
        const intervalMs = parseInt(speed, 10) * 1000;
        stopFlag.timeout = setTimeout(() => {
          scheduleNext();
        }, intervalMs);
      }
    }).catch((err) => {
      logger.error({ uniqueKey, err: err.message }, 'Unexpected error in send loop');
      if (!stopped && !stopFlag.stopped) {
        const intervalMs = parseInt(speed, 10) * 1000;
        stopFlag.timeout = setTimeout(() => {
          scheduleNext();
        }, intervalMs);
      }
    });
  };

  scheduleNext();
  logger.info({ uniqueKey, type, target, speed }, 'Sending started');
}

// ── Health / Readiness ──────────────────────────────────────
function registerHealthRoutes(app) {
  app.get('/health', (req, res) => {
    const memory = process.memoryUsage();
    const cpuUsage = process.cpuUsage();
    const uptime = process.uptime();

    const sockets = getActiveSockets();
    const totalSessions = Object.keys(sockets).length;
    const reconnectTimersObj = getReconnectTimers();
    const reconnectingCount = Object.keys(reconnectTimersObj).length;

    let connectedCount = 0;
    let sendingCount = 0;
    let idleCount = 0;
    let totalQueueSize = 0;

    for (const key of Object.keys(sockets)) {
      const sock = sockets[key];
      if (isSocketConnected(sock)) connectedCount++;
      const q = getSenderQueue(key);
      if (q) {
        totalQueueSize += q.size + q.pending;
        sendingCount++;
      } else {
        const sess = userSessions[key];
        if (sess && (sess.messaging || sess.photoing || sess.stickering)) {
          idleCount++;
        } else {
          idleCount++;
        }
      }
    }

    res.status(200).json({
      status: 'ok',
      uptime: Math.floor(uptime),
      version: config.version,
      memory: {
        rss: Math.round(memory.rss / 1024 / 1024),
        heapTotal: Math.round(memory.heapTotal / 1024 / 1024),
        heapUsed: Math.round(memory.heapUsed / 1024 / 1024),
        external: Math.round(memory.external / 1024 / 1024),
      },
      cpu: {
        user: cpuUsage.user,
        system: cpuUsage.system,
      },
      sessions: {
        total: totalSessions,
        connected: connectedCount,
        reconnecting: reconnectingCount,
        sending: sendingCount,
        idle: idleCount,
      },
      queue: {
        totalSize: totalQueueSize,
      },
      readiness: readiness,
      timestamp: Date.now(),
    });
  });

  app.get('/readiness', (req, res) => {
    if (readiness) {
      res.json({ ready: true });
    } else {
      res.status(503).json({ ready: false });
    }
  });
}

// ── Express Setup ─────────────────────────────────────────────
const app = express();

app.use(cors({ origin: '*', credentials: true }));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static(publicDir));

app.use((req, res, next) => {
  req.id = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex');
  res.setHeader('X-Request-Id', req.id);
  next();
});

registerHealthRoutes(app);

// ── Rate Limiting ────────────────────────────────────────────
const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  message: { success: false, message: 'Too many login attempts, please try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});
const uploadLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 50,
  message: { success: false, message: 'Too many upload requests, please try again later.' },
});

// ── Multer with file filter ──────────────────────────────────
const fileFilter = (req, file, cb) => {
  const mime = file.mimetype;
  if (req.path === '/startMessaging') {
    if (mime === 'text/plain') return cb(null, true);
    return cb(new Error('Only .txt files are allowed for messages'), false);
  }
  if (req.path === '/startStickerSending') {
    if (mime === 'image/webp') return cb(null, true);
    return cb(new Error('Only .webp stickers are allowed'), false);
  }
  if (req.path === '/startPhotoSending') {
    if (['image/jpeg', 'image/png', 'image/gif', 'image/webp'].includes(mime)) {
      return cb(null, true);
    }
    return cb(new Error('Only image files (jpg, png, gif, webp) are allowed'), false);
  }
  cb(null, true);
};

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const unique = crypto.randomBytes(8).toString('hex');
    const ext = path.extname(file.originalname);
    cb(null, `${Date.now()}-${unique}${ext}`);
  },
});
const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter,
});

// ── In-memory userSessions ──────────────────────────────────
const userSessions = {};

function generateUniqueKey() {
  return crypto.randomBytes(16).toString('hex');
}

async function deleteSessionUploads(uniqueKey) {
  if (!fs.existsSync(uploadDir)) return;
  const files = await fs.promises.readdir(uploadDir);
  for (const file of files) {
    if (file.startsWith(`photo_${uniqueKey}`) || file.startsWith(`sticker_${uniqueKey}`)) {
      try {
        await fs.promises.unlink(path.join(uploadDir, file));
        logger.info({ file }, 'Deleted session upload');
      } catch (_) {}
    }
  }
}

function validatePhoneNumber(phone) {
  const cleaned = phone.replace(/[^0-9]/g, '');
  if (cleaned.length < 10 || cleaned.length > 15) return false;
  return cleaned;
}

function validateSpeed(speed) {
  const num = parseInt(speed, 10);
  return (Number.isInteger(num) && num >= 1 && num <= 60) ? num : false;
}

function validateTarget(target) {
  return typeof target === 'string' && target.length > 0;
}

// ── Routes ────────────────────────────────────────────────────
app.get('/ping', (req, res) => {
  res.json({ success: true, message: 'Server is alive!', timestamp: Date.now() });
});

app.post('/login', loginLimiter, async (req, res) => {
  try {
    let { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ success: false, message: 'Phone number is required!' });
    const cleaned = validatePhoneNumber(phoneNumber);
    if (!cleaned) return res.status(400).json({ success: false, message: 'Invalid phone number format' });
    phoneNumber = cleaned;
    logger.info({ phoneNumber, reqId: req.id }, 'Login request');

    const uniqueKey = generateUniqueKey();
    let responded = false;

    const sendPairingCode = (pairingCode, isConnected = false, errorMsg = null) => {
      if (responded) return;
      responded = true;
      if (errorMsg) {
        res.json({ success: false, message: 'Error generating pairing code', error: errorMsg, uniqueKey });
      } else if (isConnected) {
        userSessions[uniqueKey] = {
          phoneNumber,
          uniqueKey,
          connected: true,
          lastUpdateTimestamp: Date.now(),
          messaging: false,
          photoing: false,
          stickering: false,
        };
        saveSessions(userSessions);
        res.json({ success: true, message: 'WhatsApp Connected!', connected: true, uniqueKey });
      } else {
        res.json({ success: true, message: 'Pairing code generated', pairingCode, uniqueKey });
      }
    };

    await connectAndLogin(phoneNumber, uniqueKey, sendPairingCode);
  } catch (error) {
    if (!res.headersSent) {
      res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
    }
  }
});

app.post('/getGroupUID', async (req, res) => {
  try {
    const { uniqueKey } = req.body;
    if (!uniqueKey) return res.status(400).json({ success: false, message: 'Missing uniqueKey' });
    if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'No active session' });
    const sock = getSocket(uniqueKey);
    if (!sock || !isSocketConnected(sock)) return res.status(400).json({ success: false, message: 'WhatsApp not connected' });

    const groups = await sock.groupFetchAllParticipating();
    const groupUIDs = Object.values(groups).map(g => ({ groupName: g.subject, groupId: g.id }));
    logger.info({ uniqueKey, count: groupUIDs.length, reqId: req.id }, 'Fetched groups');
    res.json({ success: true, groupUIDs });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error fetching groups' });
  }
});

app.post('/startMessaging', uploadLimiter, upload.single('messageFile'), async (req, res) => {
  try {
    const { uniqueKey, target, hatersName, speed } = req.body;
    const filePath = req.file?.path;
    if (!uniqueKey || !target || !speed) return res.status(400).json({ success: false, message: 'Missing required fields!' });
    if (!validateTarget(target)) return res.status(400).json({ success: false, message: 'Invalid target' });
    const speedNum = validateSpeed(speed);
    if (!speedNum) return res.status(400).json({ success: false, message: 'Speed must be between 1 and 60' });
    if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'Invalid session key!' });
    const sock = getSocket(uniqueKey);
    if (!sock || !isSocketConnected(sock)) return res.status(400).json({ success: false, message: 'WhatsApp not connected!' });
    if (!filePath) return res.status(400).json({ success: false, message: 'No message file uploaded!' });

    let messages = [];
    try {
      const fileContent = await fs.promises.readFile(filePath, 'utf-8');
      messages = fileContent.split('\n').map(l => l.trim()).filter(l => l.length > 0);
      if (messages.length === 0) return res.status(400).json({ success: false, message: 'File has no valid messages!' });
    } catch (err) {
      return res.status(500).json({ success: false, message: 'Error reading file!' });
    } finally {
      try { await fs.promises.unlink(filePath); } catch (_) {}
    }

    cleanupSending(uniqueKey);
    userSessions[uniqueKey] = {
      ...userSessions[uniqueKey],
      target,
      hatersName: hatersName || '',
      messages,
      speed: speedNum,
      messaging: true,
      photoing: false,
      stickering: false,
    };
    await saveSessions(userSessions);

    startSending(uniqueKey, target, messages, {
      type: 'text',
      prefix: hatersName || '',
      speed: speedNum,
    });

    res.json({ success: true, message: 'Message automation started!', uniqueKey, messageCount: messages.length, target });
  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

app.post('/startPhotoSending', uploadLimiter, upload.fields([
  { name: 'photoFile', maxCount: 1 },
  { name: 'photoListFile', maxCount: 1 }
]), async (req, res) => {
  try {
    const { uniqueKey, target, caption, speed, mode } = req.body;
    if (!uniqueKey || !target || !speed) return res.status(400).json({ success: false, message: 'Missing required fields!' });
    if (!validateTarget(target)) return res.status(400).json({ success: false, message: 'Invalid target' });
    const speedNum = validateSpeed(speed);
    if (!speedNum) return res.status(400).json({ success: false, message: 'Speed must be between 1 and 60' });
    if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'Invalid session key!' });
    const sock = getSocket(uniqueKey);
    if (!sock || !isSocketConnected(sock)) return res.status(400).json({ success: false, message: 'WhatsApp not connected!' });

    let photoItems = [];
    if (mode === 'single') {
      const photoFile = req.files?.photoFile?.[0];
      if (!photoFile) return res.status(400).json({ success: false, message: 'No photo file uploaded!' });
      const destPath = path.join(uploadDir, `photo_${uniqueKey}${path.extname(photoFile.originalname)}`);
      await fs.promises.rename(photoFile.path, destPath);
      photoItems = [destPath];
    } else {
      const listFile = req.files?.photoListFile?.[0];
      if (!listFile) return res.status(400).json({ success: false, message: 'No photo list file uploaded!' });
      try {
        const content = await fs.promises.readFile(listFile.path, 'utf-8');
        photoItems = content.split('\n').map(l => l.trim()).filter(l => l.length > 0);
        if (photoItems.length === 0) return res.status(400).json({ success: false, message: 'Photo list file is empty!' });
      } finally {
        try { await fs.promises.unlink(listFile.path); } catch (_) {}
      }
    }

    cleanupSending(uniqueKey);
    userSessions[uniqueKey] = {
      ...userSessions[uniqueKey],
      target,
      caption: caption || '',
      photoItems,
      speed: speedNum,
      messaging: false,
      photoing: true,
      stickering: false,
    };
    await saveSessions(userSessions);

    startSending(uniqueKey, target, photoItems, {
      type: 'photo',
      caption: caption || '',
      speed: speedNum,
    });

    res.json({ success: true, message: 'Photo sending started!', uniqueKey, photoCount: photoItems.length, target });
  } catch (error) {
    logger.error({ err: error.message, reqId: req.id }, 'Photo route error');
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

app.post('/startStickerSending', uploadLimiter, upload.single('stickerFile'), async (req, res) => {
  try {
    const { uniqueKey, target, speed } = req.body;
    if (!uniqueKey || !target || !speed) return res.status(400).json({ success: false, message: 'Missing required fields!' });
    if (!validateTarget(target)) return res.status(400).json({ success: false, message: 'Invalid target' });
    const speedNum = validateSpeed(speed);
    if (!speedNum) return res.status(400).json({ success: false, message: 'Speed must be between 1 and 60' });
    if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'Invalid session key!' });
    const sock = getSocket(uniqueKey);
    if (!sock || !isSocketConnected(sock)) return res.status(400).json({ success: false, message: 'WhatsApp not connected!' });

    const stickerFile = req.file;
    if (!stickerFile) return res.status(400).json({ success: false, message: 'No sticker file uploaded!' });
    const destPath = path.join(uploadDir, `sticker_${uniqueKey}${path.extname(stickerFile.originalname || '.webp')}`);
    await fs.promises.rename(stickerFile.path, destPath);

    cleanupSending(uniqueKey);
    userSessions[uniqueKey] = {
      ...userSessions[uniqueKey],
      target,
      stickerPath: destPath,
      speed: speedNum,
      messaging: false,
      photoing: false,
      stickering: true,
    };
    await saveSessions(userSessions);

    startSending(uniqueKey, target, [destPath], {
      type: 'sticker',
      speed: speedNum,
    });

    res.json({ success: true, message: 'Sticker sending started!', uniqueKey, target });
  } catch (error) {
    logger.error({ err: error.message, reqId: req.id }, 'Sticker route error');
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

app.get('/sessionStatus/:uniqueKey', (req, res) => {
  const { uniqueKey } = req.params;
  const session = userSessions[uniqueKey];
  const stats = getSessionStats(uniqueKey);
  if (!session) return res.json({ exists: false });
  const sock = getSocket(uniqueKey);
  const connected = isSocketConnected(sock);
  res.json({
    exists: true,
    connected: connected,
    messaging: (session.messaging || session.photoing || session.stickering) && !getStopFlags(uniqueKey)?.stopped,
    sent: stats.sent,
    failed: stats.failed,
    lastMessage: stats.lastMessage,
    type: stats.type || 'msg',
    target: session.target,
    speed: session.speed,
    messageCount: session.messages?.length || session.photoItems?.length || 1,
  });
});

app.post('/stop', async (req, res) => {
  const { uniqueKey } = req.body;
  if (!uniqueKey) return res.status(400).json({ success: false, message: 'Missing uniqueKey' });
  if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'No session found' });

  try {
    cleanupSending(uniqueKey);

    const sock = getSocket(uniqueKey);
    if (sock) {
      try {
        sock.ev.removeAllListeners();
        await sock.logout();
        logger.info({ uniqueKey }, 'Logged out successfully');
      } catch (err) {
        logger.warn({ uniqueKey, err: err.message }, 'Error during logout, forcing close');
        await closeSocketSafely(sock);
      }
    }

    removeActiveSocket(uniqueKey);
    const sessionPath = path.join(sessionDir, uniqueKey);
    removeDir(sessionPath);
    await deleteSessionUploads(uniqueKey);
    clearSessionStats(uniqueKey);
    delete userSessions[uniqueKey];
    await saveSessions(userSessions);

    logger.info({ uniqueKey }, 'Stopped and logged out');
    res.json({ success: true, message: 'Process stopped and logged out!' });
  } catch (error) {
    logger.error({ uniqueKey, err: error.message }, 'Error in stop route');
    res.status(500).json({ success: false, message: 'Error stopping process' });
  }
});

app.get('/', (req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

app.use((err, req, res, next) => {
  logger.error({ err: err.message, stack: err.stack, reqId: req.id }, 'Unhandled error');
  if (err instanceof multer.MulterError) {
    return res.status(400).json({ success: false, message: err.message });
  }
  res.status(500).json({ success: false, message: 'Internal server error' });
});

// ── Startup & Cleanup ──────────────────────────────────────
async function cleanupOldSessions() {
  if (!fs.existsSync(sessionDir)) return;
  const now = Date.now();
  const maxAge = 7 * 24 * 60 * 60 * 1000;
  const dirs = await fs.promises.readdir(sessionDir);
  for (const folder of dirs) {
    const folderPath = path.join(sessionDir, folder);
    try {
      const stats = await fs.promises.stat(folderPath);
      if (stats.isDirectory() && (now - stats.mtimeMs > maxAge) && !userSessions[folder]) {
        logger.info({ folder }, 'Deleting old session');
        removeDir(folderPath);
      }
    } catch (_) {}
  }
}

async function cleanupUploads() {
  if (!fs.existsSync(uploadDir)) return;
  const files = await fs.promises.readdir(uploadDir);
  for (const file of files) {
    const filePath = path.join(uploadDir, file);
    try {
      const stats = await fs.promises.stat(filePath);
      if (Date.now() - stats.mtimeMs > 24 * 60 * 60 * 1000) {
        await fs.promises.unlink(filePath);
        logger.info({ file }, 'Deleted old upload');
      }
    } catch (_) {}
  }
}

async function restoreSessions() {
  const saved = await loadSessions();
  Object.assign(userSessions, saved);
  const entries = Object.entries(userSessions);
  if (entries.length === 0) return;

  for (const [uniqueKey, session] of entries) {
    if (!session.phoneNumber || !session.uniqueKey) continue;
    const sessionPath = path.join(sessionDir, session.uniqueKey);
    if (!fs.existsSync(sessionPath)) continue;

    logger.info({ uniqueKey, phone: session.phoneNumber }, 'Restoring session');
    try {
      await connectAndLogin(session.phoneNumber, session.uniqueKey, null);
      await new Promise(r => setTimeout(r, 2000));
    } catch (err) {
      logger.error({ uniqueKey, err: err.message }, 'Failed to restore session');
    }
  }
  logger.info('Session restoration complete');
}

// ── Graceful Shutdown ──────────────────────────────────────
let isShuttingDown = false;
let serverInstance = null;

async function shutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info({ signal }, 'Shutting down gracefully...');

  readiness = false;

  if (serverInstance) {
    await new Promise((resolve) => {
      serverInstance.close(resolve);
    });
    logger.info('Express server closed');
  }

  clearAllReconnectTimers();
  await closeAllSockets();
  await closeRedis();
  await saveSessions(userSessions);

  logger.info('Shutdown complete');
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

process.on('unhandledRejection', (reason) => {
  logger.fatal({ reason }, 'Unhandled Rejection');
  shutdown('unhandledRejection').then(() => process.exit(1));
});
process.on('uncaughtException', (err) => {
  logger.fatal({ err }, 'Uncaught Exception');
  shutdown('uncaughtException').then(() => process.exit(1));
});

// ── Start Server ──────────────────────────────────────────────
const server = app.listen(config.port, '0.0.0.0', async () => {
  serverInstance = server;
  logger.info(`🚀 Server running on port ${config.port}`);
  logger.info(`🔗 Test: http://localhost:${config.port}/ping`);

  await cleanupOldSessions();
  await cleanupUploads();
  setInterval(cleanupOldSessions, 60 * 60 * 1000);
  setInterval(cleanupUploads, 30 * 60 * 1000);

  await restoreSessions();
});

export default server;
