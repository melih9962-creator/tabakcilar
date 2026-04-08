try {
  require("dotenv").config();
} catch (_) {}

const express = require("express");
const fs = require("fs");
const path = require("path");
const util = require("util");

const app = express();
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));

/* =========================
   ENV / AYARLAR
========================= */
const PORT = Number(process.env.PORT || 3000);

/* Sadece çağrı bildirim Telegram botu */
const CALL_TELEGRAM_BOT_TOKEN = process.env.CALL_TELEGRAM_BOT_TOKEN || "";
const CALL_TELEGRAM_CHAT_IDS = (process.env.CALL_TELEGRAM_CHAT_IDS || "")
  .split(",")
  .map((x) => x.trim())
  .filter(Boolean);

/* Self ping */
const ENABLE_SELF_PING = process.env.ENABLE_SELF_PING === "true";
const APP_BASE_URL = String(process.env.APP_BASE_URL || "").replace(/\/+$/, "");
const SELF_PING_INTERVAL_MS = Number(process.env.SELF_PING_INTERVAL_MS || 5 * 60 * 1000);

/* Dosyalar */
const CONTACTS_FILE = path.join(__dirname, process.env.CONTACTS_FILE || "rehber.csv");
const CALL_TASKS_FILE = path.join(__dirname, process.env.CALL_TASKS_FILE || "call_tasks.json");

/* Saat */
const TURKEY_TIMEZONE = "Europe/Istanbul";
const TURKEY_TIME_API_URL =
  process.env.TURKEY_TIME_API_URL ||
  "https://www.timeapi.io/api/Time/current/zone?timeZone=Europe/Istanbul";

/* Temizlik */
const CALL_STATE_TTL_MS = Number(process.env.CALL_STATE_TTL_MS || 6 * 60 * 60 * 1000); // 6 saat

/* =========================
   STATE
========================= */
let contactMap = {};
let callTasksMap = {};

/*
  unique_id -> {
    type: "inbound" | "outbound" | "unknown",
    phone: "905xxxxxxxxx",
    answered: boolean,
    createdAt: number,
    updatedAt: number
  }
*/
const callMap = {};

/* Aynı task için eşzamanlı oluşturmayı engelle */
const missedTaskInFlight = new Set();

/* Bot polling state */
const botPollingState = {
  call: {
    offset: 0,
    processedUpdateIds: new Set(),
  },
};

/* Türkiye saat cache */
let turkeyTimeCache = {
  lastOkMs: 0,
  offsetMs: 0,
  source: "init",
};

/* =========================
   LOG
========================= */
function formatLogArg(value) {
  try {
    return util.inspect(value, {
      depth: null,
      colors: false,
      maxArrayLength: null,
      maxStringLength: null,
      breakLength: 140,
      compact: false,
    });
  } catch (_) {
    return String(value);
  }
}

function log(...args) {
  console.log(`${new Date().toISOString()} - [INFO] ${args.map(formatLogArg).join(" ")}`);
}

function logError(...args) {
  console.error(`${new Date().toISOString()} - [ERROR] ${args.map(formatLogArg).join(" ")}`);
}

/* =========================
   GENEL HELPERS
========================= */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

function normalizePhone(phone) {
  let digits = String(phone || "").replace(/\D/g, "").trim();

  if (!digits) return "";

  if (digits.startsWith("00")) digits = digits.slice(2);
  if (digits.startsWith("90") && digits.length === 12) return digits;
  if (digits.startsWith("0") && digits.length === 11) return `90${digits.slice(1)}`;
  if (digits.length === 10 && digits.startsWith("5")) return `90${digits}`;

  return digits;
}

function isAllowedCallTelegramChat(chatId) {
  return CALL_TELEGRAM_CHAT_IDS.includes(String(chatId));
}

function safeJsonRead(filePath, fallback = {}) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (e) {
    logError(`JSON okunamadı: ${filePath}`, e.message);
    return fallback;
  }
}

function safeJsonWrite(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
  } catch (e) {
    logError(`JSON yazılamadı: ${filePath}`, e.message);
  }
}

function ensureEnv() {
  const missing = [];

  if (!CALL_TELEGRAM_BOT_TOKEN) missing.push("CALL_TELEGRAM_BOT_TOKEN");
  if (!CALL_TELEGRAM_CHAT_IDS.length) missing.push("CALL_TELEGRAM_CHAT_IDS");

  if (missing.length) {
    log("Eksik ENV değişkenleri:", missing.join(", "));
  }

  if (ENABLE_SELF_PING && !APP_BASE_URL) {
    log("Uyarı: ENABLE_SELF_PING=true ama APP_BASE_URL boş.");
  }
}

/* =========================
   SAAT / TÜRKİYE SAATİ
========================= */
function getSystemTurkeyParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: TURKEY_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(date);

  const get = (type) => parts.find((p) => p.type === type)?.value;

  return {
    year: get("year"),
    month: get("month"),
    day: get("day"),
    hour: Number(get("hour")),
    minute: Number(get("minute")),
    second: Number(get("second")),
  };
}

async function refreshTurkeyTimeOffset() {
  try {
    const response = await fetch(TURKEY_TIME_API_URL, { method: "GET" });
    const data = await response.json().catch(() => ({}));

    if (!response.ok) {
      throw new Error(`time api status ${response.status}`);
    }

    const year = Number(data.year);
    const month = Number(data.month);
    const day = Number(data.day);
    const hour = Number(data.hour);
    const minute = Number(data.minute);
    const second = Number(data.seconds ?? data.second ?? 0);

    if (
      !year || !month || !day ||
      Number.isNaN(hour) || Number.isNaN(minute) || Number.isNaN(second)
    ) {
      throw new Error(`time api invalid payload: ${JSON.stringify(data)}`);
    }

    const turkeyUtcMs = Date.UTC(year, month - 1, day, hour, minute, second);
    turkeyTimeCache.offsetMs = turkeyUtcMs - Date.now();
    turkeyTimeCache.lastOkMs = Date.now();
    turkeyTimeCache.source = "web";

    log("Türkiye saati webden güncellendi.");
    return true;
  } catch (e) {
    logError("Türkiye saati webden alınamadı, fallback kullanılacak:", e.message);
    return false;
  }
}

async function getTurkeyNowDate() {
  const now = Date.now();

  if (!turkeyTimeCache.lastOkMs || now - turkeyTimeCache.lastOkMs > 5 * 60 * 1000) {
    await refreshTurkeyTimeOffset();
  }

  if (turkeyTimeCache.lastOkMs) {
    return new Date(Date.now() + turkeyTimeCache.offsetMs);
  }

  const p = getSystemTurkeyParts(new Date());
  return new Date(Date.UTC(
    Number(p.year),
    Number(p.month) - 1,
    Number(p.day),
    Number(p.hour),
    Number(p.minute),
    Number(p.second)
  ));
}

async function getDateKeyInTurkey() {
  const d = await getTurkeyNowDate();
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

function formatDateTimeTR(dateValue) {
  return new Date(dateValue).toLocaleString("tr-TR", {
    timeZone: TURKEY_TIMEZONE,
    hour12: false,
  });
}

async function getTaskKeyByPhoneAndDay(phone) {
  const normalizedPhone = normalizePhone(phone);
  const dateKey = await getDateKeyInTurkey();
  return `${normalizedPhone}_${dateKey}`;
}

/* =========================
   CONTACTS
========================= */
function loadContacts() {
  try {
    if (!fs.existsSync(CONTACTS_FILE)) {
      log("rehber.csv bulunamadı:", CONTACTS_FILE);
      contactMap = {};
      return;
    }

    const raw = fs.readFileSync(CONTACTS_FILE, "utf8");
    const lines = raw
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    if (lines.length < 2) {
      log("rehber.csv boş veya yetersiz.");
      contactMap = {};
      return;
    }

    const firstLine = lines[0].replace(/^\uFEFF/, "");
    const delimiter = firstLine.includes(";") ? ";" : ",";
    const headers = firstLine.split(delimiter).map((h) => h.trim().toLowerCase());

    const phoneIndex = headers.findIndex((h) =>
      ["telefon", "phone", "numara", "telefon no", "telefonno"].includes(h)
    );

    const nameIndex = headers.findIndex((h) =>
      ["isim", "ad", "name", "ad soyad", "isim soyisim"].includes(h)
    );

    if (phoneIndex === -1 || nameIndex === -1) {
      log("rehber.csv içinde uygun kolonlar bulunamadı.");
      contactMap = {};
      return;
    }

    const map = {};

    for (let i = 1; i < lines.length; i++) {
      const row = lines[i].split(delimiter);
      const phone = normalizePhone(row[phoneIndex]);
      const name = String(row[nameIndex] || "").trim();

      if (phone && name) {
        map[phone] = name;
      }
    }

    contactMap = map;
    log(`Rehber yüklendi. Kayıt sayısı: ${Object.keys(contactMap).length}`);
  } catch (error) {
    contactMap = {};
    logError("rehber.csv okunamadı:", error.message);
  }
}

function getName(phone) {
  return contactMap[normalizePhone(phone)] || "Kayıtlı değil";
}

/* =========================
   CALL STATE
========================= */
function upsertCallState(uniqueId, patch) {
  if (!uniqueId) return null;

  const existing = callMap[uniqueId] || {
    createdAt: Date.now(),
    updatedAt: Date.now(),
    answered: false,
  };

  callMap[uniqueId] = {
    ...existing,
    ...patch,
    updatedAt: Date.now(),
  };

  return callMap[uniqueId];
}

function getCallState(uniqueId) {
  return callMap[uniqueId] || null;
}

function deleteCallState(uniqueId) {
  if (!uniqueId) return;
  delete callMap[uniqueId];
}

function cleanupStaleState() {
  const now = Date.now();

  for (const [uniqueId, state] of Object.entries(callMap)) {
    if (!state?.updatedAt || now - state.updatedAt > CALL_STATE_TTL_MS) {
      delete callMap[uniqueId];
    }
  }
}

/* =========================
   CALL TASKS
========================= */
function loadCallTasks() {
  callTasksMap = safeJsonRead(CALL_TASKS_FILE, {});
  log(`Çağrı görevleri yüklendi. Kayıt sayısı: ${Object.keys(callTasksMap).length}`);
}

function saveCallTasks() {
  safeJsonWrite(CALL_TASKS_FILE, callTasksMap);
}

function formatTelegramUser(user) {
  if (!user) return "Bilinmiyor";
  const fullName = [user.first_name, user.last_name].filter(Boolean).join(" ").trim();
  if (fullName) return fullName;
  if (user.username) return `@${user.username}`;
  return String(user.id || "Bilinmiyor");
}

function getTaskStatusLabel(status) {
  switch (status) {
    case "done":
      return "✅ Geri Arandı";
    case "later":
      return "⏰ Daha Sonra";
    case "ignore":
      return "🚫 Gereksiz";
    case "open":
    default:
      return "🟥 Açık";
  }
}

function buildCallTaskText(task) {
  const lines = [
    "📞 Kaçan çağrı",
    "",
    `Durum: ${getTaskStatusLabel(task.status)}`,
    `👤 ${task.name || "Kayıtlı değil"}`,
    `📱 ${task.phone || "-"}`,
    `🔁 Bugün ulaşamama sayısı: ${task.missedCount || 1}`,
    `🕒 İlk çağrı: ${formatDateTimeTR(task.createdAt)}`,
    `⏱ Son arama: ${formatDateTimeTR(task.lastCallAt || task.createdAt)}`,
  ];

  if (task.updatedBy) {
    lines.push(`🙋 Güncelleyen: ${task.updatedBy}`);
  }

  if (task.updatedAt && task.updatedAt !== task.createdAt) {
    lines.push(`📝 Son Güncelleme: ${formatDateTimeTR(task.updatedAt)}`);
  }

  return lines.join("\n");
}

function buildCallTaskKeyboard(task) {
  const current = task.status || "open";
  const label = (status, text) => (current === status ? `• ${text}` : text);

  return {
    inline_keyboard: [
      [
        { text: label("done", "✅ Geri Arandı"), callback_data: `calltask|done|${task.id}` },
        { text: label("later", "⏰ Daha Sonra"), callback_data: `calltask|later|${task.id}` },
      ],
      [
        { text: label("ignore", "🚫 Gereksiz"), callback_data: `calltask|ignore|${task.id}` },
        { text: label("open", "🟥 Açık"), callback_data: `calltask|open|${task.id}` },
      ],
    ],
  };
}

async function createOrUpdateDailyMissedCallTask(phone) {
  const taskId = await getTaskKeyByPhoneAndDay(phone);
  const normalizedPhone = normalizePhone(phone);

  if (callTasksMap[taskId]) {
    const existing = callTasksMap[taskId];
    existing.missedCount = (existing.missedCount || 1) + 1;
    existing.lastCallAt = Date.now();
    existing.updatedAt = Date.now();
    existing.name = getName(normalizedPhone);
    saveCallTasks();
    return { task: existing, created: false };
  }

  const task = {
    id: taskId,
    phone: normalizedPhone,
    name: getName(normalizedPhone),
    status: "open",
    createdAt: Date.now(),
    updatedAt: Date.now(),
    lastCallAt: Date.now(),
    updatedBy: null,
    missedCount: 1,
    messageRefs: [],
    type: "missed_call_daily",
  };

  callTasksMap[taskId] = task;
  saveCallTasks();
  return { task, created: true };
}

async function sendMissedCallTaskNotifications(task) {
  if (!CALL_TELEGRAM_BOT_TOKEN || !CALL_TELEGRAM_CHAT_IDS.length) {
    log("Call bot pasif; missed call görevi gönderilmedi.");
    return;
  }

  const text = buildCallTaskText(task);
  const keyboard = buildCallTaskKeyboard(task);

  if (!task.messageRefs?.length) {
    for (const chatId of CALL_TELEGRAM_CHAT_IDS) {
      try {
        const result = await sendTelegramMessageViaBot(
          CALL_TELEGRAM_BOT_TOKEN,
          chatId,
          text,
          keyboard
        );

        if (result?.message_id) {
          task.messageRefs.push({
            chatId: String(chatId),
            messageId: result.message_id,
          });
        }
      } catch (e) {
        logError(`Missed call task gönderim hatası -> chat_id=${chatId}:`, e.message);
      }
    }

    task.updatedAt = Date.now();
    saveCallTasks();
    return;
  }

  await syncCallTaskMessages(task);
}

async function syncCallTaskMessages(task) {
  if (!task?.messageRefs?.length) {
    saveCallTasks();
    return;
  }

  const text = buildCallTaskText(task);
  const keyboard = buildCallTaskKeyboard(task);

  for (const ref of task.messageRefs) {
    try {
      await telegramRequest(CALL_TELEGRAM_BOT_TOKEN, "editMessageText", {
        chat_id: String(ref.chatId),
        message_id: ref.messageId,
        text,
        reply_markup: keyboard,
      });
    } catch (e) {
      logError(
        `Call task mesaj güncelleme hatası -> chat_id=${ref.chatId}, message_id=${ref.messageId}:`,
        e.message
      );
    }
  }

  saveCallTasks();
}

/* =========================
   TELEGRAM API
========================= */
async function telegramRequest(botToken, method, payload) {
  const url = `https://api.telegram.org/bot${botToken}/${method}`;

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const data = await response.json().catch(() => ({}));

  if (!response.ok || !data.ok) {
    throw new Error(`Telegram ${method} hatası: ${JSON.stringify(data)}`);
  }

  return data;
}

async function sendTelegramMessageViaBot(botToken, chatId, text, replyMarkup = null) {
  const payload = {
    chat_id: String(chatId),
    text: String(text || ""),
  };

  if (replyMarkup) payload.reply_markup = replyMarkup;

  const data = await telegramRequest(botToken, "sendMessage", payload);
  return data.result || null;
}

async function sendCallTelegram(text, targetChatIds = null, buttons = null) {
  const chatIds = targetChatIds || CALL_TELEGRAM_CHAT_IDS;

  if (!CALL_TELEGRAM_BOT_TOKEN || !chatIds.length) {
    log("Call bot aktif değil. Bildirim atlandı.");
    return;
  }

  for (const chatId of chatIds) {
    try {
      await sendTelegramMessageViaBot(CALL_TELEGRAM_BOT_TOKEN, chatId, text, buttons);
      log(`Call bot mesaj gönderildi -> chat_id=${chatId}`);
    } catch (e) {
      logError(`Call bot gönderim hatası -> chat_id=${chatId}:`, e.message);
    }
  }
}

async function answerCallCallback(id, text = "") {
  try {
    await telegramRequest(CALL_TELEGRAM_BOT_TOKEN, "answerCallbackQuery", {
      callback_query_id: id,
      text,
      show_alert: false,
    });
  } catch (e) {
    logError("Call callback cevap hatası:", e.message);
  }
}

/* =========================
   CALL BOT MESSAGE PROCESS
========================= */
async function processCallTelegramMessage(update) {
  const message = update.message;
  if (!message) return;

  const chatId = String(message.chat.id);
  const text = String(message.text || "").trim();

  if (!isAllowedCallTelegramChat(chatId)) return;
  if (!text) return;

  if (text === "/help") {
    await sendCallTelegram(
      "Komutlar:\n\n" +
        "/help\n" +
        "/open_calls\n" +
        "/sync_calls\n" +
        "/reload_contacts\n\n" +
        "/open_calls -> açık çağrı görevlerini listeler\n" +
        "/sync_calls -> bu chate açık görev kartlarını tekrar yollar\n" +
        "/reload_contacts -> rehberi yeniden yükler",
      [chatId]
    );
    return;
  }

  if (text === "/reload_contacts") {
    loadContacts();
    await sendCallTelegram("✅ Rehber yeniden yüklendi.", [chatId]);
    return;
  }

  if (text === "/open_calls") {
    const openTasks = Object.values(callTasksMap)
      .filter((t) => t.status === "open")
      .sort((a, b) => b.lastCallAt - a.lastCallAt)
      .slice(0, 50);

    if (!openTasks.length) {
      await sendCallTelegram("Açık çağrı görevi yok.", [chatId]);
      return;
    }

    const lines = ["📋 Açık çağrı görevleri", ""];
    for (const t of openTasks) {
      lines.push(`• ${t.name || "Kayıtlı değil"} - ${t.phone} (${t.missedCount || 1})`);
    }

    await sendCallTelegram(lines.join("\n"), [chatId]);
    return;
  }

  if (text === "/sync_calls") {
    const openTasks = Object.values(callTasksMap)
      .filter((t) => t.status === "open")
      .sort((a, b) => b.lastCallAt - a.lastCallAt);

    if (!openTasks.length) {
      await sendCallTelegram("Açık çağrı görevi yok.", [chatId]);
      return;
    }

    for (const task of openTasks) {
      try {
        const result = await sendTelegramMessageViaBot(
          CALL_TELEGRAM_BOT_TOKEN,
          chatId,
          buildCallTaskText(task),
          buildCallTaskKeyboard(task)
        );

        if (result?.message_id) {
          const exists = task.messageRefs.some(
            (x) =>
              String(x.chatId) === String(chatId) &&
              String(x.messageId) === String(result.message_id)
          );

          if (!exists) {
            task.messageRefs.push({
              chatId: String(chatId),
              messageId: result.message_id,
            });
          }
        }
      } catch (e) {
        logError("sync_calls hatası:", e.message);
      }
    }

    saveCallTasks();
  }
}

/* =========================
   CALL BOT CALLBACK PROCESS
========================= */
async function processCallTelegramCallback(update) {
  const callback = update.callback_query;
  if (!callback) return;

  const chatId = String(callback.message?.chat?.id || "");
  if (!isAllowedCallTelegramChat(chatId)) {
    await answerCallCallback(callback.id, "Yetkisiz");
    return;
  }

  const data = callback.data || "";
  const parts = data.split("|");

  if (parts.length !== 3 || parts[0] !== "calltask") {
    await answerCallCallback(callback.id, "Bilinmeyen işlem");
    return;
  }

  const newStatus = parts[1];
  const taskId = parts[2];
  const task = callTasksMap[taskId];

  if (!task) {
    await answerCallCallback(callback.id, "Görev bulunamadı");
    return;
  }

  task.status = newStatus;
  task.updatedAt = Date.now();
  task.updatedBy = formatTelegramUser(callback.from);

  saveCallTasks();
  await syncCallTaskMessages(task);
  await answerCallCallback(callback.id, `Durum güncellendi: ${getTaskStatusLabel(newStatus)}`);
}

/* =========================
   TELEGRAM POLLING
========================= */
async function checkTelegram(botName, botToken, state, onUpdate) {
  if (!botToken) return;

  const url = `https://api.telegram.org/bot${botToken}/getUpdates?offset=${state.offset}&timeout=20`;

  const response = await fetch(url);
  const data = await response.json().catch(() => ({}));

  if (!data.ok) {
    logError(`${botName} update hatası:`, data);
    return;
  }

  const updates = data.result || [];

  if (updates.length) {
    log(`${botName} polling batch:`, { count: updates.length, offset: state.offset });
  }

  for (const update of updates) {
    if (state.processedUpdateIds.has(update.update_id)) continue;

    state.processedUpdateIds.add(update.update_id);

    if (state.processedUpdateIds.size > 1000) {
      const first = state.processedUpdateIds.values().next().value;
      state.processedUpdateIds.delete(first);
    }

    state.offset = update.update_id + 1;

    log(`${botName} update:`, update);

    try {
      await onUpdate(update);
    } catch (e) {
      logError(`${botName} update işleme hatası:`, e.message);
    }
  }
}

async function startBotPolling(botName, botToken, state, onUpdate) {
  if (!botToken) {
    log(`${botName} polling başlatılmadı: token yok.`);
    return;
  }

  while (true) {
    try {
      await checkTelegram(botName, botToken, state, onUpdate);
    } catch (e) {
      logError(`${botName} polling hata:`, e.message);
      await sleep(2000);
    }
  }
}

async function handleCallBotUpdate(update) {
  if (update.message) {
    await processCallTelegramMessage(update);
  }

  if (update.callback_query) {
    await processCallTelegramCallback(update);
  }
}

/* =========================
   REQUEST LOGGING
========================= */
app.use((req, res, next) => {
  const startedAt = Date.now();

  if (req.path !== "/health") {
    log("HTTP IN", {
      method: req.method,
      url: req.originalUrl,
      ip: req.ip || req.headers["x-forwarded-for"] || "",
      query: req.query || {},
      body: req.body || {},
    });
  }

  res.on("finish", () => {
    if (req.path !== "/health") {
      log("HTTP OUT", {
        method: req.method,
        url: req.originalUrl,
        status: res.statusCode,
        durationMs: Date.now() - startedAt,
      });
    }
  });

  next();
});

/* =========================
   HEALTH
========================= */
app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

/* =========================
   SELF PING
========================= */
function startSelfPing() {
  if (!ENABLE_SELF_PING) {
    log("Self-ping kapalı.");
    return;
  }

  if (!APP_BASE_URL) {
    log("Self-ping başlatılmadı: APP_BASE_URL yok.");
    return;
  }

  const target = `${APP_BASE_URL}/health`;

  setInterval(async () => {
    try {
      const response = await fetch(target, { method: "GET" });
      log(`Self-ping -> ${response.status}`);
    } catch (e) {
      logError("Self-ping hatası:", e.message);
    }
  }, SELF_PING_INTERVAL_MS);

  log("Self-ping aktif:", target, "intervalMs:", SELF_PING_INTERVAL_MS);
}

/* =========================
   SANTRAL WEBHOOK TEST
========================= */
app.all("/call-webhook-test", (req, res) => {
  log("========== SANTRAL WEBHOOK TEST ==========");
  log("METHOD:", req.method);
  log("QUERY:", req.query);
  log("BODY:", req.body);
  log("=========================================");
  res.status(200).json({ ok: true });
});

/* =========================
   SANTRAL WEBHOOK
========================= */
app.post("/call-webhook", async (req, res) => {
  res.sendStatus(200);

  try {
    const data = req.body || {};
    const scenario = String(data.scenario || "").trim();
    const uniqueId = String(data.unique_id || data.asteriskId || "").trim();
    const phone = normalizePhone(data.customer_num || data.aranan || "");

    log("Çağrı webhook event:", data);

    if (!scenario || !uniqueId) {
      log("Çağrı webhook atlandı: scenario veya unique_id eksik.");
      return;
    }

    if (scenario === "Outbound_call") {
      upsertCallState(uniqueId, {
        type: "outbound",
        phone,
        answered: false,
      });
      return;
    }

    if (scenario === "InboundtoPBX" || scenario === "Inbound_call" || scenario === "Queue") {
      upsertCallState(uniqueId, {
        type: "inbound",
        phone,
        answered: false,
      });
      return;
    }

    if (scenario === "Answer") {
      const current = getCallState(uniqueId);

      upsertCallState(uniqueId, {
        type: current?.type || "unknown",
        phone: current?.phone || phone,
        answered: true,
      });

      log("Answer event işlendi:", {
        uniqueId,
        phone: current?.phone || phone,
        type: current?.type || "unknown",
      });

      return;
    }

    if (scenario === "Hangup") {
      const state = getCallState(uniqueId);
      const targetPhone = normalizePhone(state?.phone || phone);

      if (state?.type === "outbound") {
        log("Outbound Hangup -> bildirim üretilmedi:", targetPhone);
        deleteCallState(uniqueId);
        return;
      }

      if (state?.type === "inbound") {
        if (!state.answered) {
          const taskKey = await getTaskKeyByPhoneAndDay(targetPhone);

          if (missedTaskInFlight.has(taskKey)) {
            log("Missed call task atlanıyor -> işlem zaten devam ediyor:", taskKey, targetPhone);
            deleteCallState(uniqueId);
            return;
          }

          missedTaskInFlight.add(taskKey);

          try {
            const { task, created } = await createOrUpdateDailyMissedCallTask(targetPhone);
            await sendMissedCallTaskNotifications(task);

            if (created) {
              log("Inbound Hangup -> yeni missed call kartı oluşturuldu:", taskKey, targetPhone);
            } else {
              log(
                "Inbound Hangup -> mevcut kart güncellendi:",
                taskKey,
                targetPhone,
                "count:",
                task.missedCount
              );
            }
          } finally {
            missedTaskInFlight.delete(taskKey);
          }
        } else {
          log("Inbound Hangup -> çağrı cevaplanmış, kart oluşturulmadı:", targetPhone);
        }

        deleteCallState(uniqueId);
        return;
      }

      log("Hangup -> eşleşen çağrı state bulunamadı:", uniqueId, targetPhone);
      deleteCallState(uniqueId);
      return;
    }

    if (scenario === "QueueLeave" || scenario === "cdr") {
      log(`${scenario} event alındı:`, data);
      return;
    }
  } catch (e) {
    logError("Çağrı webhook işleme hatası:", e.message);
  }
});

/* =========================
   GLOBAL ERROR CAPTURE
========================= */
process.on("uncaughtException", (err) => {
  logError("uncaughtException:", err);
});

process.on("unhandledRejection", (reason) => {
  logError("unhandledRejection:", reason);
});

/* =========================
   START
========================= */
async function bootstrap() {
  ensureEnv();
  loadContacts();
  loadCallTasks();

  await refreshTurkeyTimeOffset();

  setInterval(cleanupStaleState, 10 * 60 * 1000);

  /* Türkiye saat offset'ini arada yenile */
  setInterval(async () => {
    try {
      await refreshTurkeyTimeOffset();
    } catch (e) {
      logError("Zaman offset refresh hatası:", e.message);
    }
  }, 5 * 60 * 1000);

  app.listen(PORT, "0.0.0.0", () => {
    log(`Server çalışıyor: http://0.0.0.0:${PORT}`);

    startSelfPing();

    startBotPolling(
      "Call bot",
      CALL_TELEGRAM_BOT_TOKEN,
      botPollingState.call,
      handleCallBotUpdate
    );
  });
}

bootstrap().catch((e) => {
  logError("Bootstrap hatası:", e.message);
  process.exit(1);
});
