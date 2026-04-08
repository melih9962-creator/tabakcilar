try {
  require("dotenv").config();
} catch (_) {}

const express = require("express");
const fs = require("fs");
const path = require("path");
const util = require("util");
const { Client } = require("ldapts");

const app = express();
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));

/* =========================
   ENV / AYARLAR
========================= */
const PORT = Number(process.env.PORT || 3000);

const CALL_TELEGRAM_BOT_TOKEN = process.env.CALL_TELEGRAM_BOT_TOKEN || "";
const CALL_TELEGRAM_CHAT_IDS = (process.env.CALL_TELEGRAM_CHAT_IDS || "")
  .split(",")
  .map((x) => x.trim())
  .filter(Boolean);

const ENABLE_SELF_PING = process.env.ENABLE_SELF_PING === "true";
const APP_BASE_URL = String(process.env.APP_BASE_URL || "").replace(/\/+$/, "");
const SELF_PING_INTERVAL_MS = Number(process.env.SELF_PING_INTERVAL_MS || 5 * 60 * 1000);

const LDAP_URL = process.env.LDAP_URL || "";
const LDAP_BIND_DN = process.env.LDAP_BIND_DN || "";
const LDAP_PASSWORD = process.env.LDAP_PASSWORD || "";
const LDAP_BASE_DN = process.env.LDAP_BASE_DN || "";
const LDAP_SEARCH_FILTER = process.env.LDAP_SEARCH_FILTER || "(telephonenumber=*)";
const LDAP_NAME_ATTR = process.env.LDAP_NAME_ATTR || "cn";
const LDAP_PHONE_ATTR = process.env.LDAP_PHONE_ATTR || "telephonenumber";
const LDAP_REFRESH_MS = Number(process.env.LDAP_REFRESH_MS || 5 * 60 * 1000);

const CALL_TASKS_FILE = path.join(__dirname, process.env.CALL_TASKS_FILE || "call_tasks.json");
const CALL_STATE_TTL_MS = Number(process.env.CALL_STATE_TTL_MS || 6 * 60 * 60 * 1000);

const TURKEY_TIMEZONE = "Europe/Istanbul";

/* =========================
   STATE
========================= */
let contactMap = {};
let callTasksMap = {};

const callMap = {};
const missedTaskInFlight = new Set();

const botPollingState = {
  call: {
    offset: 0,
    processedUpdateIds: new Set(),
  },
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
   HELPERS
========================= */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

  if (!LDAP_URL) missing.push("LDAP_URL");
  if (!LDAP_BIND_DN) missing.push("LDAP_BIND_DN");
  if (!LDAP_PASSWORD) missing.push("LDAP_PASSWORD");
  if (!LDAP_BASE_DN) missing.push("LDAP_BASE_DN");

  if (missing.length) {
    log("Eksik ENV değişkenleri:", missing.join(", "));
  }

  if (ENABLE_SELF_PING && !APP_BASE_URL) {
    log("Uyarı: ENABLE_SELF_PING=true ama APP_BASE_URL boş.");
  }
}

function formatDateTimeTR(dateValue) {
  return new Date(dateValue).toLocaleString("tr-TR", {
    timeZone: TURKEY_TIMEZONE,
    hour12: false,
  });
}

function getDateKeyInTurkey(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: TURKEY_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);

  const get = (type) => parts.find((p) => p.type === type)?.value;
  return `${get("year")}-${get("month")}-${get("day")}`;
}

function getTaskKeyByPhoneAndDay(phone) {
  const normalizedPhone = normalizePhone(phone);
  const dateKey = getDateKeyInTurkey();
  return `${normalizedPhone}_${dateKey}`;
}

function getName(phone) {
  return contactMap[normalizePhone(phone)] || "Kayıtlı değil";
}

/* =========================
   LDAP CONTACTS
========================= */
async function loadContactsFromLDAP() {
  const client = new Client({
    url: LDAP_URL,
    timeout: 10000,
    connectTimeout: 10000,
  });

  const contacts = {};

  try {
    await client.bind(LDAP_BIND_DN, LDAP_PASSWORD);

    const { searchEntries } = await client.search(LDAP_BASE_DN, {
      scope: "sub",
      filter: LDAP_SEARCH_FILTER,
      attributes: ["*"],
    });

    for (const entry of searchEntries) {
      const rawName = entry[LDAP_NAME_ATTR];
      const rawPhone = entry[LDAP_PHONE_ATTR];

      const name = Array.isArray(rawName) ? String(rawName[0] || "").trim() : String(rawName || "").trim();
      const phoneValue = Array.isArray(rawPhone) ? String(rawPhone[0] || "") : String(rawPhone || "");
      const phone = normalizePhone(phoneValue);

      if (phone && name) {
        contacts[phone] = name;
      }
    }

    contactMap = contacts;
    log(`LDAP rehber yüklendi. Kayıt sayısı: ${Object.keys(contactMap).length}`);
  } finally {
    try {
      await client.unbind();
    } catch (_) {}
  }
}

async function refreshContactsSafe() {
  try {
    await loadContactsFromLDAP();
  } catch (e) {
    logError("LDAP rehber yükleme hatası:", e.message);
  }
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

function createOrUpdateDailyMissedCallTask(phone) {
  const taskId = getTaskKeyByPhoneAndDay(phone);
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
   TELEGRAM BOT PROCESS
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
        "/sync_calls -> açık görev kartlarını yeniden yollar\n" +
        "/reload_contacts -> LDAP rehberi yeniden çeker",
      [chatId]
    );
    return;
  }

  if (text === "/reload_contacts") {
    await refreshContactsSafe();
    await sendCallTelegram("✅ LDAP rehberi yeniden çekildi.", [chatId]);
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

  for (const update of updates) {
    if (state.processedUpdateIds.has(update.update_id)) continue;

    state.processedUpdateIds.add(update.update_id);

    if (state.processedUpdateIds.size > 1000) {
      const first = state.processedUpdateIds.values().next().value;
      state.processedUpdateIds.delete(first);
    }

    state.offset = update.update_id + 1;

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
  if (update.message) await processCallTelegramMessage(update);
  if (update.callback_query) await processCallTelegramCallback(update);
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
   HEALTH / SELF PING
========================= */
app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

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
   WEBHOOK
========================= */
app.all("/call-webhook-test", (req, res) => {
  log("========== SANTRAL WEBHOOK TEST ==========");
  log("METHOD:", req.method);
  log("QUERY:", req.query);
  log("BODY:", req.body);
  log("=========================================");
  res.status(200).json({ ok: true });
});

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

      return;
    }

    if (scenario === "Hangup") {
      const state = getCallState(uniqueId);
      const targetPhone = normalizePhone(state?.phone || phone);

      if (state?.type === "outbound") {
        deleteCallState(uniqueId);
        return;
      }

      if (state?.type === "inbound") {
        if (!state.answered) {
          const taskKey = getTaskKeyByPhoneAndDay(targetPhone);

          if (missedTaskInFlight.has(taskKey)) {
            log("Missed call task atlanıyor -> işlem zaten devam ediyor:", taskKey, targetPhone);
            deleteCallState(uniqueId);
            return;
          }

          missedTaskInFlight.add(taskKey);

          try {
            const { task, created } = createOrUpdateDailyMissedCallTask(targetPhone);
            await sendMissedCallTaskNotifications(task);

            if (created) {
              log("Inbound Hangup -> yeni missed call kartı oluşturuldu:", taskKey, targetPhone);
            } else {
              log("Inbound Hangup -> mevcut kart güncellendi:", taskKey, targetPhone, "count:", task.missedCount);
            }
          } finally {
            missedTaskInFlight.delete(taskKey);
          }
        }

        deleteCallState(uniqueId);
        return;
      }

      deleteCallState(uniqueId);
      return;
    }
  } catch (e) {
    logError("Çağrı webhook işleme hatası:", e.message);
  }
});

/* =========================
   GLOBAL ERROR
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
  loadCallTasks();
  await refreshContactsSafe();

  setInterval(cleanupStaleState, 10 * 60 * 1000);
  setInterval(refreshContactsSafe, LDAP_REFRESH_MS);

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

app.get("/ldap-test", async (req, res) => {
  try {
    const client = new Client({
      url: LDAP_URL,
      timeout: 10000,
      connectTimeout: 10000,
    });

    await client.bind(LDAP_BIND_DN, LDAP_PASSWORD);

    const baseDn = String(req.query.base || LDAP_BASE_DN || "").trim();
    const filter = String(req.query.filter || LDAP_SEARCH_FILTER || "(objectClass=*)").trim();
    const attrsRaw = String(req.query.attrs || "cn,telephonenumber,dn").trim();
    const sizeLimit = Math.min(Number(req.query.limit || 10), 50);

    const attributes = attrsRaw
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);

    const { searchEntries } = await client.search(baseDn, {
      scope: "sub",
      filter,
      attributes,
      sizeLimit,
    });

    await client.unbind();

    return res.status(200).json({
      ok: true,
      baseDn,
      filter,
      attributes,
      count: searchEntries.length,
      entries: searchEntries,
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      message: e.message,
      code: e.code || null,
      name: e.name || null,
    });
  }
});
