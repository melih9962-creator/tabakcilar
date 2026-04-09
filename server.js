try {
  require("dotenv").config();
} catch (_) {}

const express = require("express");
const fs = require("fs");
const path = require("path");
const util = require("util");
const { Client } = require("ldapts");

const app = express();
app.use(express.json({ limit: "5mb" }));
app.use(express.urlencoded({ extended: true }));

/* =========================
   ENV
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

const REPORT_TIMEZONE = "Europe/Istanbul";
const REPORT_HOUR = Number(process.env.REPORT_HOUR || 20);
const REPORT_MINUTE = Number(process.env.REPORT_MINUTE || 0);

const TURKEY_TIME_API_URL =
  process.env.TURKEY_TIME_API_URL ||
  "https://www.timeapi.io/api/Time/current/zone?timeZone=Europe/Istanbul";

const LDAP_URL = process.env.LDAP_URL || "";
const LDAP_BIND_DN = process.env.LDAP_BIND_DN || "";
const LDAP_PASSWORD = process.env.LDAP_PASSWORD || "";
const LDAP_BASE_DN = process.env.LDAP_BASE_DN || "";
const LDAP_SEARCH_FILTER = process.env.LDAP_SEARCH_FILTER || "(objectClass=*)";
const LDAP_NAME_ATTR = process.env.LDAP_NAME_ATTR || "cn";
const LDAP_PHONE_ATTR = process.env.LDAP_PHONE_ATTR || "telephonenumber";
const LDAP_REFRESH_MS = Number(process.env.LDAP_REFRESH_MS || 5 * 60 * 1000);

const CONTACTS_FILE = path.join(__dirname, process.env.CONTACTS_FILE || "rehber.csv");
const CALLS_FILE = path.join(__dirname, process.env.CALLS_FILE || "daily_calls.json");
const DAILY_CARD_FILE = path.join(__dirname, process.env.DAILY_CARD_FILE || "daily_card_refs.json");
const CALL_TASKS_FILE = path.join(__dirname, process.env.CALL_TASKS_FILE || "call_tasks.json");

const CALL_STATE_TTL_MS = Number(process.env.CALL_STATE_TTL_MS || 6 * 60 * 60 * 1000);

/* =========================
   STATE
========================= */
let contactMap = {};
let callStore = {};
let dailyCardRefs = {};
let callTasksMap = {};

const callMap = {};
const finalizedCallLocks = new Set();

const botPollingState = {
  call: {
    offset: 0,
    processedUpdateIds: new Set(),
  },
};

let turkeyTimeCache = {
  lastOkMs: 0,
  offsetMs: 0,
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

function pad2(v) {
  return String(v).padStart(2, "0");
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

function isMeaningfulPhone(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits.length >= 7;
}

function cleanDisplayTarget(value, fallback = "-") {
  const raw = String(value || "").trim();
  if (!raw) return fallback;
  if (isMeaningfulPhone(raw)) return normalizePhone(raw);
  return raw;
}

function sanitizeRecording(value) {
  const v = String(value || "").trim();
  if (!v) return "";
  return v;
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

function isAllowedCallTelegramChat(chatId) {
  return CALL_TELEGRAM_CHAT_IDS.includes(String(chatId));
}

function formatDuration(seconds) {
  const s = Number(seconds || 0);
  if (!s) return "0 sn";

  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;

  const parts = [];
  if (h) parts.push(`${h} sa`);
  if (m) parts.push(`${m} dk`);
  if (sec || !parts.length) parts.push(`${sec} sn`);
  return parts.join(" ");
}

function truncateText(text, max = 3900) {
  if (text.length <= max) return text;
  return `${text.slice(0, max - 24)}\n\n... kısaltıldı ...`;
}

function escapeHtml(input) {
  return String(input ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function inferDirection(raw = {}) {
  const scenario = String(raw.scenario || "");
  const yon = String(raw.yon || "");

  if (scenario === "Outbound_call") return "outbound";
  if (scenario === "Inbound_call" || scenario === "InboundtoPBX" || scenario === "Queue") return "inbound";
  if (yon === "2") return "outbound";
  if (yon === "1") return "inbound";
  return "unknown";
}

function formatTelegramUser(user) {
  if (!user) return "Bilinmiyor";
  const fullName = [user.first_name, user.last_name].filter(Boolean).join(" ").trim();
  if (fullName) return fullName;
  if (user.username) return `@${user.username}`;
  return String(user.id || "Bilinmiyor");
}

/* =========================
   TURKEY TIME
========================= */
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

    if (!year || !month || !day) {
      throw new Error(`invalid time payload: ${JSON.stringify(data)}`);
    }

    const turkeyUtcMs = Date.UTC(year, month - 1, day, hour, minute, second);
    turkeyTimeCache.offsetMs = turkeyUtcMs - Date.now();
    turkeyTimeCache.lastOkMs = Date.now();

    log("Türkiye saati webden güncellendi.");
    return true;
  } catch (e) {
    logError("Türkiye saati webden alınamadı:", e.message);
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

  return new Date();
}

async function getDateKeyInTurkey() {
  const d = await getTurkeyNowDate();
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

async function getDateKeyDaysAgoTurkey(days = 0) {
  const d = await getTurkeyNowDate();
  const shifted = new Date(d.getTime() - days * 24 * 60 * 60 * 1000);
  return `${shifted.getUTCFullYear()}-${pad2(shifted.getUTCMonth() + 1)}-${pad2(shifted.getUTCDate())}`;
}

function formatDateTimeTR(dateValue) {
  return new Date(dateValue).toLocaleString("tr-TR", {
    timeZone: REPORT_TIMEZONE,
    hour12: false,
  });
}

function formatOnlyTimeTR(dateValue) {
  return new Date(dateValue).toLocaleTimeString("tr-TR", {
    timeZone: REPORT_TIMEZONE,
    hour12: false,
  });
}

/* =========================
   CONTACTS
========================= */
function loadContactsFromCsv() {
  try {
    if (!fs.existsSync(CONTACTS_FILE)) {
      contactMap = {};
      log("CSV rehber yok:", CONTACTS_FILE);
      return;
    }

    const raw = fs.readFileSync(CONTACTS_FILE, "utf8");
    const lines = raw
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    if (lines.length < 2) {
      contactMap = {};
      log("CSV rehber boş.");
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
      contactMap = {};
      log("CSV rehber kolonları uygun değil.");
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
    log(`CSV rehber yüklendi. Kayıt sayısı: ${Object.keys(contactMap).length}`);
  } catch (e) {
    contactMap = {};
    logError("CSV rehber yükleme hatası:", e.message);
  }
}

async function loadContactsFromLDAP() {
  if (!LDAP_URL || !LDAP_BIND_DN || !LDAP_PASSWORD || !LDAP_BASE_DN) {
    throw new Error("LDAP env eksik");
  }

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
      attributes: [LDAP_NAME_ATTR, LDAP_PHONE_ATTR],
    });

    for (const entry of searchEntries) {
      const rawName = entry[LDAP_NAME_ATTR];
      const rawPhone = entry[LDAP_PHONE_ATTR];

      const name = Array.isArray(rawName)
        ? String(rawName[0] || "").trim()
        : String(rawName || "").trim();

      const phoneValue = Array.isArray(rawPhone)
        ? String(rawPhone[0] || "")
        : String(rawPhone || "");

      const phone = normalizePhone(phoneValue);

      if (!phone || !name) continue;

      // Dahili numaraları dışla
      if (/^\d{2,4}$/.test(phone)) continue;

      contacts[phone] = name;
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
  if (LDAP_URL && LDAP_BIND_DN && LDAP_PASSWORD && LDAP_BASE_DN) {
    try {
      await loadContactsFromLDAP();
      if (Object.keys(contactMap).length > 0) return;
      log("LDAP boş döndü, CSV fallback kullanılacak.");
    } catch (e) {
      logError("LDAP rehber yükleme hatası:", e.message);
    }
  }

  loadContactsFromCsv();
}

function getName(phone) {
  return contactMap[normalizePhone(phone)] || "Kayıtlı değil";
}

/* =========================
   STORE
========================= */
function loadCallStore() {
  callStore = safeJsonRead(CALLS_FILE, {});
  log(`Günlük çağrı store yüklendi. Gün sayısı: ${Object.keys(callStore).length}`);
}

function saveCallStore() {
  safeJsonWrite(CALLS_FILE, callStore);
}

function ensureDayStore(dateKey) {
  if (!callStore[dateKey]) {
    callStore[dateKey] = {
      calls: [],
      finalReportSent: false,
      finalReportSentAt: null,
    };
  }
  return callStore[dateKey];
}

function loadDailyCardRefs() {
  dailyCardRefs = safeJsonRead(DAILY_CARD_FILE, {});
  log(`Günlük kart referansları yüklendi. Gün sayısı: ${Object.keys(dailyCardRefs).length}`);
}

function saveDailyCardRefs() {
  safeJsonWrite(DAILY_CARD_FILE, dailyCardRefs);
}

function ensureDailyCardRef(dateKey) {
  if (!dailyCardRefs[dateKey]) {
    dailyCardRefs[dateKey] = {};
  }
  return dailyCardRefs[dateKey];
}

function loadCallTasks() {
  callTasksMap = safeJsonRead(CALL_TASKS_FILE, {});
  log(`Çağrı görevleri yüklendi. Kayıt sayısı: ${Object.keys(callTasksMap).length}`);
}

function saveCallTasks() {
  safeJsonWrite(CALL_TASKS_FILE, callTasksMap);
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
    finalized: false,
    recording: "",
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

async function sendTelegramMessageViaBot(botToken, chatId, text, replyMarkup = null, parseMode = "HTML") {
  const payload = {
    chat_id: String(chatId),
    text: String(text || ""),
    disable_web_page_preview: true,
    parse_mode: parseMode,
  };

  if (replyMarkup) payload.reply_markup = replyMarkup;

  const data = await telegramRequest(botToken, "sendMessage", payload);
  return data.result || null;
}

async function editTelegramMessageViaBot(botToken, chatId, messageId, text, replyMarkup = null, parseMode = "HTML") {
  const payload = {
    chat_id: String(chatId),
    message_id: Number(messageId),
    text: String(text || ""),
    disable_web_page_preview: true,
    parse_mode: parseMode,
  };

  if (replyMarkup) payload.reply_markup = replyMarkup;

  const data = await telegramRequest(botToken, "editMessageText", payload);
  return data.result || null;
}

async function sendCallTelegram(text, targetChatIds = null, replyMarkup = null, parseMode = "HTML") {
  const chatIds = targetChatIds || CALL_TELEGRAM_CHAT_IDS;

  if (!CALL_TELEGRAM_BOT_TOKEN || !chatIds.length) {
    log("Call bot aktif değil. Mesaj atlanıyor.");
    return;
  }

  for (const chatId of chatIds) {
    try {
      await sendTelegramMessageViaBot(CALL_TELEGRAM_BOT_TOKEN, chatId, text, replyMarkup, parseMode);
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

async function clearTelegramWebhook() {
  if (!CALL_TELEGRAM_BOT_TOKEN) return;

  try {
    await telegramRequest(CALL_TELEGRAM_BOT_TOKEN, "deleteWebhook", {
      drop_pending_updates: false,
    });
    log("Telegram webhook temizlendi.");
  } catch (e) {
    logError("Telegram deleteWebhook hatası:", e.message);
  }
}

/* =========================
   RECORDING LINKS
========================= */
function buildRecordingListenUrl(callUniqueId) {
  if (!APP_BASE_URL || !callUniqueId) return "";
  return `${APP_BASE_URL}/recording/${encodeURIComponent(callUniqueId)}`;
}

function buildRecordingHtml(callUniqueId) {
  const url = buildRecordingListenUrl(callUniqueId);
  if (!url) return "🎧 Kayıt: mevcut";
  return `🎧 <a href="${escapeHtml(url)}">Dinle</a>`;
}

/* =========================
   MISSED CALL TASKS
========================= */
async function getTaskKeyByPhoneAndDay(phone) {
  const normalizedPhone = normalizePhone(phone);
  const dateKey = await getDateKeyInTurkey();
  return `${normalizedPhone}_${dateKey}`;
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

function buildMissedCallTaskText(task) {
  const lines = [
    "📞 <b>Kaçan çağrı</b>",
    "",
    `Durum: ${escapeHtml(getTaskStatusLabel(task.status))}`,
    `👤 ${escapeHtml(task.name || "Kayıtlı değil")}`,
    `📱 ${escapeHtml(task.phone || "-")}`,
    `🔁 Bugün ulaşamama sayısı: ${escapeHtml(String(task.missedCount || 1))}`,
    `🕒 İlk çağrı: ${escapeHtml(formatDateTimeTR(task.createdAt))}`,
    `⏱ Son arama: ${escapeHtml(formatDateTimeTR(task.lastCallAt || task.createdAt))}`,
  ];

  if (task.lastRecordingUniqueId) {
    lines.push(buildRecordingHtml(task.lastRecordingUniqueId));
  } else {
    lines.push("🎧 Kayıt: yok");
  }

  if (task.updatedBy) {
    lines.push(`🙋 Güncelleyen: ${escapeHtml(task.updatedBy)}`);
  }

  if (task.updatedAt && task.updatedAt !== task.createdAt) {
    lines.push(`📝 Son Güncelleme: ${escapeHtml(formatDateTimeTR(task.updatedAt))}`);
  }

  return lines.join("\n");
}

function buildMissedCallTaskKeyboard(task) {
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

async function createOrUpdateDailyMissedCallTask(phone, sourceUniqueId, extra = {}) {
  const taskId = await getTaskKeyByPhoneAndDay(phone);
  const normalizedPhone = normalizePhone(phone);

  if (callTasksMap[taskId]) {
    const existing = callTasksMap[taskId];
    existing.sourceUniqueIds = Array.isArray(existing.sourceUniqueIds) ? existing.sourceUniqueIds : [];

    const alreadySeen = sourceUniqueId && existing.sourceUniqueIds.includes(sourceUniqueId);
    if (!alreadySeen) {
      existing.missedCount = (existing.missedCount || 1) + 1;
      if (sourceUniqueId) existing.sourceUniqueIds.push(sourceUniqueId);
    }

    existing.lastCallAt = Date.now();
    existing.updatedAt = Date.now();
    existing.name = getName(normalizedPhone);

    if (extra.lastRecordingUniqueId) {
      existing.lastRecordingUniqueId = extra.lastRecordingUniqueId;
    }

    saveCallTasks();
    return { task: existing, created: !alreadySeen && (existing.missedCount || 1) === 1, changed: !alreadySeen };
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
    lastRecordingUniqueId: extra.lastRecordingUniqueId || "",
    sourceUniqueIds: sourceUniqueId ? [sourceUniqueId] : [],
  };

  callTasksMap[taskId] = task;
  saveCallTasks();
  return { task, created: true, changed: true };
}

async function sendMissedCallTaskNotifications(task) {
  if (!CALL_TELEGRAM_BOT_TOKEN || !CALL_TELEGRAM_CHAT_IDS.length) {
    log("Call bot pasif; missed call görevi gönderilmedi.");
    return;
  }

  const text = buildMissedCallTaskText(task);
  const keyboard = buildMissedCallTaskKeyboard(task);

  if (!task.messageRefs?.length) {
    for (const chatId of CALL_TELEGRAM_CHAT_IDS) {
      try {
        const result = await sendTelegramMessageViaBot(
          CALL_TELEGRAM_BOT_TOKEN,
          chatId,
          text,
          keyboard,
          "HTML"
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

  await syncMissedCallTaskMessages(task);
}

async function syncMissedCallTaskMessages(task) {
  if (!task?.messageRefs?.length) {
    saveCallTasks();
    return;
  }

  const text = buildMissedCallTaskText(task);
  const keyboard = buildMissedCallTaskKeyboard(task);

  for (const ref of task.messageRefs) {
    try {
      await editTelegramMessageViaBot(
        CALL_TELEGRAM_BOT_TOKEN,
        ref.chatId,
        ref.messageId,
        text,
        keyboard,
        "HTML"
      );
    } catch (e) {
      logError(
        `Missed call task mesaj güncelleme hatası -> chat_id=${ref.chatId}, message_id=${ref.messageId}:`,
        e.message
      );
    }
  }

  saveCallTasks();
}

/* =========================
   CARD BUILDERS
========================= */
function getCallStatusLabel(call) {
  if (call.direction === "inbound" && !call.answered) return "❌ Kaçan";
  if (call.direction === "outbound" && !call.answered) return "📴 Cevapsız";
  if (call.answered) return "✅ Görüşüldü";
  return "ℹ️ Bilinmiyor";
}

function getDirectionLabel(direction) {
  if (direction === "inbound") return "📥 Inbound";
  if (direction === "outbound") return "📤 Outbound";
  return "↔️ Unknown";
}

function buildCallLine(call, index) {
  const lines = [
    `${index + 1}) ${escapeHtml(getDirectionLabel(call.direction))} | ${escapeHtml(getCallStatusLabel(call))} | ${escapeHtml(formatOnlyTimeTR(call.startAt))}`,
    `👤 ${escapeHtml(call.contactName || "Kayıtlı değil")}`,
    `📞 Arayan: ${escapeHtml(call.caller || "-")}`,
    `📲 Aranan: ${escapeHtml(call.callee || "-")}`,
    `🏢 Dahili: ${escapeHtml(call.internal || "-")}`,
    `⏱ Süre: ${escapeHtml(formatDuration(call.talkTime))}`,
  ];

  if (call.holdTime) {
    lines.push(`⏸ Bekleme: ${escapeHtml(formatDuration(call.holdTime))}`);
  }

  if (call.recording) {
    lines.push(buildRecordingHtml(call.uniqueId));
  } else {
    lines.push("🎧 Kayıt: yok");
  }

  return lines.join("\n");
}

function summarizeDay(day) {
  const calls = day.calls || [];

  return {
    total: calls.length,
    inboundAnswered: calls.filter((x) => x.direction === "inbound" && x.answered).length,
    inboundMissed: calls.filter((x) => x.direction === "inbound" && !x.answered).length,
    outboundAnswered: calls.filter((x) => x.direction === "outbound" && x.answered).length,
    outboundNoAnswer: calls.filter((x) => x.direction === "outbound" && !x.answered).length,
    withRecording: calls.filter((x) => x.recording).length,
  };
}

function buildDailyLiveCardText(dateKey, day) {
  const summary = summarizeDay(day);
  const calls = [...(day.calls || [])].sort((a, b) => a.startAt - b.startAt);

  const lines = [
    `📋 <b>Günlük Çağrı Kartı</b>`,
    `📅 Tarih: ${escapeHtml(dateKey)}`,
    ``,
    `Toplam: ${escapeHtml(String(summary.total))}`,
    `📥 Inbound cevaplanan: ${escapeHtml(String(summary.inboundAnswered))}`,
    `📥 Inbound kaçan: ${escapeHtml(String(summary.inboundMissed))}`,
    `📤 Outbound görüşülen: ${escapeHtml(String(summary.outboundAnswered))}`,
    `📤 Outbound cevapsız: ${escapeHtml(String(summary.outboundNoAnswer))}`,
    `🎧 Ses kaydı olan: ${escapeHtml(String(summary.withRecording))}`,
    ``,
  ];

  if (!calls.length) {
    lines.push("Bugün henüz çağrı yok.");
  } else {
    calls.forEach((call, i) => {
      lines.push(buildCallLine(call, i));
      lines.push("");
    });
  }

  return truncateText(lines.join("\n"));
}

function buildFinalReportText(dateKey, day) {
  const summary = summarizeDay(day);
  const calls = [...(day.calls || [])].sort((a, b) => a.startAt - b.startAt);

  const lines = [
    `📊 <b>Gün Sonu Çağrı Raporu</b>`,
    `📅 Tarih: ${escapeHtml(dateKey)}`,
    ``,
    `Toplam görüşme: ${escapeHtml(String(summary.total))}`,
    `📥 Inbound cevaplanan: ${escapeHtml(String(summary.inboundAnswered))}`,
    `📥 Inbound kaçan: ${escapeHtml(String(summary.inboundMissed))}`,
    `📤 Outbound görüşülen: ${escapeHtml(String(summary.outboundAnswered))}`,
    `📤 Outbound cevapsız: ${escapeHtml(String(summary.outboundNoAnswer))}`,
    `🎧 Ses kaydı olan: ${escapeHtml(String(summary.withRecording))}`,
    ``,
    `Detaylar:`,
    ``,
  ];

  if (!calls.length) {
    lines.push("Bugün çağrı kaydı yok.");
  } else {
    calls.forEach((call, i) => {
      lines.push(buildCallLine(call, i));
      lines.push("");
    });
  }

  return truncateText(lines.join("\n"));
}

/* =========================
   DAILY CARD SEND/UPDATE
========================= */
async function sendOrUpdateDailyLiveCard(dateKey) {
  const day = ensureDayStore(dateKey);
  const text = buildDailyLiveCardText(dateKey, day);
  const dayRefs = ensureDailyCardRef(dateKey);

  for (const chatId of CALL_TELEGRAM_CHAT_IDS) {
    const ref = dayRefs[String(chatId)];

    try {
      if (ref?.messageId) {
        await editTelegramMessageViaBot(
          CALL_TELEGRAM_BOT_TOKEN,
          chatId,
          ref.messageId,
          text,
          null,
          "HTML"
        );
      } else {
        const sent = await sendTelegramMessageViaBot(
          CALL_TELEGRAM_BOT_TOKEN,
          chatId,
          text,
          null,
          "HTML"
        );

        if (sent?.message_id) {
          dayRefs[String(chatId)] = {
            messageId: sent.message_id,
            updatedAt: Date.now(),
          };
        }
      }
    } catch (e) {
      logError(`Günlük kart gönder/güncelle hatası -> chat_id=${chatId}:`, e.message);

      try {
        const sent = await sendTelegramMessageViaBot(
          CALL_TELEGRAM_BOT_TOKEN,
          chatId,
          text,
          null,
          "HTML"
        );

        if (sent?.message_id) {
          dayRefs[String(chatId)] = {
            messageId: sent.message_id,
            updatedAt: Date.now(),
          };
        }
      } catch (inner) {
        logError(`Fallback günlük kart gönderim hatası -> chat_id=${chatId}:`, inner.message);
      }
    }
  }

  saveDailyCardRefs();
}

async function sendFinalReportIfDue() {
  const now = await getTurkeyNowDate();
  const hour = now.getUTCHours();
  const minute = now.getUTCMinutes();

  if (hour !== REPORT_HOUR || minute !== REPORT_MINUTE) {
    return;
  }

  const dateKey = await getDateKeyInTurkey();
  const day = ensureDayStore(dateKey);

  if (day.finalReportSent) {
    return;
  }

  const text = buildFinalReportText(dateKey, day);
  await sendCallTelegram(text, null, null, "HTML");

  day.finalReportSent = true;
  day.finalReportSentAt = Date.now();
  saveCallStore();

  log("Gün sonu raporu gönderildi:", dateKey);
}

function startDailyReportScheduler() {
  setInterval(async () => {
    try {
      await sendFinalReportIfDue();
    } catch (e) {
      logError("Gün sonu rapor scheduler hatası:", e.message);
    }
  }, 30 * 1000);

  log(`Gün sonu rapor scheduler aktif: ${REPORT_HOUR}:${pad2(REPORT_MINUTE)} (${REPORT_TIMEZONE})`);
}

/* =========================
   FINALIZE CALL
========================= */
function dedupeExistingCall(day, key) {
  return (day.calls || []).find((x) => x.key === key);
}

async function finalizeCallRecord(source, patch = {}) {
  const uniqueId = String(
    patch.uniqueId ||
      source.uniqueId ||
      source.asteriskId ||
      source.unique_id ||
      ""
  ).trim();

  if (!uniqueId) return;
  if (finalizedCallLocks.has(uniqueId)) return;

  finalizedCallLocks.add(uniqueId);

  try {
    const state = getCallState(uniqueId) || {};
    if (state.finalized) return;

    const direction = patch.direction || state.type || inferDirection(source);

    const callerRaw = String(
      patch.caller ||
        source.caller ||
        source.arayan ||
        (direction === "inbound" ? source.customer_num : source.internal_num) ||
        ""
    ).trim();

    const calleeRaw = String(
      patch.callee ||
        source.callee ||
        source.aranan ||
        source.customer_num ||
        source.incoming_number ||
        source.santral ||
        ""
    ).trim();

    const caller = isMeaningfulPhone(callerRaw) ? normalizePhone(callerRaw) : callerRaw || "-";
    const callee = cleanDisplayTarget(calleeRaw, "-");

    const internal = String(
      patch.internal ||
        source.internal_num ||
        state.internal ||
        ""
    ).trim() || "-";

    const talkTime = Number(patch.talkTime ?? source.talktime ?? source.sure ?? 0);
    const holdTime = Number(patch.holdTime ?? source.holdtime ?? 0);

    const answered =
      typeof patch.answered === "boolean"
        ? patch.answered
        : Boolean(state.answered || talkTime > 0 || String(source.callConnectedTime || "") !== "");

    const startAt = Number(
      patch.startAt ||
        source.callInitiatedTime ||
        state.createdAt ||
        Date.now()
    );

    const endAt = Number(
      patch.endAt ||
        source.callEndedTime ||
        source.timestamp ||
        Date.now()
    );

    const recording = sanitizeRecording(
      patch.recording ||
        source.seskaydi ||
        state.recording ||
        ""
    );

    let externalTarget = "";
    if (direction === "inbound") {
      externalTarget = normalizePhone(source.customer_num || source.arayan || callerRaw || "");
    } else if (direction === "outbound") {
      externalTarget = normalizePhone(source.customer_num || source.aranan || calleeRaw || "");
    }

    const contactName = getName(externalTarget);

    const dateKey = await getDateKeyInTurkey();
    const day = ensureDayStore(dateKey);

    const record = {
      key: uniqueId,
      uniqueId,
      direction,
      answered,
      caller,
      callee,
      internal,
      talkTime,
      holdTime,
      startAt,
      endAt,
      recording,
      contactName,
      rawScenario: String(source.scenario || patch.rawScenario || ""),
      updatedAt: Date.now(),
    };

    const existing = dedupeExistingCall(day, uniqueId);
    if (existing) {
      Object.assign(existing, record);
    } else {
      day.calls.push(record);
    }

    saveCallStore();

    upsertCallState(uniqueId, {
      ...state,
      finalized: true,
      recording,
    });

    await sendOrUpdateDailyLiveCard(dateKey);

    log("Çağrı finalize edildi:", {
      uniqueId,
      direction,
      answered,
      caller: record.caller,
      callee: record.callee,
      internal: record.internal,
      recording: Boolean(recording),
    });
  } catch (e) {
    logError("finalizeCallRecord hatası:", e.message);
  } finally {
    finalizedCallLocks.delete(uniqueId);
  }
}

/* =========================
   TELEGRAM COMMANDS
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
        "/today_card\n" +
        "/today_report\n" +
        "/yesterday_report\n" +
        "/reload_contacts\n" +
        "/open_calls\n\n" +
        "/today_card -> bugünün canlı kartı\n" +
        "/today_report -> bugünün raporu\n" +
        "/yesterday_report -> dünün raporu\n" +
        "/reload_contacts -> rehberi yeniden yükler\n" +
        "/open_calls -> açık kaçan çağrı görevleri",
      [chatId],
      null,
      "HTML"
    );
    return;
  }

  if (text === "/reload_contacts") {
    await refreshContactsSafe();
    await sendCallTelegram("✅ Rehber yeniden yüklendi.", [chatId], null, "HTML");
    return;
  }

  if (text === "/today_card") {
    const dateKey = await getDateKeyInTurkey();
    const day = ensureDayStore(dateKey);
    await sendCallTelegram(buildDailyLiveCardText(dateKey, day), [chatId], null, "HTML");
    return;
  }

  if (text === "/today_report") {
    const dateKey = await getDateKeyInTurkey();
    const day = ensureDayStore(dateKey);
    await sendCallTelegram(buildFinalReportText(dateKey, day), [chatId], null, "HTML");
    return;
  }

  if (text === "/yesterday_report") {
    const dateKey = await getDateKeyDaysAgoTurkey(1);
    const day = ensureDayStore(dateKey);
    await sendCallTelegram(buildFinalReportText(dateKey, day), [chatId], null, "HTML");
    return;
  }

  if (text === "/open_calls") {
    const openTasks = Object.values(callTasksMap)
      .filter((t) => t.status === "open")
      .sort((a, b) => b.lastCallAt - a.lastCallAt)
      .slice(0, 30);

    if (!openTasks.length) {
      await sendCallTelegram("Açık kaçan çağrı görevi yok.", [chatId], null, "HTML");
      return;
    }

    const lines = ["📋 <b>Açık kaçan çağrı görevleri</b>", ""];
    for (const t of openTasks) {
      lines.push(`• ${escapeHtml(t.name || "Kayıtlı değil")} - ${escapeHtml(t.phone || "-")} (${escapeHtml(String(t.missedCount || 1))})`);
    }

    await sendCallTelegram(lines.join("\n"), [chatId], null, "HTML");
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

  if (parts.length === 3 && parts[0] === "calltask") {
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
    await syncMissedCallTaskMessages(task);
    await answerCallCallback(callback.id, `Durum güncellendi: ${getTaskStatusLabel(newStatus)}`);
    return;
  }

  await answerCallCallback(callback.id);
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
    if (data.error_code === 409) {
      logError(`${botName} update hatası: aynı bot başka instance tarafından kullanılıyor.`);
      return;
    }
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
   RECORDING PROXY
========================= */
app.get("/recording/:callId", async (req, res) => {
  try {
    const callId = String(req.params.callId || "").trim();
    if (!callId) {
      return res.status(400).send("missing call id");
    }

    let found = null;
    for (const day of Object.values(callStore)) {
      const match = (day.calls || []).find((c) => c.uniqueId === callId && c.recording);
      if (match) {
        found = match;
        break;
      }
    }

    if (!found || !found.recording) {
      return res.status(404).send("recording not found");
    }

    const upstream = await fetch(found.recording, {
      method: "GET",
      redirect: "follow",
    });

    if (!upstream.ok) {
      return res.status(502).send("upstream recording fetch failed");
    }

    const contentType = upstream.headers.get("content-type") || "audio/mpeg";
    res.setHeader("Content-Type", contentType);
    res.setHeader("Content-Disposition", 'inline; filename="recording.mp3"');
    res.setHeader("Cache-Control", "private, max-age=300");

    const arrayBuffer = await upstream.arrayBuffer();
    return res.status(200).send(Buffer.from(arrayBuffer));
  } catch (e) {
    logError("recording proxy hatası:", e.message);
    return res.status(500).send("recording proxy error");
  }
});

/* =========================
   DEBUG
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
   WEBHOOK
========================= */
app.post("/call-webhook", async (req, res) => {
  res.sendStatus(200);

  try {
    const data = req.body || {};
    const scenario = String(data.scenario || "").trim();
    const uniqueId = String(data.unique_id || data.asteriskId || "").trim();

    log("Çağrı webhook event:", data);

    if (!scenario || !uniqueId) {
      log("Çağrı webhook atlandı: scenario veya unique_id eksik.");
      return;
    }

    if (scenario === "Outbound_call") {
      upsertCallState(uniqueId, {
        type: "outbound",
        phone: normalizePhone(data.customer_num || data.aranan || ""),
        internal: String(data.internal_num || "").trim(),
        answered: false,
        recording: "",
      });
      return;
    }

    if (scenario === "InboundtoPBX" || scenario === "Inbound_call" || scenario === "Queue") {
      upsertCallState(uniqueId, {
        type: "inbound",
        phone: normalizePhone(data.customer_num || data.aranan || data.incoming_number || ""),
        internal: String(data.internal_num || "").trim(),
        answered: false,
        recording: "",
      });
      return;
    }

    if (scenario === "Answer") {
      const current = getCallState(uniqueId);

      upsertCallState(uniqueId, {
        type: current?.type || inferDirection(data),
        phone: current?.phone || normalizePhone(data.customer_num || data.aranan || ""),
        internal: current?.internal || String(data.internal_num || "").trim(),
        answered: true,
      });
      return;
    }

    if (scenario === "Hangup") {
      const state = getCallState(uniqueId);
      const direction = state?.type || inferDirection(data);
      const answered = Boolean(state?.answered || Number(data.talktime || 0) > 0);

      await finalizeCallRecord(
        {
          ...data,
          uniqueId,
        },
        {
          direction,
          answered,
          caller:
            direction === "inbound"
              ? (data.customer_num || data.arayan || "")
              : (data.internal_num || data.arayan || ""),
          callee:
            direction === "inbound"
              ? (data.incoming_number || data.santral || data.aranan || "")
              : (data.customer_num || data.aranan || ""),
          internal: data.internal_num || state?.internal || "",
          talkTime: Number(data.talktime || 0),
          holdTime: Number(data.holdtime || 0),
          endAt: Number(data.timestamp || Date.now()),
          rawScenario: "Hangup",
        }
      );

      if (direction === "inbound" && !answered) {
        const missedPhone = normalizePhone(data.customer_num || data.arayan || "");
        if (missedPhone) {
          const { task, changed } = await createOrUpdateDailyMissedCallTask(missedPhone, uniqueId, {
            lastRecordingUniqueId: uniqueId,
          });

          if (changed) {
            await sendMissedCallTaskNotifications(task);
          } else {
            await syncMissedCallTaskMessages(task);
          }
        }
      }

      deleteCallState(uniqueId);
      return;
    }

    if (scenario === "cdr") {
      const direction =
        String(data.yon || "") === "1"
          ? "inbound"
          : String(data.yon || "") === "2"
          ? "outbound"
          : inferDirection(data);

      const answered = Number(data.sure || 0) > 0 || String(data.callConnectedTime || "") !== "";

      const caller =
        direction === "inbound"
          ? normalizePhone(data.arayan || "") || String(data.arayan || "").trim()
          : normalizePhone(data.arayan || "") || String(data.arayan || "").trim();

      const callee =
        direction === "inbound"
          ? cleanDisplayTarget(data.santral || data.incoming_number || data.aranan || "")
          : cleanDisplayTarget(data.aranan || "");

      const internal =
        direction === "outbound"
          ? String(data.arayan || "").trim()
          : String(data.internal_num || "").trim();

      await finalizeCallRecord(
        {
          ...data,
          uniqueId,
        },
        {
          direction,
          answered,
          caller,
          callee,
          internal,
          talkTime: Number(data.sure || 0),
          holdTime: Number(data.holdtime || 0),
          startAt: Number(data.callInitiatedTime || Date.now()),
          endAt: Number(data.callEndedTime || Date.now()),
          recording: sanitizeRecording(data.seskaydi || ""),
          rawScenario: "cdr",
        }
      );

      if (direction === "inbound" && !answered) {
        const missedPhone = normalizePhone(data.arayan || data.customer_num || "");
        if (missedPhone) {
          const { task, changed } = await createOrUpdateDailyMissedCallTask(missedPhone, uniqueId, {
            lastRecordingUniqueId: sanitizeRecording(data.seskaydi || "") ? uniqueId : "",
          });

          if (changed) {
            await sendMissedCallTaskNotifications(task);
          } else {
            await syncMissedCallTaskMessages(task);
          }
        }
      }

      deleteCallState(uniqueId);
      return;
    }

    if (scenario === "QueueLeave") {
      log("QueueLeave event alındı:", data);
      return;
    }
  } catch (e) {
    logError("Çağrı webhook işleme hatası:", e.message);
  }
});

/* =========================
   ERROR CAPTURE
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

  loadCallStore();
  loadDailyCardRefs();
  loadCallTasks();

  await refreshTurkeyTimeOffset();
  await refreshContactsSafe();
  await clearTelegramWebhook();

  setInterval(cleanupStaleState, 10 * 60 * 1000);
  setInterval(refreshContactsSafe, LDAP_REFRESH_MS);
  setInterval(async () => {
    try {
      await refreshTurkeyTimeOffset();
    } catch (e) {
      logError("Türkiye saat refresh hatası:", e.message);
    }
  }, 5 * 60 * 1000);

  startDailyReportScheduler();

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
