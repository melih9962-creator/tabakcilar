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
const CALL_STATE_TTL_MS = Number(process.env.CALL_STATE_TTL_MS || 6 * 60 * 60 * 1000);

/* =========================
   STATE
========================= */
let contactMap = {};
let callStore = {};
let dailyCardRefs = {};

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

function sanitizeRecording(value) {
  const v = String(value || "").trim();
  if (!v) return "";
  return v;
}

function truncateTelegramText(text, max = 3900) {
  if (text.length <= max) return text;
  return `${text.slice(0, max - 40)}\n\n... liste kısaltıldı ...`;
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
    turkeyTimeCache.source = "web";

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

      const name = Array.isArray(rawName) ? String(rawName[0] || "").trim() : String(rawName || "").trim();
      const phoneValue = Array.isArray(rawPhone) ? String(rawPhone[0] || "") : String(rawPhone || "");
      const phone = normalizePhone(phoneValue);

      if (!phone || !name) continue;

      // Dahilileri dışla; dış rehber yoksa CSV fallback devreye girsin
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
      log("LDAP boş döndü, CSV fallback denenecek.");
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
   CALL STORE
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

async function sendTelegramMessageViaBot(botToken, chatId, text, replyMarkup = null) {
  const payload = {
    chat_id: String(chatId),
    text: String(text || ""),
    disable_web_page_preview: true,
  };

  if (replyMarkup) payload.reply_markup = replyMarkup;

  const data = await telegramRequest(botToken, "sendMessage", payload);
  return data.result || null;
}

async function editTelegramMessageViaBot(botToken, chatId, messageId, text, replyMarkup = null) {
  const payload = {
    chat_id: String(chatId),
    message_id: Number(messageId),
    text: String(text || ""),
    disable_web_page_preview: true,
  };

  if (replyMarkup) payload.reply_markup = replyMarkup;

  const data = await telegramRequest(botToken, "editMessageText", payload);
  return data.result || null;
}

async function sendCallTelegram(text, targetChatIds = null) {
  const chatIds = targetChatIds || CALL_TELEGRAM_CHAT_IDS;
  if (!CALL_TELEGRAM_BOT_TOKEN || !chatIds.length) {
    log("Call bot aktif değil. Mesaj atlanıyor.");
    return;
  }

  for (const chatId of chatIds) {
    try {
      await sendTelegramMessageViaBot(CALL_TELEGRAM_BOT_TOKEN, chatId, text);
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
    `${index + 1}) ${getDirectionLabel(call.direction)} | ${getCallStatusLabel(call)} | ${formatOnlyTimeTR(call.startAt)}`,
    `👤 ${call.contactName || "Kayıtlı değil"}`,
    `📞 Arayan: ${call.caller || "-"}`,
    `📲 Aranan: ${call.callee || "-"}`,
    `🏢 Dahili: ${call.internal || "-"}`,
    `⏱ Süre: ${formatDuration(call.talkTime)}`,
  ];

  if (call.holdTime) {
    lines.push(`⏸ Bekleme: ${formatDuration(call.holdTime)}`);
  }

  if (call.recording) {
    lines.push(`🎧 Kayıt: ${call.recording}`);
  } else {
    lines.push(`🎧 Kayıt: yok`);
  }

  return lines.join("\n");
}

function summarizeDay(day) {
  const calls = day.calls || [];

  const inboundAnswered = calls.filter((x) => x.direction === "inbound" && x.answered).length;
  const inboundMissed = calls.filter((x) => x.direction === "inbound" && !x.answered).length;
  const outboundAnswered = calls.filter((x) => x.direction === "outbound" && x.answered).length;
  const outboundNoAnswer = calls.filter((x) => x.direction === "outbound" && !x.answered).length;
  const withRecording = calls.filter((x) => x.recording).length;

  return {
    total: calls.length,
    inboundAnswered,
    inboundMissed,
    outboundAnswered,
    outboundNoAnswer,
    withRecording,
  };
}

function buildDailyLiveCardText(dateKey, day) {
