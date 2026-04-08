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
const PORT = process.env.PORT || 3000;

/* Ana Telegram botu: WhatsApp operasyonu */
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_IDS = (process.env.TELEGRAM_CHAT_IDS || "")
  .split(",")
  .map((x) => x.trim())
  .filter(Boolean);

/* Ayrı çağrı bildirim botu */
const CALL_TELEGRAM_BOT_TOKEN = process.env.CALL_TELEGRAM_BOT_TOKEN || "";
const CALL_TELEGRAM_CHAT_IDS = (process.env.CALL_TELEGRAM_CHAT_IDS || "")
  .split(",")
  .map((x) => x.trim())
  .filter(Boolean);

/* Asistan bildirim botu */
const ASSISTANT_TELEGRAM_BOT_TOKEN = process.env.ASSISTANT_TELEGRAM_BOT_TOKEN || "";
const ASSISTANT_TELEGRAM_CHAT_IDS = (process.env.ASSISTANT_TELEGRAM_CHAT_IDS || "")
  .split(",")
  .map((x) => x.trim())
  .filter(Boolean);

const VSOFT_API_KEY = process.env.VSOFT_API_KEY || "";
const PHONE_NUMBER_ID = process.env.PHONE_NUMBER_ID || "";
const BUSINESS_DEVICE_ID = process.env.BUSINESS_DEVICE_ID || "";
const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "";

const ENABLE_SELF_PING = process.env.ENABLE_SELF_PING === "true";
const APP_BASE_URL = (process.env.APP_BASE_URL || "").replace(/\/+$/, "");
const GOOGLE_SHEETS_WEBHOOK_SECRET = process.env.GOOGLE_SHEETS_WEBHOOK_SECRET || "";
const ALLOW_EMPTY_ASSISTANT_SECRET = process.env.ALLOW_EMPTY_ASSISTANT_SECRET === "true";

/* Canlı log ekranı */
const LOG_VIEW_TOKEN = process.env.LOG_VIEW_TOKEN || "";
const MAX_RUNTIME_LOGS = Number(process.env.MAX_RUNTIME_LOGS || 1000);

/* Şablon */
const CALL_THANK_YOU_TEMPLATE_ID =
  process.env.CALL_THANK_YOU_TEMPLATE_ID || "1667339934262053";

/* Süreler */
const MESSAGE_COOLDOWN_MS = Number(process.env.MESSAGE_COOLDOWN_MS || 60 * 60 * 1000); // 1 saat
const CALL_STATE_TTL_MS = Number(process.env.CALL_STATE_TTL_MS || 2 * 60 * 60 * 1000); // 2 saat

/* Rapor */
const CALL_STATS_FILE = path.join(__dirname, "call_stats.json");
const REPORT_TIMEZONE = "Europe/Istanbul";
const REPORT_HOUR = Number(process.env.REPORT_HOUR || 20);
const REPORT_MINUTE = Number(process.env.REPORT_MINUTE || 0);

const CONTACTS_FILE = path.join(__dirname, "rehber.csv");
const HISTORY_FILE = path.join(__dirname, "history.json");
const CALL_TASKS_FILE = path.join(__dirname, "call_tasks.json");
const ASSISTANT_TASKS_FILE = path.join(
  __dirname,
  process.env.ASSISTANT_TASKS_FILE || "assistant_tasks.json"
);

/* =========================
   STATE
========================= */
let replyState = {};
let contactMap = {};
let historyMap = {};
let callTasksMap = {};
let assistantTasksMap = {};
let callStatsStore = {
  daily: {},
  reportSentDates: {},
};

const runtimeLogs = [];

/*
  Anlık çağrı state
*/
const callMap = {};

/* Aynı numaraya kısa sürede tekrar template atmayı engelle */
const recentMessages = {};

/* Eşzamanlı gönderim kilitleri */
const thankYouInFlight = new Set();
const missedTaskInFlight = new Set();

/* Her bot için ayrı polling state */
const botPollingState = {
  main: {
    offset: 0,
    processedUpdateIds: new Set(),
  },
  call: {
    offset: 0,
    processedUpdateIds: new Set(),
  },
  assistant: {
    offset: 0,
    processedUpdateIds: new Set(),
  },
};

/* =========================
   HELPERS
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
  } catch (e) {
    return String(value);
  }
}

function pushRuntimeLog(level, args) {
  const line =
    `${new Date().toISOString()} - [${level}] ` +
    args.map(formatLogArg).join(" ");

  runtimeLogs.push(line);

  if (runtimeLogs.length > MAX_RUNTIME_LOGS) {
    runtimeLogs.splice(0, runtimeLogs.length - MAX_RUNTIME_LOGS);
  }

  return line;
}

function log(...args) {
  console.log(pushRuntimeLog("INFO", args));
}

function logError(...args) {
  console.error(pushRuntimeLog("ERROR", args));
}

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

function isLikelyTurkishMobile(phone) {
  return /^905\d{9}$/.test(normalizePhone(phone));
}

function isAllowedTelegramChat(chatId) {
  return TELEGRAM_CHAT_IDS.includes(String(chatId));
}

function isAllowedCallTelegramChat(chatId) {
  return CALL_TELEGRAM_CHAT_IDS.includes(String(chatId));
}

function isAllowedAssistantTelegramChat(chatId) {
  return ASSISTANT_TELEGRAM_CHAT_IDS.includes(String(chatId));
}

function ensureEnv() {
  const missing = [];

  if (!TELEGRAM_BOT_TOKEN) missing.push("TELEGRAM_BOT_TOKEN");
  if (!VSOFT_API_KEY) missing.push("VSOFT_API_KEY");
  if (!PHONE_NUMBER_ID) missing.push("PHONE_NUMBER_ID");
  if (!BUSINESS_DEVICE_ID) missing.push("BUSINESS_DEVICE_ID");
  if (!VERIFY_TOKEN) missing.push("VERIFY_TOKEN");

  if (missing.length) {
    log("Eksik ENV değişkenleri:", missing.join(", "));
  }

  if (!TELEGRAM_CHAT_IDS.length) {
    log("Uyarı: TELEGRAM_CHAT_IDS boş.");
  }

  if (!CALL_TELEGRAM_BOT_TOKEN || !CALL_TELEGRAM_CHAT_IDS.length) {
    log("Uyarı: CALL_TELEGRAM_BOT_TOKEN / CALL_TELEGRAM_CHAT_IDS eksik. Çağrı botu pasif kalır.");
  }

  if (!ASSISTANT_TELEGRAM_BOT_TOKEN || !ASSISTANT_TELEGRAM_CHAT_IDS.length) {
    log("Uyarı: ASSISTANT_TELEGRAM_BOT_TOKEN / ASSISTANT_TELEGRAM_CHAT_IDS eksik. Asistan botu pasif kalır.");
  }
  if (!GOOGLE_SHEETS_WEBHOOK_SECRET) {
    log("Uyarı: GOOGLE_SHEETS_WEBHOOK_SECRET eksik.");
  }

  if (ALLOW_EMPTY_ASSISTANT_SECRET) {
    log("Uyarı: ALLOW_EMPTY_ASSISTANT_SECRET=true. /asistan endpointinde secret doğrulaması gevşetildi.");
  }

  if (!LOG_VIEW_TOKEN) {
    log("Uyarı: LOG_VIEW_TOKEN boş. /asistan log ekranı herkese açık.");
  }
}

function wasRecentlyMessaged(phone) {
  const normalized = normalizePhone(phone);
  const last = recentMessages[normalized];
  if (!last) return false;
  return Date.now() - last < MESSAGE_COOLDOWN_MS;
}

function markAsMessaged(phone) {
  const normalized = normalizePhone(phone);
  if (!normalized) return;
  recentMessages[normalized] = Date.now();
}

function upsertCallState(uniqueId, patch) {
  if (!uniqueId) return null;

  const existing = callMap[uniqueId] || {
    createdAt: Date.now(),
    updatedAt: Date.now(),
    answered: false,
    thankYouSent: false,
    thankYouSending: false,
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

  for (const [phone, timestamp] of Object.entries(recentMessages)) {
    if (now - timestamp > MESSAGE_COOLDOWN_MS) {
      delete recentMessages[phone];
    }
  }

  for (const [uniqueId, state] of Object.entries(callMap)) {
    if (!state?.updatedAt || now - state.updatedAt > CALL_STATE_TTL_MS) {
      delete callMap[uniqueId];
    }
  }
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

function formatTelegramUser(user) {
  if (!user) return "Bilinmiyor";
  const fullName = [user.first_name, user.last_name].filter(Boolean).join(" ").trim();
  if (fullName) return fullName;
  if (user.username) return `@${user.username}`;
  return String(user.id || "Bilinmiyor");
}

function getZonedDateParts(date = new Date(), timeZone = REPORT_TIMEZONE) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
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

function getDateKeyInIstanbul(date = new Date()) {
  const p = getZonedDateParts(date);
  return `${p.year}-${p.month}-${p.day}`;
}

function getDateKeyDaysAgo(days = 0) {
  return getDateKeyInIstanbul(new Date(Date.now() - days * 24 * 60 * 60 * 1000));
}

function formatDateTimeTR(dateValue) {
  return new Date(dateValue).toLocaleString("tr-TR", { hour12: false });
}

function getTaskKeyByPhoneAndDay(phone, date = new Date()) {
  const normalizedPhone = normalizePhone(phone);
  const dateKey = getDateKeyInIstanbul(date);
  return `${normalizedPhone}_${dateKey}`;
}

async function sendCallThankYouOnce(uniqueId, phone, campaignName) {
  const state = getCallState(uniqueId);

  if (!state) {
    log("sendCallThankYouOnce -> state bulunamadı:", uniqueId, phone);
    return false;
  }

  const targetPhone = normalizePhone(phone || state.phone);

  if (!isLikelyTurkishMobile(targetPhone)) {
    log("Teşekkür şablonu atlanıyor -> uygun mobil numara değil:", targetPhone);
    return false;
  }

  const uniqueLockKey = `call:${uniqueId}`;
  const phoneLockKey = `phone:${targetPhone}`;

  if (thankYouInFlight.has(uniqueLockKey) || thankYouInFlight.has(phoneLockKey)) {
    log("Teşekkür şablonu atlanıyor -> işlem zaten devam ediyor:", uniqueId, targetPhone);
    return false;
  }

  if (state.thankYouSent) {
    log("Teşekkür şablonu atlanıyor -> aynı çağrı için zaten gönderilmiş:", uniqueId, targetPhone);
    return false;
  }

  if (wasRecentlyMessaged(targetPhone)) {
    log("Teşekkür şablonu atlanıyor -> cooldown aktif:", targetPhone);
    return false;
  }

  thankYouInFlight.add(uniqueLockKey);
  thankYouInFlight.add(phoneLockKey);

  upsertCallState(uniqueId, {
    phone: targetPhone,
    thankYouSending: true,
  });

  try {
    await sendWhatsAppTemplate(targetPhone, CALL_THANK_YOU_TEMPLATE_ID, campaignName);

    upsertCallState(uniqueId, {
      phone: targetPhone,
      thankYouSending: false,
      thankYouSent: true,
    });

    markAsMessaged(targetPhone);
    addHistory(
      targetPhone,
      "out",
      "[Şablon] Ferah Kurban ile iletişime geçtiğiniz için teşekkür ederiz."
    );

    incrementDailyStat("thankYouTemplatesSent");

    log("Teşekkür şablonu başarıyla gönderildi:", uniqueId, targetPhone, campaignName);
    return true;
  } catch (e) {
    upsertCallState(uniqueId, {
      phone: targetPhone,
      thankYouSending: false,
    });

    logError("Teşekkür şablonu gönderim hatası:", uniqueId, targetPhone, e.message);
    throw e;
  } finally {
    thankYouInFlight.delete(uniqueLockKey);
    thankYouInFlight.delete(phoneLockKey);
  }
}

/* =========================
   REQUEST LOGGING
========================= */
app.use((req, res, next) => {
  const startedAt = Date.now();

  const skip =
    (req.method === "GET" && req.path === "/asistan") ||
    (req.method === "GET" && req.path === "/asistan/logs") ||
    req.path === "/favicon.ico";

  if (!skip) {
    log("HTTP IN", {
      method: req.method,
      url: req.originalUrl,
      ip: req.ip || req.headers["x-forwarded-for"] || "",
      query: req.query || {},
      body: req.body || {},
    });
  }

  res.on("finish", () => {
    if (!skip) {
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
   CSV / CONTACTS
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
   HISTORY
========================= */
function loadHistory() {
  historyMap = safeJsonRead(HISTORY_FILE, {});
  log(`Geçmiş yüklendi. Kişi sayısı: ${Object.keys(historyMap).length}`);
}

function saveHistory() {
  safeJsonWrite(HISTORY_FILE, historyMap);
}

function addHistory(phone, type, text) {
  const p = normalizePhone(phone);
  if (!p) return;

  if (!historyMap[p]) historyMap[p] = [];

  historyMap[p].push({
    type,
    text: String(text || "").trim(),
    time: Date.now(),
  });

  historyMap[p] = historyMap[p].slice(-20);
  saveHistory();
}

function getHistory(phone, limit = 5) {
  const p = normalizePhone(phone);
  const list = historyMap[p] || [];

  if (!list.length) return "📜 Geçmiş yok";

  return (
    "📜 Son Mesajlar:\n" +
    list
      .slice(-limit)
      .map((x) => {
        const who = x.type === "in" ? "Müşteri" : "Sen";
        return `• ${who}: ${x.text}`;
      })
      .join("\n")
  );
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
    `📱 ${task.phone}`,
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
      logError(`Call task mesaj güncelleme hatası -> chat_id=${ref.chatId}, message_id=${ref.messageId}:`, e.message);
    }
  }

  saveCallTasks();
}

/* =========================
   ASSISTANT TASKS
========================= */
function loadAssistantTasks() {
  assistantTasksMap = safeJsonRead(ASSISTANT_TASKS_FILE, {});
  log(`Asistan görevleri yüklendi. Kayıt sayısı: ${Object.keys(assistantTasksMap).length}`);
}

function saveAssistantTasks() {
  safeJsonWrite(ASSISTANT_TASKS_FILE, assistantTasksMap);
}

function getAssistantTaskStatusLabel(status) {
  switch (status) {
    case "called":
      return "✅ Arandı";
    case "saved":
      return "📝 Kaydedildi";
    case "ignore":
      return "🚫 Gereksiz";
    case "open":
    default:
      return "🟥 Açık";
  }
}

function buildAssistantTaskText(task) {
  const lines = [
    "📥 Yeni Form Kaydı",
    "",
    `Durum: ${getAssistantTaskStatusLabel(task.status)}`,
    `👤 Ad Soyad: ${task.adiSoyadi || "-"}`,
    `📞 Telefon: ${task.telefon || "-"}`,
    `🤝 Katılım: ${task.katilim || "-"}`,
    `🐄 Hayvan Türü: ${task.hayvanTuru || "-"}`,
    `📂 Alt Tür: ${task.altTur || "-"}`,
    `📦 Paket Türü: ${task.paketTuru || "-"}`,
    `🔪 Kesim/Paketleme: ${task.paketleme || "-"}`,
    `💰 Fiyat: ${task.fiyatAraligi || "-"}`,
    `🕒 Tarih: ${task.tarih || "-"}`,
    `📍 Satır No: ${task.rowNumber || "-"}`,
  ];

  if (task.updatedBy) {
    lines.push(`🙋 Güncelleyen: ${task.updatedBy}`);
  }

  if (task.updatedAt && task.createdAt && task.updatedAt !== task.createdAt) {
    lines.push(`📝 Son Güncelleme: ${formatDateTimeTR(task.updatedAt)}`);
  }

  return lines.join("\n");
}

function buildAssistantTaskKeyboard(task) {
  const current = task.status || "open";
  const label = (status, text) => (current === status ? `• ${text}` : text);

  return {
    inline_keyboard: [
      [
        { text: label("called", "✅ Arandı"), callback_data: `assistanttask|called|${task.id}` },
        { text: label("saved", "📝 Kaydedildi"), callback_data: `assistanttask|saved|${task.id}` },
      ],
      [
        { text: label("ignore", "🚫 Gereksiz"), callback_data: `assistanttask|ignore|${task.id}` },
        { text: label("open", "🟥 Açık"), callback_data: `assistanttask|open|${task.id}` },
      ],
    ],
  };
}

async function createAssistantTaskFromSheet(payload) {
  const safeRowNumber =
    payload.rowNumber ||
    payload.row_number ||
    payload.id ||
    `${Date.now()}`;

  const taskId = `sheet_${safeRowNumber}`;

  if (assistantTasksMap[taskId]) {
    const existing = assistantTasksMap[taskId];

    existing.updatedAt = Date.now();
    existing.adiSoyadi = payload.adi_soyadi || existing.adiSoyadi || "";
    existing.telefon = normalizePhone(payload.telefon || existing.telefon || "");
    existing.katilim = payload.katilim || existing.katilim || "";
    existing.hayvanTuru = payload.hayvan_turu || existing.hayvanTuru || "";
    existing.altTur = payload.alt_tur || existing.altTur || "";
    existing.paketTuru = payload.paket_turu || existing.paketTuru || "";
    existing.paketleme = payload.paketleme || existing.paketleme || "";
    existing.fiyatAraligi = payload.fiyat_araligi || existing.fiyatAraligi || "";
    existing.tarih = payload.tarih || existing.tarih || "";

    saveAssistantTasks();
    await syncAssistantTaskMessages(existing);
    return existing;
  }

  const task = {
    id: taskId,
    status: "open",
    createdAt: Date.now(),
    updatedAt: Date.now(),
    updatedBy: null,
    rowNumber: safeRowNumber,
    adiSoyadi: payload.adi_soyadi || "",
    telefon: normalizePhone(payload.telefon || ""),
    katilim: payload.katilim || "",
    hayvanTuru: payload.hayvan_turu || "",
    altTur: payload.alt_tur || "",
    paketTuru: payload.paket_turu || "",
    paketleme: payload.paketleme || "",
    fiyatAraligi: payload.fiyat_araligi || "",
    tarih: payload.tarih || "",
    messageRefs: [],
    type: "sheet_form_task",
  };

  assistantTasksMap[taskId] = task;
  saveAssistantTasks();

  await sendAssistantTaskNotifications(task);
  return task;
}

async function sendAssistantTaskNotifications(task) {
  if (!ASSISTANT_TELEGRAM_BOT_TOKEN || !ASSISTANT_TELEGRAM_CHAT_IDS.length) {
    log("Assistant bot pasif; görev gönderilmedi.");
    return;
  }

  const text = buildAssistantTaskText(task);
  const keyboard = buildAssistantTaskKeyboard(task);

  if (!task.messageRefs?.length) {
    for (const chatId of ASSISTANT_TELEGRAM_CHAT_IDS) {
      try {
        const result = await sendTelegramMessageViaBot(
          ASSISTANT_TELEGRAM_BOT_TOKEN,
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
        logError(`Assistant task gönderim hatası -> chat_id=${chatId}:`, e.message);
      }
    }

    task.updatedAt = Date.now();
    saveAssistantTasks();
    return;
  }

  await syncAssistantTaskMessages(task);
}

async function syncAssistantTaskMessages(task) {
  if (!task?.messageRefs?.length) {
    saveAssistantTasks();
    return;
  }

  const text = buildAssistantTaskText(task);
  const keyboard = buildAssistantTaskKeyboard(task);

  for (const ref of task.messageRefs) {
    try {
      await telegramRequest(ASSISTANT_TELEGRAM_BOT_TOKEN, "editMessageText", {
        chat_id: String(ref.chatId),
        message_id: ref.messageId,
        text,
        reply_markup: keyboard,
      });
    } catch (e) {
      logError(
        `Assistant task mesaj güncelleme hatası -> chat_id=${ref.chatId}, message_id=${ref.messageId}:`,
        e.message
      );
    }
  }

  saveAssistantTasks();
}

/* =========================
   CALL STATS / REPORTS
========================= */
function loadCallStats() {
  callStatsStore = safeJsonRead(CALL_STATS_FILE, {
    daily: {},
    reportSentDates: {},
  });

  if (!callStatsStore.daily) callStatsStore.daily = {};
  if (!callStatsStore.reportSentDates) callStatsStore.reportSentDates = {};

  log(`Çağrı istatistikleri yüklendi. Gün sayısı: ${Object.keys(callStatsStore.daily).length}`);
}

function saveCallStats() {
  safeJsonWrite(CALL_STATS_FILE, callStatsStore);
}

function ensureDailyStats(dateKey) {
  if (!callStatsStore.daily[dateKey]) {
    callStatsStore.daily[dateKey] = {
      inboundAnswered: 0,
      inboundMissed: 0,
      outboundAnswered: 0,
      outboundNoAnswer: 0,
      thankYouTemplatesSent: 0,
      missedTasksCreated: 0,
      taskDoneActions: 0,
      taskLaterActions: 0,
      taskIgnoreActions: 0,
      taskReopenedActions: 0,
    };
  }

  return callStatsStore.daily[dateKey];
}

function incrementDailyStat(statKey, amount = 1, dateKey = getDateKeyInIstanbul()) {
  const stats = ensureDailyStats(dateKey);
  if (typeof stats[statKey] !== "number") {
    stats[statKey] = 0;
  }
  stats[statKey] += amount;
  saveCallStats();
}

function countOpenTasksForDate(dateKey) {
  return Object.values(callTasksMap).filter((t) => {
    return t.status === "open" && getDateKeyInIstanbul(new Date(t.createdAt)) === dateKey;
  }).length;
}

function countAllOpenTasks() {
  return Object.values(callTasksMap).filter((t) => t.status === "open").length;
}

function buildDailyReport(dateKey) {
  const stats = ensureDailyStats(dateKey);
  const openToday = countOpenTasksForDate(dateKey);
  const allOpen = countAllOpenTasks();

  const humanDate = new Date(`${dateKey}T12:00:00`).toLocaleDateString("tr-TR", {
    timeZone: REPORT_TIMEZONE,
  });

  return [
    `📊 Günlük Çağrı Raporu`,
    ``,
    `📅 Tarih: ${humanDate}`,
    ``,
    `📥 Inbound`,
    `• Cevaplanan: ${stats.inboundAnswered}`,
    `• Kaçan: ${stats.inboundMissed}`,
    ``,
    `📤 Outbound`,
    `• Görüşülen: ${stats.outboundAnswered}`,
    `• Cevapsız: ${stats.outboundNoAnswer}`,
    ``,
    `💬 WhatsApp`,
    `• Gönderilen teşekkür şablonu: ${stats.thankYouTemplatesSent}`,
    ``,
    `📋 Görev`,
    `• Açılan günlük kaçan çağrı kartı: ${stats.missedTasksCreated}`,
    `• Geri arandı işaretlenen: ${stats.taskDoneActions}`,
    `• Daha sonra işaretlenen: ${stats.taskLaterActions}`,
    `• Gereksiz işaretlenen: ${stats.taskIgnoreActions}`,
    `• Yeniden açık yapılan: ${stats.taskReopenedActions}`,
    `• Bugünden açık kalan: ${openToday}`,
    `• Toplam açık görev: ${allOpen}`,
  ].join("\n");
}

async function sendDailyReportIfDue() {
  const parts = getZonedDateParts();
  const dateKey = getDateKeyInIstanbul();

  if (parts.hour !== REPORT_HOUR || parts.minute !== REPORT_MINUTE) {
    return;
  }

  if (callStatsStore.reportSentDates[dateKey]) {
    return;
  }

  const reportText = buildDailyReport(dateKey);
  await sendCallTelegram(reportText);

  callStatsStore.reportSentDates[dateKey] = true;
  saveCallStats();

  log("Günlük çağrı raporu gönderildi:", dateKey);
}

function startDailyReportScheduler() {
  setInterval(async () => {
    try {
      await sendDailyReportIfDue();
    } catch (e) {
      logError("Günlük rapor scheduler hatası:", e.message);
    }
  }, 30 * 1000);

  log(
    `Günlük rapor scheduler aktif: ${REPORT_HOUR}:${String(REPORT_MINUTE).padStart(2, "0")} (${REPORT_TIMEZONE})`
  );
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

/* Ana operasyon botu */
async function sendTelegram(text, buttons = null, targetChatIds = null) {
  const chatIds = targetChatIds || TELEGRAM_CHAT_IDS;

  for (const chatId of chatIds) {
    try {
      await sendTelegramMessageViaBot(TELEGRAM_BOT_TOKEN, chatId, text, buttons);
      log(`Telegram mesaj gönderildi -> chat_id=${chatId}`);
    } catch (e) {
      logError(`Telegram gönderim hatası -> chat_id=${chatId}:`, e.message);
    }
  }
}

/* Ayrı çağrı botu */
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

/* Asistan botu */
async function sendAssistantTelegram(text, targetChatIds = null, buttons = null) {
  const chatIds = targetChatIds || ASSISTANT_TELEGRAM_CHAT_IDS;

  if (!ASSISTANT_TELEGRAM_BOT_TOKEN || !chatIds.length) {
    log("Assistant bot aktif değil. Bildirim atlandı.");
    return;
  }

  for (const chatId of chatIds) {
    try {
      await sendTelegramMessageViaBot(ASSISTANT_TELEGRAM_BOT_TOKEN, chatId, text, buttons);
      log(`Assistant bot mesaj gönderildi -> chat_id=${chatId}`);
    } catch (e) {
      logError(`Assistant bot gönderim hatası -> chat_id=${chatId}:`, e.message);
    }
  }
}

async function answerMainCallback(id, text = "") {
  try {
    await telegramRequest(TELEGRAM_BOT_TOKEN, "answerCallbackQuery", {
      callback_query_id: id,
      text,
      show_alert: false,
    });
  } catch (e) {
    logError("Main callback cevap hatası:", e.message);
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

async function answerAssistantCallback(id, text = "") {
  try {
    await telegramRequest(ASSISTANT_TELEGRAM_BOT_TOKEN, "answerCallbackQuery", {
      callback_query_id: id,
      text,
      show_alert: false,
    });
  } catch (e) {
    logError("Assistant callback cevap hatası:", e.message);
  }
}

/* =========================
   WHATSAPP API
========================= */
async function sendWhatsApp(to, text) {
  const response = await fetch("https://api.toplusms.app/api/v1/wabusiness/message/send", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Api-Key": VSOFT_API_KEY,
    },
    body: JSON.stringify({
      message_type: 1,
      payload: {
        text_message: {
          message: String(text || ""),
        },
      },
      to: normalizePhone(to),
      phone_number_id: PHONE_NUMBER_ID,
    }),
  });

  const data = await response.json().catch(() => ({}));
  log("WhatsApp gönderim yanıtı:", data);

  if (!response.ok) {
    throw new Error(JSON.stringify(data));
  }

  return data;
}

async function sendWhatsAppTemplate(to, templateId, campaignName = "Call Thank You") {
  const normalizedPhone = normalizePhone(to);

  const response = await fetch("https://api.toplusms.app/api/v1/wabusiness/template/send", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Api-Key": VSOFT_API_KEY,
    },
    body: JSON.stringify({
      template_id: templateId,
      recipients: [
        {
          to: normalizedPhone,
        },
      ],
      campaign_name: campaignName,
      business_device_id: BUSINESS_DEVICE_ID,
      requests_per_minute: 60,
    }),
  });

  const data = await response.json().catch(() => ({}));
  log("WhatsApp template gönderim yanıtı:", data);

  if (!response.ok) {
    throw new Error(JSON.stringify(data));
  }

  return data;
}

/* =========================
   TELEGRAM BUTTONS (MAIN BOT)
========================= */
function buttons(phone) {
  return {
    inline_keyboard: [
      [
        { text: "✉️ Yanıtla", callback_data: `reply:${phone}` },
        { text: "📜 Geçmiş", callback_data: `history:${phone}` },
      ],
      [
        { text: "👋 Karşılama", callback_data: `q1:${phone}` },
        { text: "🕒 Bilgi", callback_data: `q2:${phone}` },
      ],
      [{ text: "❌ İptal", callback_data: `cancel:${phone}` }],
    ],
  };
}

/* =========================
   MAIN BOT MESSAGE PROCESS
========================= */
async function processMainTelegramMessage(update) {
  const message = update.message;
  if (!message) return;

  const chatId = String(message.chat.id);
  const text = String(message.text || "").trim();

  if (!isAllowedTelegramChat(chatId)) return;
  if (!text) return;

  if (text === "/help") {
    await sendTelegram(
      "Komutlar:\n\n" +
        "/help\n" +
        "/reload\n" +
        "/cancel\n" +
        "/history 90xxxxxxxxxx\n" +
        "/reply 90xxxxxxxxxx Mesajınız\n\n" +
        "Kullanım:\n" +
        "1) Gelen WhatsApp mesajındaki '✉️ Yanıtla' butonuna bas\n" +
        "2) Telegram'a düz mesajını yaz\n" +
        "3) Mesaj müşteriye WhatsApp'tan gider",
      null,
      [chatId]
    );
    return;
  }

  if (text === "/reload") {
    loadContacts();
    await sendTelegram("✅ Rehber yeniden yüklendi.", null, [chatId]);
    return;
  }

  if (text === "/cancel") {
    delete replyState[chatId];
    await sendTelegram("❌ Yanıtlama modu kapatıldı.", null, [chatId]);
    return;
  }

  if (text.startsWith("/history ")) {
    const phone = normalizePhone(text.replace("/history ", ""));
    await sendTelegram(
      `📜 Müşteri Geçmişi\n\n👤 ${getName(phone)}\n📱 ${phone}\n\n${getHistory(phone, 10)}`,
      null,
      [chatId]
    );
    return;
  }

  if (replyState[chatId] && !text.startsWith("/")) {
    const phone = replyState[chatId];

    try {
      await sendWhatsApp(phone, text);
      addHistory(phone, "out", text);

      await sendTelegram(
        `✅ Mesaj Gönderildi\n\n👤 ${getName(phone)}\n📱 ${phone}\n\n💬 ${text}`,
        null,
        TELEGRAM_CHAT_IDS
      );

      delete replyState[chatId];
    } catch (e) {
      await sendTelegram(`❌ Gönderim hatası\n\n${e.message}`, null, [chatId]);
    }
    return;
  }

  if (text.startsWith("/reply ")) {
    const parts = text.split(" ");
    if (parts.length < 3) {
      await sendTelegram("Kullanım:\n/reply 90xxxxxxxxxx Mesajınız", null, [chatId]);
      return;
    }

    const phone = normalizePhone(parts[1]);
    const replyText = parts.slice(2).join(" ").trim();

    try {
      await sendWhatsApp(phone, replyText);
      addHistory(phone, "out", replyText);

      await sendTelegram(
        `✅ Mesaj Gönderildi\n\n👤 ${getName(phone)}\n📱 ${phone}\n\n💬 ${replyText}`,
        null,
        TELEGRAM_CHAT_IDS
      );
    } catch (e) {
      await sendTelegram(`❌ Gönderim hatası\n\n${e.message}`, null, [chatId]);
    }
  }
}

/* =========================
   MAIN BOT CALLBACK PROCESS
========================= */
async function processMainTelegramCallback(update) {
  const callback = update.callback_query;
  if (!callback) return;

  const chatId = String(callback.message.chat.id);
  if (!isAllowedTelegramChat(chatId)) return;

  const data = callback.data || "";
  const [type, phoneRaw] = data.split(":");
  const phone = normalizePhone(phoneRaw);

  await answerMainCallback(callback.id);

  if (type === "reply") {
    replyState[chatId] = phone;

    await sendTelegram(
      `✍️ Yanıtlama Modu Açıldı\n\n👤 ${getName(phone)}\n📱 ${phone}\n\nŞimdi Telegram'a düz mesajını yaz.\nİptal için: /cancel`,
      null,
      [chatId]
    );
    return;
  }

  if (type === "history") {
    await sendTelegram(
      `📜 Müşteri Geçmişi\n\n👤 ${getName(phone)}\n📱 ${phone}\n\n${getHistory(phone, 10)}`,
      null,
      [chatId]
    );
    return;
  }

  if (type === "q1") {
    const msg = "Merhaba, mesajınızı aldık. En kısa sürede dönüş yapacağız.";

    try {
      await sendWhatsApp(phone, msg);
      addHistory(phone, "out", msg);

      await sendTelegram(
        `✅ Hazır Mesaj Gönderildi\n\n👤 ${getName(phone)}\n📱 ${phone}\n\n💬 ${msg}`,
        null,
        TELEGRAM_CHAT_IDS
      );
    } catch (e) {
      await sendTelegram(`❌ Gönderim hatası\n\n${e.message}`, null, [chatId]);
    }
    return;
  }

  if (type === "q2") {
    const msg = "Merhaba, talebinizi aldım. Kısa süre içinde detaylı bilgi paylaşacağım.";

    try {
      await sendWhatsApp(phone, msg);
      addHistory(phone, "out", msg);

      await sendTelegram(
        `✅ Hazır Mesaj Gönderildi\n\n👤 ${getName(phone)}\n📱 ${phone}\n\n💬 ${msg}`,
        null,
        TELEGRAM_CHAT_IDS
      );
    } catch (e) {
      await sendTelegram(`❌ Gönderim hatası\n\n${e.message}`, null, [chatId]);
    }
    return;
  }

  if (type === "cancel") {
    delete replyState[chatId];
    await sendTelegram("❌ İşlem iptal edildi.", null, [chatId]);
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

  if (text === "/help") {
    await sendCallTelegram(
      "Komutlar:\n\n/help\n/open_calls\n/sync_calls\n/report_today\n/report_yesterday\n\n" +
        "/open_calls -> açık çağrı görevlerini listeler\n" +
        "/sync_calls -> bu chate açık görev kartlarını tekrar yollar\n" +
        "/report_today -> bugünün raporu\n" +
        "/report_yesterday -> dünün raporu",
      [chatId]
    );
    return;
  }

  if (text === "/open_calls") {
    const openTasks = Object.values(callTasksMap)
      .filter((t) => t.status === "open")
      .sort((a, b) => b.lastCallAt - a.lastCallAt)
      .slice(0, 20);

    if (!openTasks.length) {
      await sendCallTelegram("Açık çağrı görevi yok.", [chatId]);
      return;
    }

    const textLines = ["📋 Açık çağrı görevleri", ""];
    for (const t of openTasks) {
      textLines.push(`• ${t.name || "Kayıtlı değil"} - ${t.phone} (${t.missedCount || 1})`);
    }

    await sendCallTelegram(textLines.join("\n"), [chatId]);
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
            (x) => String(x.chatId) === String(chatId) && String(x.messageId) === String(result.message_id)
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
    return;
  }

  if (text === "/report_today") {
    await sendCallTelegram(buildDailyReport(getDateKeyInIstanbul()), [chatId]);
    return;
  }

  if (text === "/report_yesterday") {
    await sendCallTelegram(buildDailyReport(getDateKeyDaysAgo(1)), [chatId]);
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

  const oldStatus = task.status;

  task.status = newStatus;
  task.updatedAt = Date.now();
  task.updatedBy = formatTelegramUser(callback.from);

  if (oldStatus !== newStatus) {
    if (newStatus === "done") incrementDailyStat("taskDoneActions");
    if (newStatus === "later") incrementDailyStat("taskLaterActions");
    if (newStatus === "ignore") incrementDailyStat("taskIgnoreActions");
    if (newStatus === "open") incrementDailyStat("taskReopenedActions");
  }

  saveCallTasks();
  await syncCallTaskMessages(task);
  await answerCallCallback(callback.id, `Durum güncellendi: ${getTaskStatusLabel(newStatus)}`);
}

/* =========================
   ASSISTANT BOT MESSAGE PROCESS
========================= */
async function processAssistantTelegramMessage(update) {
  const message = update.message;
  if (!message) return;

  const chatId = String(message.chat.id);
  const text = String(message.text || "").trim();

  if (!isAllowedAssistantTelegramChat(chatId)) return;

  if (text === "/help") {
    await sendAssistantTelegram(
      "Komutlar:\n\n/help\n/open_forms\n\n/open_forms -> açık form görevlerini listeler",
      [chatId]
    );
    return;
  }

  if (text === "/open_forms") {
    const openTasks = Object.values(assistantTasksMap)
      .filter((t) => t.status === "open")
      .sort((a, b) => b.createdAt - a.createdAt)
      .slice(0, 20);

    if (!openTasks.length) {
      await sendAssistantTelegram("Açık form görevi yok.", [chatId]);
      return;
    }

    const lines = ["📋 Açık form görevleri", ""];
    for (const t of openTasks) {
      lines.push(`• Satır ${t.rowNumber} - ${t.adiSoyadi || "-"} - ${t.telefon || "-"}`);
    }

    await sendAssistantTelegram(lines.join("\n"), [chatId]);
  }
}

/* =========================
   ASSISTANT BOT CALLBACK PROCESS
========================= */
async function processAssistantTelegramCallback(update) {
  const callback = update.callback_query;
  if (!callback) return;

  const chatId = String(callback.message?.chat?.id || "");
  if (!isAllowedAssistantTelegramChat(chatId)) {
    await answerAssistantCallback(callback.id, "Yetkisiz");
    return;
  }

  const data = callback.data || "";
  const parts = data.split("|");

  if (parts.length !== 3 || parts[0] !== "assistanttask") {
    await answerAssistantCallback(callback.id, "Bilinmeyen işlem");
    return;
  }

  const newStatus = parts[1];
  const taskId = parts[2];
  const task = assistantTasksMap[taskId];

  if (!task) {
    await answerAssistantCallback(callback.id, "Görev bulunamadı");
    return;
  }

  task.status = newStatus;
  task.updatedAt = Date.now();
  task.updatedBy = formatTelegramUser(callback.from);

  saveAssistantTasks();
  await syncAssistantTaskMessages(task);
  await answerAssistantCallback(
    callback.id,
    `Durum güncellendi: ${getAssistantTaskStatusLabel(newStatus)}`
  );
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

async function handleMainBotUpdate(update) {
  if (update.message) {
    await processMainTelegramMessage(update);
  }

  if (update.callback_query) {
    await processMainTelegramCallback(update);
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

async function handleAssistantBotUpdate(update) {
  if (update.message) {
    await processAssistantTelegramMessage(update);
  }

  if (update.callback_query) {
    await processAssistantTelegramCallback(update);
  }
}

/* =========================
   HEALTH / KEEP ALIVE
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
  }, 5 * 60 * 1000);

  log("Self-ping aktif:", target);
}

/* =========================
   CANLI LOG EKRANI
========================= */
app.get("/asistan", (req, res) => {
  if (LOG_VIEW_TOKEN && String(req.query.token || "") !== LOG_VIEW_TOKEN) {
    return res.status(403).send("unauthorized");
  }

  res.setHeader("Content-Type", "text/html; charset=utf-8");

  return res.send(`<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Canlı Loglar</title>
  <style>
    body {
      margin: 0;
      background: #0b1020;
      color: #e5e7eb;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .bar {
      position: sticky;
      top: 0;
      z-index: 10;
      background: #111827;
      border-bottom: 1px solid #1f2937;
      padding: 12px 16px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    .badge {
      background: #1f2937;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
    }
    button {
      border: 0;
      border-radius: 8px;
      padding: 8px 12px;
      cursor: pointer;
      font: inherit;
      background: #2563eb;
      color: white;
    }
    button.secondary {
      background: #374151;
    }
    #wrap {
      padding: 16px;
    }
    #log {
      white-space: pre-wrap;
      word-break: break-word;
      line-height: 1.45;
      font-size: 13px;
      min-height: calc(100vh - 100px);
    }
    input {
      padding: 8px 10px;
      border-radius: 8px;
      border: 1px solid #374151;
      background: #0f172a;
      color: #fff;
      min-width: 240px;
    }
  </style>
</head>
<body>
  <div class="bar">
    <div class="badge">/asistan canlı log ekranı</div>
    <div class="badge" id="count">0 kayıt</div>
    <div class="badge" id="updated">-</div>
    <input id="search" placeholder="Filtrele... telefon, hata, rowNumber..." />
    <button id="toggle">Oto yenile: Açık</button>
    <button id="refresh" class="secondary">Yenile</button>
  </div>

  <div id="wrap">
    <div id="log">Yükleniyor...</div>
  </div>

  <script>
    const params = new URLSearchParams(location.search);
    const token = params.get("token") || "";
    const searchInput = document.getElementById("search");
    const logEl = document.getElementById("log");
    const countEl = document.getElementById("count");
    const updatedEl = document.getElementById("updated");
    const toggleBtn = document.getElementById("toggle");
    const refreshBtn = document.getElementById("refresh");

    let autoRefresh = true;
    let allLogs = [];

    function renderLogs() {
      const q = (searchInput.value || "").toLowerCase().trim();
      const filtered = !q
        ? allLogs
        : allLogs.filter(x => String(x).toLowerCase().includes(q));

      logEl.textContent = filtered.length ? filtered.join("\\n") : "Kayıt yok.";
      countEl.textContent = filtered.length + " kayıt";
    }

    async function loadLogs() {
      try {
        const qs = token ? ("?token=" + encodeURIComponent(token)) : "";
        const res = await fetch("/asistan/logs" + qs, { cache: "no-store" });
        const data = await res.json();

        if (!res.ok || !data.ok) {
          logEl.textContent = data.message || "Log alınamadı";
          return;
        }

        allLogs = Array.isArray(data.logs) ? data.logs : [];
        updatedEl.textContent = "Son güncelleme: " + (data.generatedAt || "-");
        renderLogs();
      } catch (err) {
        logEl.textContent = "Log çekme hatası: " + (err.message || err);
      }
    }

    toggleBtn.addEventListener("click", () => {
      autoRefresh = !autoRefresh;
      toggleBtn.textContent = "Oto yenile: " + (autoRefresh ? "Açık" : "Kapalı");
    });

    refreshBtn.addEventListener("click", loadLogs);
    searchInput.addEventListener("input", renderLogs);

    loadLogs();
    setInterval(() => {
      if (autoRefresh) loadLogs();
    }, 1500);
  </script>
</body>
</html>`);
});

app.get("/asistan/logs", (req, res) => {
  try {
    if (LOG_VIEW_TOKEN && String(req.query.token || "") !== LOG_VIEW_TOKEN) {
      return res.status(403).json({ ok: false, message: "unauthorized" });
    }

    return res.status(200).json({
      ok: true,
      logs: runtimeLogs,
      count: runtimeLogs.length,
      generatedAt: new Date().toLocaleString("tr-TR", { hour12: false }),
    });
  } catch (e) {
    logError("GET /asistan/logs hatası:", e);
    return res.status(500).json({ ok: false, message: "log view error" });
  }
});

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
        thankYouSent: false,
        thankYouSending: false,
      });
      return;
    }

    if (scenario === "InboundtoPBX" || scenario === "Inbound_call" || scenario === "Queue") {
      upsertCallState(uniqueId, {
        type: "inbound",
        phone,
        answered: false,
        thankYouSent: false,
        thankYouSending: false,
      });
      return;
    }

    if (scenario === "Answer") {
      const current = getCallState(uniqueId);

      const state = upsertCallState(uniqueId, {
        type: current?.type || "unknown",
        phone: current?.phone || phone,
        answered: true,
      });

      if (state?.type === "inbound") {
        const sent = await sendCallThankYouOnce(
          uniqueId,
          state.phone,
          "Inbound Call Thank You"
        );

        if (sent) {
          incrementDailyStat("inboundAnswered");
          log("Inbound Answer -> teşekkür şablonu gönderildi:", uniqueId, state.phone);
        }
      }

      return;
    }

    if (scenario === "Hangup") {
      const state = getCallState(uniqueId);
      const targetPhone = normalizePhone(state?.phone || phone);

      if (state?.type === "outbound") {
        if (state.answered) {
          const sent = await sendCallThankYouOnce(
            uniqueId,
            targetPhone,
            "Outbound Call Thank You"
          );

          if (sent) {
            incrementDailyStat("outboundAnswered");
            log("Outbound Hangup -> teşekkür şablonu gönderildi:", uniqueId, targetPhone);
          }
        } else {
          incrementDailyStat("outboundNoAnswer");
          log("Outbound Hangup -> Answer yok, şablon gönderilmedi:", targetPhone);
        }

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

            incrementDailyStat("inboundMissed");

            if (created) {
              incrementDailyStat("missedTasksCreated");
              log("Inbound Hangup -> yeni günlük missed call kartı oluşturuldu:", taskKey, targetPhone);
            } else {
              log(
                "Inbound Hangup -> mevcut günlük kart güncellendi:",
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
          log("Inbound Hangup -> çağrı cevaplanmış, missed call task oluşturulmadı:", targetPhone);
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
   ASSISTANT SHEET WEBHOOK
========================= */
app.post("/asistan", async (req, res) => {
  try {
    const rawBody = req.body || {};

    // Payload bazen body.data içinde, bazen direkt body içinde gelir
    const source =
      rawBody.data && typeof rawBody.data === "object"
        ? rawBody.data
        : rawBody;

    // Secret farklı yerlerden gelebilir
    const incomingSecret =
      String(
        rawBody.secret ||
        source.secret ||
        source.webhook_secret ||
        source.token ||
        req.headers["x-webhook-secret"] ||
        req.headers["x-assistant-secret"] ||
        req.headers["x-api-key"] ||
        req.query.secret ||
        ""
      ).trim();
    
    const normalizedData = {
      rowNumber:
        source.rowNumber ||
        source.row_number ||
        source.id ||
        `${Date.now()}`,
      adi_soyadi: String(source.adi_soyadi || source.adSoyad || source.name || "").trim(),
      telefon: normalizePhone(source.telefon || source.phone || source.tel || ""),
      katilim: String(source.katilim || "").trim(),
      hayvan_turu: String(source.hayvan_turu || source.hayvanTuru || "").trim(),
      alt_tur: String(source.alt_tur || source.altTur || "").trim(),
      paket_turu: String(source.paket_turu || source.paketTuru || "").trim(),
      paketleme: String(source.paketleme || "").trim(),
      fiyat_araligi: String(source.fiyat_araligi || source.fiyatAraligi || "").trim(),
      tarih: String(source.tarih || "").trim(),
    };

    log("Asistan webhook raw body:", rawBody);

    log("Asistan webhook normalize edildi:", {
      rowNumber: normalizedData.rowNumber,
      adiSoyadi: normalizedData.adi_soyadi,
      telefon: normalizedData.telefon,
      katilim: normalizedData.katilim,
      hayvanTuru: normalizedData.hayvan_turu,
      altTur: normalizedData.alt_tur,
      paketTuru: normalizedData.paket_turu,
      paketleme: normalizedData.paketleme,
      fiyatAraligi: normalizedData.fiyat_araligi,
      tarih: normalizedData.tarih,
    });

    // Secret tanımlıysa doğrula, tanımlı değilse geç
    if (
      GOOGLE_SHEETS_WEBHOOK_SECRET &&
      !ALLOW_EMPTY_ASSISTANT_SECRET &&
      incomingSecret !== GOOGLE_SHEETS_WEBHOOK_SECRET
    ) {
      log("Asistan webhook unauthorized:", {
        incomingSecretPresent: Boolean(incomingSecret),
        hasExpectedSecret: Boolean(GOOGLE_SHEETS_WEBHOOK_SECRET),
      });

      return res.status(403).json({ ok: false, message: "unauthorized" });
    }

    if (!normalizedData.telefon && !normalizedData.adi_soyadi) {
      return res.status(400).json({
        ok: false,
        message: "geçerli veri yok",
      });
    }

    const task = await createAssistantTaskFromSheet(normalizedData);

    log("Asistan görevi oluşturuldu / güncellendi:", {
      taskId: task.id,
      rowNumber: task.rowNumber,
      telefon: task.telefon,
      adiSoyadi: task.adiSoyadi,
      status: task.status,
    });

    return res.status(200).json({
      ok: true,
      taskId: task.id,
      rowNumber: task.rowNumber,
    });
  } catch (e) {
    logError("assistan hatası:", e);
    return res.status(500).json({
      ok: false,
      message: e.message,
    });
  }
});
/* =========================
   WHATSAPP WEBHOOK VERIFY
========================= */
app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    log("Webhook doğrulandı");
    return res.status(200).send(challenge);
  }

  return res.sendStatus(403);
});

/* =========================
   WHATSAPP WEBHOOK RECEIVE
========================= */
app.post("/webhook", async (req, res) => {
  try {
    const event = req.body;

    res.sendStatus(200);

    log("Yeni WhatsApp event geldi:", event);

    if (event.event_type === "message.received") {
      const from = normalizePhone(event?.data?.from);
      const messageType = event?.data?.message_type;

      let msg = "[metin dışı mesaj]";
      if (messageType === 1) {
        msg = event?.data?.message_payload?.text_message?.message || "[boş mesaj]";
      }

      addHistory(from, "in", msg);

      const text =
        `📩 Yeni WhatsApp Mesajı\n\n` +
        `👤 ${getName(from)}\n` +
        `📱 ${from}\n\n` +
        `💬 ${msg}\n\n` +
        `${getHistory(from, 5)}`;

      await sendTelegram(text, buttons(from), TELEGRAM_CHAT_IDS);
    }
  } catch (e) {
    logError("WhatsApp webhook işleme hatası:", e.message);
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
ensureEnv();
loadContacts();
loadHistory();
loadCallTasks();
loadAssistantTasks();
loadCallStats();

setInterval(cleanupStaleState, 10 * 60 * 1000);

app.listen(PORT, "0.0.0.0", () => {
  log(`Server çalışıyor: http://0.0.0.0:${PORT}`);
  startSelfPing();

  startBotPolling(
    "Main bot",
    TELEGRAM_BOT_TOKEN,
    botPollingState.main,
    handleMainBotUpdate
  );

  startBotPolling(
    "Call bot",
    CALL_TELEGRAM_BOT_TOKEN,
    botPollingState.call,
    handleCallBotUpdate
  );

  startBotPolling(
    "Assistant bot",
    ASSISTANT_TELEGRAM_BOT_TOKEN,
    botPollingState.assistant,
    handleAssistantBotUpdate
  );

  startDailyReportScheduler();
});
