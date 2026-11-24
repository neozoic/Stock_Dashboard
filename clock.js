/* ============================================================
   开盘时间指示器：显示距离 A 股 / 美股 开盘的倒计时 + 交易状态
   （原 dashboard.html 内联，2026-11-22 抽成共享文件，
     随"综合选股"二级页一起提供）
   ============================================================ */
(function () {
function updateClocks() {
  const now = new Date();
  const bj = now.toLocaleString("zh-CN", {
    timeZone: "Asia/Shanghai",
    hour12: false,
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
  const ny = now.toLocaleString("en-US", {
    timeZone: "America/New_York",
    hour12: false,
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
  const bjEl = document.getElementById("bj");
  const nyEl = document.getElementById("ny");
  if (bjEl) bjEl.textContent = bj;
  if (nyEl) nyEl.textContent = ny;
}

// Intl.DateTimeFormat 构造昂贵，按时区模块级缓存（心跳每秒调用多次）
const _dtfCache = {};
function _getDtf(tz) {
  if (!_dtfCache[tz]) {
    _dtfCache[tz] = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz, hour12: false,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
      weekday: "short",
    });
  }
  return _dtfCache[tz];
}

function nextOpenCountdown(now, tz, openHour, openMinute, sessions, extra) {
  extra = extra || {};
  const pre = extra.pre || [];
  const after = extra.after || [];
  const overnight = extra.overnight || [];
  const workdays = extra.workdays || [1, 2, 3, 4, 5];

  const localFmt = _getDtf(tz);
  const parts = localFmt.formatToParts(now).reduce((acc, p) => {
    if (p.type !== "literal") acc[p.type] = p.value;
    return acc;
  }, {});
  const y = parseInt(parts.year, 10);
  const mo = parseInt(parts.month, 10) - 1;
  const d = parseInt(parts.day, 10);
  const h = parseInt(parts.hour === "24" ? "0" : parts.hour, 10);
  const mi = parseInt(parts.minute, 10);
  const s = parseInt(parts.second, 10);
  const wdMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  const wd = wdMap[parts.weekday];
  const curMin = h * 60 + mi;
  const isWorkday = workdays.includes(wd);

  const inSess = (s) => curMin >= s[0] * 60 + s[1] && curMin < s[2] * 60 + s[3];

  if (!isWorkday) {
    return _calcNextOpen(now, tz, parts, wdMap, workdays, y, mo, d, curMin, s, openHour, openMinute);
  }

  // 盘前(早于当日首个 session 起点):统一显示"距开盘 N 分钟",
  // 不再 fall through 到 _calcNextOpen(否则会算下个工作日,显示 24h+)
  if (sessions.length > 0) {
    const sortedSess = [...sessions].sort((a, b) => (a[0] * 60 + a[1]) - (b[0] * 60 + b[1]));
    const firstStart = sortedSess[0][0] * 60 + sortedSess[0][1];
    if (curMin < firstStart) {
      // 用 nextOpenCountdown 自己的 firstStart 算今天开盘 ms
      const nextUtc = _composeNextUtc(parts, wdMap, workdays, now, firstStart, openHour, openMinute, tz);
      if (nextUtc === null) {
        return _calcNextOpen(now, tz, parts, wdMap, workdays, y, mo, d, curMin, s, openHour, openMinute);
      }
      const remainMs = nextUtc - now.getTime();
      return { state: "closed", text: `距开盘 ${fmtDuration(remainMs)}` };
    }
  }

  if (overnight.length > 0) {
    const o1 = overnight[0];
    if (curMin >= o1[0] * 60 + o1[1] && curMin < o1[2] * 60 + o1[3]) {
      const remain = (o1[2] * 60 + o1[3] - curMin) * 60 - s;
      return { state: "overnight", text: `夜盘 · 剩 ${fmtDuration(remain * 1000)}` };
    }
    if (overnight.length > 1) {
      const o2 = overnight[1];
      if (curMin >= o2[0] * 60 + o2[1] && curMin < o2[2] * 60 + o2[3]) {
        const remain = (o2[2] * 60 + o2[3] - curMin) * 60 - s;
        return { state: "overnight", text: `夜盘 · 剩 ${fmtDuration(remain * 1000)}` };
      }
    }
  }

  if (pre.length > 0 && pre.some(inSess)) {
    const sess = pre.find(inSess);
    const remain = (sess[2] * 60 + sess[3] - curMin) * 60 - s;
    return { state: "pre", text: `盘前 · 剩 ${fmtDuration(remain * 1000)}` };
  }

  for (const sess of sessions) {
    if (inSess(sess)) {
      const remain = (sess[2] * 60 + sess[3] - curMin) * 60 - s;
      return { state: "open", text: `盘中 · 剩 ${fmtDuration(remain * 1000)}` };
    }
  }

  if (after.length > 0 && after.some(inSess)) {
    const sess = after.find(inSess);
    const remain = (sess[2] * 60 + sess[3] - curMin) * 60 - s;
    return { state: "after", text: `盘后 · 剩 ${fmtDuration(remain * 1000)}` };
  }

  const sortedSessions = [...sessions].sort((a, b) => (a[0] * 60 + a[1]) - (b[0] * 60 + b[1]));
  for (let i = 0; i < sortedSessions.length - 1; i++) {
    const endEnd = sortedSessions[i][2] * 60 + sortedSessions[i][3];
    const nextStart = sortedSessions[i + 1][0] * 60 + sortedSessions[i + 1][1];
    if (curMin >= endEnd && curMin < nextStart) {
      const remain = (nextStart - curMin) * 60 - s;
      return { state: "between", text: `午休 · 距开盘 ${fmtDuration(remain * 1000)}` };
    }
  }

  return _calcNextOpen(now, tz, parts, wdMap, workdays, y, mo, d, curMin, s, openHour, openMinute);
}

function _composeNextUtc(parts, wdMap, workdays, now, targetStartMin, openHour, openMinute, tz) {
  // 构造"目标工作日"的"年-月-日 在 tz 下的 09:30:00"对应的 UTC 毫秒。
  // 关键:不把 gSec 加进去,否则 nextUtc 会随 now 的秒数同步偏移,导致 remain 永远不变。
  const targetFmt = _getDtf(tz);
  const localParts = (date) => targetFmt.formatToParts(date).reduce((acc, p) => {
    if (p.type !== "literal") acc[p.type] = p.value;
    return acc;
  }, {});

  // 从 n=0（今天）开始：盘前场景需要"今天 09:30"；nextUtc > now 的判断会排除过去时刻
  for (let n = 0; n <= 8; n++) {
    const guess = new Date(now.getTime() + n * 24 * 3600 * 1000);
    const gp = localParts(guess);
    if (!workdays.includes(wdMap[gp.weekday])) continue;
    // 构造 guess 在 tz 下的"年-月-日"对应的 00:00:00 UTC 时刻
    const localDayStartUtc = Date.UTC(
      parseInt(gp.year), parseInt(gp.month) - 1, parseInt(gp.day),
      0, 0, 0
    );
    // 推算 tz 偏移:guess 在 tz 下显示为某天某时某分某秒,反推 tz 偏移
    const tzOffset = guess.getTime() - Date.UTC(
      parseInt(gp.year), parseInt(gp.month) - 1, parseInt(gp.day),
      parseInt(gp.hour === "24" ? "0" : gp.hour, 10),
      parseInt(gp.minute, 10),
      parseInt(gp.second, 10)
    );
    // tz 下"目标日 targetStartMin:00"对应的 UTC 时刻
    const targetH = Math.floor(targetStartMin / 60);
    const targetM = targetStartMin % 60;
    const nextUtc = localDayStartUtc + tzOffset + targetH * 3600 * 1000 + targetM * 60 * 1000;
    if (nextUtc > now.getTime()) {
      return nextUtc;
    }
  }
  return null;
}

function _calcNextOpen(now, tz, parts, wdMap, workdays, y, mo, d, curMin, s, openHour, openMinute) {
  // 默认找下个工作日 09:30 开市
  const nextUtc = _composeNextUtc(parts, wdMap, workdays, now, openHour * 60 + openMinute, openHour, openMinute, tz);
  if (nextUtc === null) {
    return { state: "closed", text: "已收盘" };
  }
  const remain = nextUtc - now.getTime();
  return { state: "closed", text: `距开盘 ${fmtDuration(remain)}` };
}

function fmtDuration(ms) {
  if (ms <= 0) return "00:00:00";
  const total = Math.floor(ms / 1000);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  return [h, m, s].map(v => String(v).padStart(2, "0")).join(":");
}

const STATE_LABEL = {
  closed:    { cn: "CN 休市", us: "US 休市", open: false },
  pre:       { cn: "CN 盘前", us: "US 盘前", open: false },
  open:      { cn: "CN 交易中", us: "US 交易中", open: true },
  after:     { cn: "CN 盘后", us: "US 盘后", open: false },
  overnight: { cn: "CN 已收盘", us: "US 夜盘", open: false },
  between:   { cn: "CN 午休", us: "US 午休", open: false },
};

const CN_CFG = {
  tz: "Asia/Shanghai", openHour: 9, openMinute: 30,
  sessions: [[9, 30, 11, 30], [13, 0, 15, 0]],
  pre: [], after: [], overnight: [],
  workdays: [1, 2, 3, 4, 5],
};

const US_CFG = {
  tz: "America/New_York", openHour: 9, openMinute: 30,
  sessions: [[9, 30, 16, 0]],
  pre:       [[4, 0, 9, 30]],
  after:     [[16, 0, 20, 0]],
  overnight: [[20, 0, 24, 0], [0, 0, 4, 0]],
  workdays: [1, 2, 3, 4, 5],
};

function updateOpenCountdown() {
  const now = new Date();
  const cn = nextOpenCountdown(now, CN_CFG.tz, CN_CFG.openHour, CN_CFG.openMinute, CN_CFG.sessions, CN_CFG);
  const us = nextOpenCountdown(now, US_CFG.tz, US_CFG.openHour, US_CFG.openMinute, US_CFG.sessions, US_CFG);

  const cnEl = document.getElementById("cn-open");
  const usEl = document.getElementById("us-open");
  if (cnEl) {
    cnEl.textContent = cn.text;
    cnEl.parentElement.classList.toggle("is-open", STATE_LABEL[cn.state]?.open ?? false);
  }
  if (usEl) {
    usEl.textContent = us.text;
    usEl.parentElement.classList.toggle("is-open", STATE_LABEL[us.state]?.open ?? false);
  }
}

function updateTradingStates() {
  const now = new Date();
  const cn = nextOpenCountdown(now, CN_CFG.tz, CN_CFG.openHour, CN_CFG.openMinute, CN_CFG.sessions, CN_CFG);
  const us = nextOpenCountdown(now, US_CFG.tz, US_CFG.openHour, US_CFG.openMinute, US_CFG.sessions, US_CFG);

  const apply = (id, info) => {
    const wrap = document.getElementById(id);
    if (!wrap) return;
    const textEl = wrap.querySelector(".state-text");
    if (textEl) textEl.textContent = ` ${info.label}`;
    wrap.parentElement?.classList.toggle("is-open", STATE_LABEL[info.state]?.open ?? false);
  };

  apply("cn-state", { state: cn.state, label: STATE_LABEL[cn.state]?.cn || "CN 休市" });
  apply("us-state", { state: us.state, label: STATE_LABEL[us.state]?.us || "US 休市" });
}

  // ---- 自启动 ----
  updateClocks();
  updateOpenCountdown();
  updateTradingStates();
  setInterval(function () {
    updateClocks();
    updateOpenCountdown();
    updateTradingStates();
  }, 1000);
})();
