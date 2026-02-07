const metricsEl = document.getElementById("metrics");
const sessionsEl = document.getElementById("sessions");
const modelsEl = document.getElementById("models");
const statusEl = document.getElementById("status");
const detailEl = document.getElementById("detail");
const sessionCountEl = document.getElementById("session-count");
const periodEl = document.getElementById("period");
const sortSelect = document.getElementById("sort-select");
const sortDirSelect = document.getElementById("sort-dir-select");
const statusSelect = document.getElementById("status-select");
const overviewViewEl = document.getElementById("overview-view");
const sessionViewEl = document.getElementById("session-view");
const sessionBreadcrumbEl = document.getElementById("session-breadcrumb");
const backButton = document.getElementById("back-button");

const gradient = ["#B6512B", "#D96840", "#FF7A45", "#FF8F4D", "#FFB25A"];
const modelColors = {
  claude: {
    opus: "#D97BC1",
    sonnet: "#B656B1",
    haiku: "#8E3F8A",
  },
  openai: {
    "gpt-4o": "#7DD9FF",
    "gpt-4": "#4EB1E9",
    "gpt-4o-mini": "#3889B8",
    "gpt-3.5": "#2A6588",
  },
  google: {
    "gemini-2.0": "#7DD9FF",
    "gemini-1.5-pro": "#4EB1E9",
    "gemini-1.5": "#3889B8",
    gemma: "#2A6588",
  },
};

const filters = {
  sort: "cost",
  sortDir: "desc",
  status: "",
  period: "30d",
  periodEnabled: false,
};

let overviewState = null;
let sessionDetailState = null;
let selectedSessionId = null;
let selectedMessageIndex = 0;
let sessionIndex = 0;
let currentView = "overview";

const formatCost = (value) => `$${value.toFixed(3)}`;
const formatTokens = (value) => {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return `${value}`;
};
const formatPercent = (value) => `${Math.round(value * 100)}%`;
const formatDuration = (valueNs) => {
  const seconds = Math.floor(valueNs / 1e9);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const min = minutes % 60;
  const sec = seconds % 60;
  if (hours > 0) return `${hours}h ${min}m`;
  if (minutes > 0) return `${min}m ${sec}s`;
  return `${sec}s`;
};

const compareValues = (current, previous, invert) => {
  if (!previous || previous === 0) return null;
  const change = ((current - previous) / previous) * 100;
  const direction = change >= 0 ? "up" : "down";
  const positive = invert ? change <= 0 : change >= 0;
  return {
    value: `${Math.abs(change).toFixed(1)}%`,
    direction,
    positive,
  };
};

const statusClass = (status) => {
  if (status === "completed") return "completed";
  if (status === "failed") return "failed";
  return "abandoned";
};

const colorForModel = (name) => {
  if (!name) return "#FF6B4A";
  const value = name.toLowerCase();
  for (const [tier, color] of Object.entries(modelColors.claude)) {
    if (value.includes(tier)) return color;
  }
  for (const [model, color] of Object.entries(modelColors.openai)) {
    if (value.includes(model.replace(/-/g, "")) || value.includes(model)) return color;
  }
  for (const [model, color] of Object.entries(modelColors.google)) {
    if (value.includes(model.replace(/-/g, "")) || value.includes(model)) return color;
  }
  return "#FF6B4A";
};

const costScale = (sessions) => {
  let min = Infinity;
  let max = 0;
  sessions.forEach((session) => {
    if (session.total_cost < min) min = session.total_cost;
    if (session.total_cost > max) max = session.total_cost;
  });
  if (!Number.isFinite(min)) min = 0;
  if (max <= min) max = min + 1;
  return { min, max };
};

const costColor = (value, min, max) => {
  const ratio = Math.min(Math.max((value - min) / (max - min), 0), 1);
  const index = Math.min(gradient.length - 1, Math.floor(ratio * (gradient.length - 1)));
  return gradient[index];
};

const buildParams = () => {
  const params = new URLSearchParams();
  if (filters.sort) params.set("sort", filters.sort);
  if (filters.sortDir) params.set("sort_dir", filters.sortDir);
  if (filters.status) params.set("status", filters.status);
  if (filters.periodEnabled && filters.period) params.set("since", filters.period);
  return params.toString();
};

const renderPeriodControls = () => {
  periodEl.innerHTML = "";
  const periods = [
    { label: "30d", value: "30d" },
    { label: "3M", value: "90d" },
    { label: "6M", value: "180d" },
  ];
  periods.forEach((period) => {
    const button = document.createElement("button");
    button.className = "period__button";
    if (filters.period === period.value) {
      button.classList.add("period__button--active");
    }
    button.textContent = period.label;
    button.addEventListener("click", () => {
      filters.period = period.value;
      filters.periodEnabled = true;
      renderPeriodControls();
      loadOverview();
    });
    periodEl.appendChild(button);
  });
};

const renderMetrics = (data) => {
  metricsEl.innerHTML = "";

  const items = [
    {
      label: "total spend",
      value: formatCost(data.total_cost),
      sub: `avg ${formatCost(data.total_cost / Math.max(data.sessions.length, 1))}`,
      comparison: data.previous_period
        ? compareValues(data.total_cost, data.previous_period.total_cost, true)
        : null,
    },
    {
      label: "tokens used",
      value: formatTokens(data.total_tokens),
      sub: `${formatTokens(data.input_tokens)} in / ${formatTokens(data.output_tokens)} out`,
      comparison: data.previous_period
        ? compareValues(data.total_tokens, data.previous_period.total_tokens, false)
        : null,
    },
    {
      label: "agent time",
      value: formatDuration(data.total_duration_ns),
      sub: `${formatDuration(data.total_duration_ns / Math.max(data.sessions.length, 1))} avg`,
      comparison: data.previous_period
        ? compareValues(data.total_duration_ns, data.previous_period.total_duration_ns, false)
        : null,
    },
    {
      label: "tool calls",
      value: data.total_tool_calls,
      sub: `${(data.total_tool_calls / Math.max(data.sessions.length, 1)).toFixed(1)} avg`,
      comparison: data.previous_period
        ? compareValues(data.total_tool_calls, data.previous_period.total_tool_calls, false)
        : null,
    },
    {
      label: "success rate",
      value: formatPercent(data.success_rate),
      sub: `${data.completed}/${data.sessions.length} complete`,
      comparison: data.previous_period
        ? compareValues(data.success_rate, data.previous_period.success_rate, false)
        : null,
    },
  ];

  items.forEach((item) => {
    const card = document.createElement("div");
    card.className = "metric";

    const label = document.createElement("div");
    label.className = "metric__label";
    label.textContent = item.label;

    const value = document.createElement("div");
    value.className = "metric__value";
    value.textContent = item.value;

    card.appendChild(label);
    card.appendChild(value);

    if (item.comparison) {
      const compare = document.createElement("div");
      compare.className = "metric__compare";
      compare.classList.add(item.comparison.positive ? "metric__compare--up" : "metric__compare--down");
      compare.textContent = `${item.comparison.direction === "up" ? "^" : "v"} ${item.comparison.value} vs prev`;
      card.appendChild(compare);
    }

    const sub = document.createElement("div");
    sub.className = "metric__sub";
    sub.textContent = item.sub;
    card.appendChild(sub);

    metricsEl.appendChild(card);
  });
};

const renderModels = (data) => {
  modelsEl.innerHTML = "";
  const models = Object.values(data.cost_by_model || {}).sort((a, b) => b.total_cost - a.total_cost);
  if (models.length === 0) {
    modelsEl.textContent = "no model cost data";
    return;
  }

  const max = models[0].total_cost || 1;
  models.forEach((model) => {
    const row = document.createElement("div");
    row.className = "model-row";

    const name = document.createElement("div");
    name.className = "model-row__name";
    name.textContent = model.model;
    name.style.color = colorForModel(model.model);

    const bar = document.createElement("div");
    bar.className = "model-row__bar";
    const fill = document.createElement("div");
    fill.className = "model-row__fill";
    fill.style.width = `${Math.round((model.total_cost / max) * 100)}%`;
    fill.style.background = `linear-gradient(90deg, ${colorForModel(model.model)}, #FFB25A)`;
    bar.appendChild(fill);

    const meta = document.createElement("div");
    meta.className = "model-row__meta";
    meta.textContent = `${formatCost(model.total_cost)} / ${model.session_count} sessions`;

    row.appendChild(name);
    row.appendChild(bar);
    row.appendChild(meta);
    modelsEl.appendChild(row);
  });
};

const renderStatus = (data) => {
  statusEl.innerHTML = "";
  const total = Math.max(data.sessions.length, 1);
  const completedPct = (data.completed / total) * 100;
  const failedPct = (data.failed / total) * 100;
  const abandonedPct = (data.abandoned / total) * 100;

  const container = document.createElement("div");
  container.className = "status-summary";

  const bar = document.createElement("div");
  bar.className = "status__bar";

  const completed = document.createElement("span");
  completed.className = "status__segment--completed";
  completed.style.width = `${completedPct}%`;

  const failed = document.createElement("span");
  failed.className = "status__segment--failed";
  failed.style.width = `${failedPct}%`;

  const abandoned = document.createElement("span");
  abandoned.className = "status__segment--abandoned";
  abandoned.style.width = `${abandonedPct}%`;

  bar.appendChild(completed);
  bar.appendChild(failed);
  bar.appendChild(abandoned);

  const legend = document.createElement("div");
  legend.className = "status__legend";

  const legendItems = [
    { label: "completed", value: completedPct, count: data.completed, className: "status__dot--completed" },
    { label: "failed", value: failedPct, count: data.failed, className: "status__dot--failed" },
    { label: "abandoned", value: abandonedPct, count: data.abandoned, className: "status__dot--abandoned" },
  ];

  legendItems.forEach((item) => {
    const entry = document.createElement("div");
    entry.className = "status__legend-item";
    const dot = document.createElement("span");
    dot.className = `status__dot ${item.className}`;
    const text = document.createElement("span");
    text.textContent = `${item.label} ${item.value.toFixed(0)}% (${item.count})`;
    entry.appendChild(dot);
    entry.appendChild(text);
    legend.appendChild(entry);
  });

  const efficiency = document.createElement("div");
  const tokensPerMinute = data.total_duration_ns > 0
    ? Math.round((data.total_tokens / (data.total_duration_ns / 1e9)) * 60)
    : 0;
  const costPerSession = data.total_cost / Math.max(data.sessions.length, 1);
  efficiency.className = "model-row__meta";
  efficiency.textContent = `eff: ${formatCost(costPerSession)} per sess | ${tokensPerMinute} tok/min`;

  container.appendChild(bar);
  container.appendChild(legend);
  container.appendChild(efficiency);
  statusEl.appendChild(container);
};

const renderSessions = (data) => {
  sessionsEl.innerHTML = "";
  if (!data.sessions.length) {
    sessionsEl.textContent = "no sessions";
    return;
  }

  const selectedIndex = data.sessions.findIndex((session) => session.id === selectedSessionId);
  if (selectedIndex >= 0) {
    sessionIndex = selectedIndex;
  }

  const header = document.createElement("div");
  header.className = "table__row table__row--header";
  header.innerHTML =
    "<div></div><div>label</div><div>model</div><div>dur</div><div>tokens</div><div>in/out</div><div>cost</div><div>tools</div><div>msgs</div><div>status</div>";
  sessionsEl.appendChild(header);

  const scale = costScale(data.sessions);
  data.sessions.forEach((session, index) => {
    const row = document.createElement("div");
    row.className = "table__row";
    if (session.id === selectedSessionId) {
      row.classList.add("table__row--active");
    }

    const number = document.createElement("div");
    number.className = "session-number";
    number.textContent = (index + 1).toString(16).padStart(2, "0");

    const label = document.createElement("div");
    label.textContent = session.label;

    const model = document.createElement("div");
    model.className = "session-model";
    model.textContent = session.model || "unknown";
    model.style.color = colorForModel(session.model);

    const duration = document.createElement("div");
    duration.textContent = formatDuration(session.duration_ns);

    const tokens = document.createElement("div");
    tokens.textContent = formatTokens(session.input_tokens + session.output_tokens);

    const barbell = document.createElement("div");
    barbell.className = "barbell";
    const leftDot = document.createElement("span");
    leftDot.className = "barbell__dot barbell__dot--in";
    const bar = document.createElement("span");
    bar.className = "barbell__bar";
    const totalCost = session.input_cost + session.output_cost || 1;
    const inputPct = Math.round((session.input_cost / totalCost) * 100);
    bar.style.setProperty("--in", `${inputPct}%`);
    const rightDot = document.createElement("span");
    rightDot.className = "barbell__dot barbell__dot--out";
    barbell.appendChild(leftDot);
    barbell.appendChild(bar);
    barbell.appendChild(rightDot);

    const cost = document.createElement("div");
    cost.className = "cost-indicator";
    const dot = document.createElement("span");
    dot.className = "cost-indicator__dot";
    dot.style.background = costColor(session.total_cost, scale.min, scale.max);
    const value = document.createElement("span");
    value.textContent = formatCost(session.total_cost);
    cost.appendChild(dot);
    cost.appendChild(value);

    const tools = document.createElement("div");
    tools.textContent = session.tool_calls;

    const msgs = document.createElement("div");
    msgs.textContent = session.message_count;

    const status = document.createElement("div");
    status.className = "status";
    const statusDot = document.createElement("span");
    statusDot.className = `status__dot status__dot--${statusClass(session.status)}`;
    const statusText = document.createElement("span");
    statusText.textContent = session.status;
    status.appendChild(statusDot);
    status.appendChild(statusText);

    row.appendChild(number);
    row.appendChild(label);
    row.appendChild(model);
    row.appendChild(duration);
    row.appendChild(tokens);
    row.appendChild(barbell);
    row.appendChild(cost);
    row.appendChild(tools);
    row.appendChild(msgs);
    row.appendChild(status);

    row.addEventListener("click", () => loadSession(session.id));
    sessionsEl.appendChild(row);
  });
};

const renderDetailMetrics = (detail) => {
  const container = document.createElement("div");
  container.className = "detail__metrics";

  let avgCost = 0;
  let avgDuration = 0;
  let avgTokens = 0;
  let avgToolCalls = 0;
  if (overviewState && overviewState.sessions.length) {
    const total = overviewState.sessions.length;
    overviewState.sessions.forEach((session) => {
      avgCost += session.total_cost;
      avgDuration += session.duration_ns;
      avgTokens += session.input_tokens + session.output_tokens;
      avgToolCalls += session.tool_calls;
    });
    avgCost /= total;
    avgDuration /= total;
    avgTokens /= total;
    avgToolCalls /= total;
  }

  const totalTokens = detail.summary.input_tokens + detail.summary.output_tokens;
  const tokenSplit = totalTokens ? (detail.summary.input_tokens / totalTokens) * 100 : 50;

  const metrics = [
    {
      label: "total cost",
      value: formatCost(detail.summary.total_cost),
      sub: avgCost ? `${formatCost(avgCost)} avg` : "no avg",
      change: avgCost ? compareValues(detail.summary.total_cost, avgCost, true) : null,
    },
    {
      label: "tokens used",
      value: formatTokens(totalTokens),
      sub: `${formatTokens(detail.summary.input_tokens)} in / ${formatTokens(detail.summary.output_tokens)} out`,
      change: avgTokens ? compareValues(totalTokens, avgTokens, false) : null,
      tokenSplit,
    },
    {
      label: "agent time",
      value: formatDuration(detail.summary.duration_ns),
      sub: avgDuration ? `${formatDuration(avgDuration)} avg` : "no avg",
      change: avgDuration ? compareValues(detail.summary.duration_ns, avgDuration, false) : null,
    },
    {
      label: "tool calls",
      value: detail.summary.tool_calls,
      sub: avgToolCalls ? `${avgToolCalls.toFixed(1)} avg` : "no avg",
      change: avgToolCalls ? compareValues(detail.summary.tool_calls, avgToolCalls, false) : null,
    },
  ];

  metrics.forEach((metric) => {
    const card = document.createElement("div");
    card.className = "detail__metric";

    const label = document.createElement("div");
    label.className = "detail__metric-label";
    label.textContent = metric.label;

    const value = document.createElement("div");
    value.className = "detail__metric-value";
    value.textContent = metric.value;

    const sub = document.createElement("div");
    sub.className = "detail__metric-sub";
    sub.textContent = metric.sub;

    card.appendChild(label);
    card.appendChild(value);

    if (metric.change) {
      const change = document.createElement("div");
      change.className = "metric__compare";
      change.classList.add(metric.change.positive ? "metric__compare--up" : "metric__compare--down");
      change.textContent = `${metric.change.direction === "up" ? "^" : "v"} ${metric.change.value} vs avg`;
      card.appendChild(change);
    }

    card.appendChild(sub);

    if (metric.label === "tokens used") {
      const bar = document.createElement("div");
      bar.className = "detail__token-bar";
      const fill = document.createElement("span");
      fill.style.width = `${metric.tokenSplit || 50}%`;
      bar.appendChild(fill);
      card.appendChild(bar);
    }

    container.appendChild(card);
  });

  return container;
};

const renderTimeline = (detail) => {
  const wrapper = document.createElement("div");
  wrapper.className = "timeline";

  const title = document.createElement("div");
  title.className = "timeline__title";
  title.textContent = "conversation timeline";

  const chart = document.createElement("div");
  chart.className = "timeline__chart";

  const maxDelta = Math.max(...detail.messages.map((msg) => msg.delta_ns || 0), 1);
  detail.messages.forEach((msg) => {
    const bar = document.createElement("div");
    bar.className = "timeline__bar";
    if (msg.role === "user") {
      bar.classList.add("timeline__bar--user");
    }
    const height = Math.max((msg.delta_ns || 0) / maxDelta, 0.1) * 100;
    bar.style.setProperty("--height", `${height}%`);
    chart.appendChild(bar);
  });

  wrapper.appendChild(title);
  wrapper.appendChild(chart);
  return wrapper;
};

const renderConversation = (detail) => {
  const container = document.createElement("div");
  container.className = "detail__conversation";

  const table = document.createElement("div");
  table.className = "conversation__table";

  const header = document.createElement("div");
  header.className = "conversation__row conversation__row--header";
  header.innerHTML = "<div>time</div><div>role</div><div>tokens</div><div>cost</div>";
  table.appendChild(header);

  detail.messages.forEach((msg, idx) => {
    const row = document.createElement("div");
    row.className = "conversation__row";
    if (idx === selectedMessageIndex) {
      row.classList.add("conversation__row--active");
    }

    const time = document.createElement("div");
    time.textContent = new Date(msg.timestamp).toLocaleTimeString();

    const role = document.createElement("div");
    role.textContent = msg.role;
    role.style.color = msg.role === "user" ? "#4EB1E9" : "#FF6B4A";

    const tokens = document.createElement("div");
    tokens.textContent = formatTokens(msg.total_tokens);

    const cost = document.createElement("div");
    cost.textContent = formatCost(msg.total_cost);

    row.appendChild(time);
    row.appendChild(role);
    row.appendChild(tokens);
    row.appendChild(cost);
    row.addEventListener("click", () => {
      selectedMessageIndex = idx;
      renderSessionDetail(detail);
    });
    table.appendChild(row);
  });

  const detailPane = document.createElement("div");
  detailPane.className = "conversation__detail";
  const msg = detail.messages[selectedMessageIndex] || detail.messages[0];

  const meta = document.createElement("div");
  meta.className = "conversation__meta";
  const metaItems = [
    { label: "role", value: msg.role },
    { label: "model", value: msg.model || "unknown" },
    { label: "tokens", value: formatTokens(msg.total_tokens) },
    { label: "cost", value: formatCost(msg.total_cost) },
  ];
  metaItems.forEach((item) => {
    const block = document.createElement("div");
    block.textContent = item.label;
    const span = document.createElement("span");
    span.textContent = item.value;
    block.appendChild(span);
    meta.appendChild(block);
  });

  const tools = document.createElement("div");
  tools.className = "conversation__meta";
  const toolBlock = document.createElement("div");
  toolBlock.textContent = "tools";
  const toolValue = document.createElement("span");
  toolValue.textContent = msg.tool_calls && msg.tool_calls.length ? msg.tool_calls.join(", ") : "none";
  toolBlock.appendChild(toolValue);
  tools.appendChild(toolBlock);

  const text = document.createElement("div");
  text.className = "conversation__text";
  text.textContent = msg.text || "";

  detailPane.appendChild(meta);
  detailPane.appendChild(tools);
  detailPane.appendChild(text);

  container.appendChild(table);
  container.appendChild(detailPane);
  return container;
};

const renderSessionDetail = (detail) => {
  detailEl.innerHTML = "";

  const title = document.createElement("div");
  title.className = "panel__title";
  title.textContent = "session detail";

  const header = document.createElement("div");
  header.className = "detail__header";
  const headerText = document.createElement("div");
  const headerTitle = document.createElement("div");
  headerTitle.className = "detail__title";
  headerTitle.textContent = detail.summary.label;
  const headerSub = document.createElement("div");
  headerSub.className = "detail__subtitle";
  headerSub.textContent = detail.summary.id;
  headerText.appendChild(headerTitle);
  headerText.appendChild(headerSub);

  const status = document.createElement("div");
  status.className = "detail__status";
  const dot = document.createElement("span");
  dot.className = `status__dot status__dot--${statusClass(detail.summary.status)}`;
  const statusText = document.createElement("span");
  statusText.textContent = detail.summary.status;
  status.appendChild(dot);
  status.appendChild(statusText);

  header.appendChild(headerText);
  header.appendChild(status);

  detailEl.appendChild(title);
  detailEl.appendChild(header);
  detailEl.appendChild(renderDetailMetrics(detail));
  detailEl.appendChild(renderTimeline(detail));
  detailEl.appendChild(renderConversation(detail));
};

const loadOverview = async () => {
  const res = await fetch(`/api/overview?${buildParams()}`);
  const data = await res.json();
  overviewState = data;
  const filterBits = [];
  if (filters.status) filterBits.push(`status ${filters.status}`);
  if (filters.periodEnabled) {
    const label = filters.period === "90d" ? "3M" : filters.period === "180d" ? "6M" : "30d";
    filterBits.push(`period ${label}`);
  }
  const filterText = filterBits.length ? ` | ${filterBits.join(" | ")}` : "";
  sessionCountEl.textContent = `${data.sessions.length} sessions | ${formatDuration(data.total_duration_ns)} total${filterText}`;
  renderPeriodControls();
  renderMetrics(data);
  renderModels(data);
  renderStatus(data);
  renderSessions(data);
  if (selectedSessionId) {
    loadSession(selectedSessionId, true);
  } else if (data.sessions.length) {
    sessionIndex = Math.min(sessionIndex, data.sessions.length - 1);
  }
};

const loadSession = async (sessionId, keepMessage) => {
  selectedSessionId = sessionId;
  const res = await fetch(`/api/session/${sessionId}`);
  const data = await res.json();
  sessionDetailState = data;
  if (!keepMessage) {
    selectedMessageIndex = 0;
  }
  renderSessionDetail(data);
  if (overviewState) {
    renderSessions(overviewState);
  }
  sessionBreadcrumbEl.textContent = `tapes > ${data.summary.label}`;
  setView("session");
  if (window.location.pathname !== `/session/${sessionId}`) {
    window.history.pushState({}, "", `/session/${sessionId}`);
  }
};

const setView = (view) => {
  currentView = view;
  if (view === "session") {
    overviewViewEl.hidden = true;
    sessionViewEl.hidden = false;
  } else {
    overviewViewEl.hidden = false;
    sessionViewEl.hidden = true;
  }
};

const backToOverview = () => {
  selectedSessionId = null;
  selectedMessageIndex = 0;
  sessionDetailState = null;
  detailEl.innerHTML = "";
  setView("overview");
  window.history.pushState({}, "", "/");
  if (overviewState) {
    renderSessions(overviewState);
  }
};

const moveSession = (delta) => {
  if (!overviewState || !overviewState.sessions.length) return;
  sessionIndex = Math.min(Math.max(sessionIndex + delta, 0), overviewState.sessions.length - 1);
  const session = overviewState.sessions[sessionIndex];
  selectedSessionId = session.id;
  renderSessions(overviewState);
  const rows = sessionsEl.querySelectorAll(".table__row");
  if (rows[sessionIndex + 1]) {
    rows[sessionIndex + 1].scrollIntoView({ block: "nearest" });
  }
};

const moveMessage = (delta) => {
  if (!detailEl || !detailEl.querySelectorAll) return;
  const rows = detailEl.querySelectorAll(".conversation__row");
  if (!rows.length) return;
  const maxIndex = rows.length - 2;
  selectedMessageIndex = Math.min(Math.max(selectedMessageIndex + delta, 0), maxIndex);
  const row = rows[selectedMessageIndex + 1];
  if (row) {
    row.scrollIntoView({ block: "nearest" });
    row.click();
  }
};

const handleKey = (event) => {
  if (event.target && ["INPUT", "SELECT", "TEXTAREA"].includes(event.target.tagName)) {
    return;
  }
  switch (event.key) {
    case "j":
    case "ArrowDown":
      if (currentView === "session") {
        moveMessage(1);
      } else {
        moveSession(1);
      }
      break;
    case "k":
    case "ArrowUp":
      if (currentView === "session") {
        moveMessage(-1);
      } else {
        moveSession(-1);
      }
      break;
    case "Enter":
      if (currentView === "overview" && overviewState && overviewState.sessions[sessionIndex]) {
        loadSession(overviewState.sessions[sessionIndex].id);
      }
      break;
    case "h":
      if (currentView === "session") {
        backToOverview();
      }
      break;
    case "s":
      if (currentView === "overview") {
        sortSelect.focus();
      }
      break;
    case "f":
      if (currentView === "overview") {
        statusSelect.focus();
      }
      break;
    case "p":
      if (currentView === "overview") {
        filters.periodEnabled = true;
        filters.period = filters.period === "30d" ? "90d" : filters.period === "90d" ? "180d" : "30d";
        renderPeriodControls();
        loadOverview();
      }
      break;
    default:
      break;
  }
};

const parseUrlFilters = () => {
  const params = new URLSearchParams(window.location.search);
  if (params.get("sort")) filters.sort = params.get("sort");
  if (params.get("sort_dir")) filters.sortDir = params.get("sort_dir");
  if (params.get("status")) filters.status = params.get("status");
  if (params.get("since")) {
    filters.period = params.get("since");
    filters.periodEnabled = true;
  }
};

parseUrlFilters();
sortSelect.value = filters.sort;
sortDirSelect.value = filters.sortDir;
statusSelect.value = filters.status;

sortSelect.addEventListener("change", () => {
  filters.sort = sortSelect.value;
  loadOverview();
});
sortDirSelect.addEventListener("change", () => {
  filters.sortDir = sortDirSelect.value;
  loadOverview();
});
statusSelect.addEventListener("change", () => {
  filters.status = statusSelect.value;
  loadOverview();
});

loadOverview().catch((err) => {
  sessionCountEl.textContent = "failed to load data";
  console.error(err);
});

window.addEventListener("keydown", handleKey);
backButton.addEventListener("click", backToOverview);

window.addEventListener("popstate", () => {
  if (window.location.pathname.startsWith("/session/")) {
    const sessionId = window.location.pathname.replace("/session/", "");
    if (sessionId) {
      loadSession(sessionId, true);
      return;
    }
  }
  backToOverview();
});

if (window.location.pathname.startsWith("/session/")) {
  const sessionId = window.location.pathname.replace("/session/", "");
  if (sessionId) {
    loadSession(sessionId);
  }
} else {
  setView("overview");
}
