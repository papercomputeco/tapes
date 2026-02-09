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
const statusLabelEl = document.getElementById("status-label");
const sessionsMoreEl = document.getElementById("sessions-more");
const showMoreButton = document.getElementById("show-more-button");
const sessionsFiltersEl = document.getElementById("sessions-filters");

const modelColors = {
  claude: {
    opus: "#f472b6",
    sonnet: "#f472b6",
    haiku: "#38bdf8",
  },
  openai: {
    "gpt-4o": "#4ade80",
    "gpt-4": "#4ade80",
    "gpt-4o-mini": "#4ade80",
    "gpt-3.5": "#4ade80",
  },
  google: {
    "gemini-2.0": "#fb923c",
    "gemini-1.5-pro": "#fb923c",
    "gemini-1.5": "#fb923c",
    gemma: "#fb923c",
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
let visibleSessionCount = 38;
let filtersVisible = false;

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
  if (!name) return "#f472b6";
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
  return "#f472b6";
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
  const sessionCount = Math.max(data.sessions.length, 1);

  const items = [
    {
      label: "total spend",
      value: formatCost(data.total_cost),
      sub: `avg ${formatCost(data.total_cost / sessionCount)} / sess`,
      comparison: data.previous_period
        ? compareValues(data.total_cost, data.previous_period.total_cost, true)
        : null,
    },
    {
      label: "tokens used",
      value: formatTokens(data.total_tokens),
      sub: `${formatTokens(data.input_tokens)} in / ${formatTokens(data.output_tokens)} out`,
      showTokenBar: true,
      inputTokens: data.input_tokens,
      outputTokens: data.output_tokens,
      comparison: data.previous_period
        ? compareValues(data.total_tokens, data.previous_period.total_tokens, false)
        : null,
    },
    {
      label: "agent time",
      value: formatDuration(data.total_duration_ns),
      sub: `${formatDuration(data.total_duration_ns / sessionCount)} avg session`,
      comparison: data.previous_period
        ? compareValues(data.total_duration_ns, data.previous_period.total_duration_ns, false)
        : null,
    },
    {
      label: "success rate",
      value: formatPercent(data.success_rate),
      valueClass: "metric__value--green",
      sub: `${data.completed}/${data.sessions.length} sessions complete`,
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
    if (item.valueClass) value.classList.add(item.valueClass);
    value.textContent = item.value;

    card.appendChild(label);
    card.appendChild(value);

    if (item.comparison) {
      const compare = document.createElement("div");
      compare.className = "metric__compare";
      compare.classList.add(item.comparison.positive ? "metric__compare--up" : "metric__compare--down");
      compare.textContent = `${item.comparison.direction === "up" ? "\u25B2" : "\u25BC"} ${item.comparison.value} vs prev`;
      card.appendChild(compare);
    }

    const sub = document.createElement("div");
    sub.className = "metric__sub";
    sub.textContent = item.sub;
    card.appendChild(sub);

    if (item.showTokenBar) {
      const total = item.inputTokens + item.outputTokens;
      if (total > 0) {
        const bar = document.createElement("div");
        bar.className = "metric__token-bar";
        const inSpan = document.createElement("span");
        inSpan.style.width = `${(item.inputTokens / total) * 100}%`;
        const outSpan = document.createElement("span");
        outSpan.style.width = `${(item.outputTokens / total) * 100}%`;
        bar.appendChild(inSpan);
        bar.appendChild(outSpan);
        card.appendChild(bar);
      }
    }

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

    const header = document.createElement("div");
    header.className = "model-row__header";

    const name = document.createElement("span");
    name.className = "model-row__name";
    name.textContent = model.model;
    name.style.color = colorForModel(model.model);

    const meta = document.createElement("span");
    meta.className = "model-row__meta";
    meta.textContent = `${formatCost(model.total_cost)} / ${model.session_count}`;

    header.appendChild(name);
    header.appendChild(meta);

    const bar = document.createElement("div");
    bar.className = "model-row__bar";
    const fill = document.createElement("div");
    fill.className = "model-row__fill";
    fill.style.width = `${Math.round((model.total_cost / max) * 100)}%`;
    fill.style.background = colorForModel(model.model);
    bar.appendChild(fill);

    row.appendChild(header);
    row.appendChild(bar);
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
    { label: "completed", pct: completedPct, count: data.completed, cls: "status__dot--completed" },
    { label: "failed", pct: failedPct, count: data.failed, cls: "status__dot--failed" },
    { label: "abandoned", pct: abandonedPct, count: data.abandoned, cls: "status__dot--abandoned" },
  ];

  legendItems.forEach((item) => {
    const entry = document.createElement("div");
    entry.className = "status__legend-item";
    const dot = document.createElement("span");
    dot.className = `status__dot ${item.cls}`;
    const text = document.createElement("span");
    text.textContent = `${item.label} ${item.pct.toFixed(0)}% (${item.count})`;
    entry.appendChild(dot);
    entry.appendChild(text);
    legend.appendChild(entry);
  });

  const efficiency = document.createElement("div");
  efficiency.className = "status__efficiency";
  const tokensPerMinute = data.total_duration_ns > 0
    ? Math.round((data.total_tokens / (data.total_duration_ns / 1e9)) * 60)
    : 0;
  const costPerSession = data.total_cost / Math.max(data.sessions.length, 1);
  efficiency.textContent = `eff: ${formatCost(costPerSession)}/sess`;

  container.appendChild(bar);
  container.appendChild(legend);
  container.appendChild(efficiency);
  statusEl.appendChild(container);
};

const renderSessions = (data) => {
  sessionsEl.innerHTML = "";
  if (!data.sessions.length) {
    sessionsEl.textContent = "no sessions";
    sessionsMoreEl.hidden = true;
    return;
  }

  const selectedIndex = data.sessions.findIndex((s) => s.id === selectedSessionId);
  if (selectedIndex >= 0) {
    sessionIndex = selectedIndex;
  }

  const header = document.createElement("div");
  header.className = "sessions-row sessions-row--header";
  header.innerHTML =
    "<div>#</div><div>label</div><div>model</div><div>dur</div><div>tokens</div><div>cost</div><div>tools</div><div>msgs</div><div>status</div>";
  sessionsEl.appendChild(header);

  const visible = data.sessions.slice(0, visibleSessionCount);
  visible.forEach((session, index) => {
    const row = document.createElement("div");
    row.className = "sessions-row";
    if (session.id === selectedSessionId) {
      row.classList.add("sessions-row--active");
    }

    const number = document.createElement("div");
    number.className = "session-number";
    number.textContent = String(index + 1).padStart(2, "0");

    const label = document.createElement("div");
    label.className = "session-label";
    label.textContent = session.label;

    const model = document.createElement("div");
    model.className = "session-model";
    model.textContent = session.model || "unknown";
    model.style.color = colorForModel(session.model);

    const duration = document.createElement("div");
    duration.textContent = formatDuration(session.duration_ns);

    const tokens = document.createElement("div");
    tokens.textContent = formatTokens(session.input_tokens + session.output_tokens);

    const cost = document.createElement("div");
    cost.className = "session-cost";
    const costBar = document.createElement("div");
    costBar.className = "session-cost__bar";
    const totalCostParts = session.input_cost + session.output_cost || 1;
    const inputPct = Math.round((session.input_cost / totalCostParts) * 100);
    const barIn = document.createElement("div");
    barIn.className = "session-cost__bar-in";
    barIn.style.width = `${inputPct}%`;
    const barOut = document.createElement("div");
    barOut.className = "session-cost__bar-out";
    barOut.style.width = `${100 - inputPct}%`;
    costBar.appendChild(barIn);
    costBar.appendChild(barOut);
    const costValue = document.createElement("span");
    costValue.textContent = formatCost(session.total_cost);
    cost.appendChild(costBar);
    cost.appendChild(costValue);

    const tools = document.createElement("div");
    tools.textContent = session.tool_calls;

    const msgs = document.createElement("div");
    msgs.textContent = session.message_count;

    const status = document.createElement("div");
    status.className = `session-status session-status--${statusClass(session.status)}`;
    const statusDot = document.createElement("span");
    statusDot.className = `session-status__dot session-status__dot--${statusClass(session.status)}`;
    const statusText = document.createElement("span");
    statusText.textContent = session.status;
    status.appendChild(statusDot);
    status.appendChild(statusText);

    row.appendChild(number);
    row.appendChild(label);
    row.appendChild(model);
    row.appendChild(duration);
    row.appendChild(tokens);
    row.appendChild(cost);
    row.appendChild(tools);
    row.appendChild(msgs);
    row.appendChild(status);

    row.addEventListener("click", () => loadSession(session.id));
    sessionsEl.appendChild(row);
  });

  if (data.sessions.length > visibleSessionCount) {
    sessionsMoreEl.hidden = false;
    showMoreButton.textContent = `show more (${visibleSessionCount}-${data.sessions.length} of ${data.sessions.length})`;
  } else {
    sessionsMoreEl.hidden = true;
  }
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
      sub: avgCost ? `${formatCost(avgCost)} avg` : "",
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
      sub: avgDuration ? `${formatDuration(avgDuration)} avg` : "",
      change: avgDuration ? compareValues(detail.summary.duration_ns, avgDuration, false) : null,
    },
    {
      label: "tool calls",
      value: detail.summary.tool_calls,
      sub: avgToolCalls ? `${avgToolCalls.toFixed(1)} avg` : "",
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
      change.textContent = `${metric.change.direction === "up" ? "\u25B2" : "\u25BC"} ${metric.change.value} vs avg`;
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
  title.textContent = `conversation timeline (${detail.messages.length} turns, ${detail.messages.length} messages)`;

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
  const outer = document.createElement("div");

  const sectionHeader = document.createElement("div");
  sectionHeader.className = "detail__conversation-header";
  const headerLabel = document.createElement("span");
  headerLabel.className = "section-header__label";
  headerLabel.textContent = "conversation";
  const headerLine = document.createElement("div");
  headerLine.className = "section-header__line";
  headerLine.style.flex = "1";
  sectionHeader.appendChild(headerLabel);
  sectionHeader.appendChild(headerLine);

  const tabs = document.createElement("div");
  tabs.className = "detail__conversation-tabs";
  const tableTab = document.createElement("button");
  tableTab.className = "detail__conversation-tab detail__conversation-tab--active";
  tableTab.textContent = "table";
  const rawTab = document.createElement("button");
  rawTab.className = "detail__conversation-tab";
  rawTab.textContent = "raw";
  tabs.appendChild(tableTab);
  tabs.appendChild(rawTab);
  sectionHeader.appendChild(tabs);

  const container = document.createElement("div");
  container.className = "detail__conversation";

  const table = document.createElement("div");
  table.className = "conversation__table";

  const header = document.createElement("div");
  header.className = "conversation__row conversation__row--header";
  header.innerHTML = "<div>#</div><div>role</div><div>tokens</div><div>cost</div>";
  table.appendChild(header);

  detail.messages.forEach((msg, idx) => {
    const row = document.createElement("div");
    row.className = "conversation__row";
    if (idx === selectedMessageIndex) {
      row.classList.add("conversation__row--active");
    }

    const num = document.createElement("div");
    num.textContent = String(idx + 1);
    num.style.color = "#525252";

    const role = document.createElement("div");
    role.textContent = msg.role;
    role.style.color = msg.role === "user" ? "#38bdf8" : "#fb923c";
    role.style.fontWeight = "600";

    const tokens = document.createElement("div");
    tokens.textContent = formatTokens(msg.total_tokens);

    const cost = document.createElement("div");
    cost.textContent = formatCost(msg.total_cost);

    row.appendChild(num);
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

  if (msg) {
    const meta = document.createElement("div");
    meta.className = "conversation__meta";
    const metaItems = [
      { label: "role", value: msg.role },
      { label: "time", value: new Date(msg.timestamp).toLocaleTimeString() },
      { label: "model", value: msg.model || "unknown" },
      { label: "tokens", value: `In ${formatTokens(msg.input_tokens)}  Out ${formatTokens(msg.output_tokens)}  Total ${formatTokens(msg.total_tokens)}` },
      { label: "cost", value: `In ${formatCost(msg.input_cost)}  Out ${formatCost(msg.output_cost)}  Total ${formatCost(msg.total_cost)}` },
    ];
    metaItems.forEach((item) => {
      const block = document.createElement("div");
      block.textContent = item.label;
      const span = document.createElement("span");
      span.className = "conversation__meta-value";
      span.textContent = item.value;
      block.appendChild(span);
      meta.appendChild(block);
    });

    const tools = document.createElement("div");
    tools.className = "conversation__meta";
    const toolBlock = document.createElement("div");
    toolBlock.textContent = "tools";
    const toolValue = document.createElement("span");
    toolValue.className = "conversation__meta-value";
    toolValue.textContent = msg.tool_calls && msg.tool_calls.length ? msg.tool_calls.join(", ") : "none";
    toolBlock.appendChild(toolValue);
    tools.appendChild(toolBlock);

    const text = document.createElement("div");
    text.className = "conversation__text";
    text.textContent = msg.text || "";

    detailPane.appendChild(meta);
    detailPane.appendChild(tools);
    detailPane.appendChild(text);
  }

  container.appendChild(table);
  container.appendChild(detailPane);

  outer.appendChild(sectionHeader);
  outer.appendChild(container);
  return outer;
};

const renderSessionDetail = (detail) => {
  detailEl.innerHTML = "";

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
  status.className = `detail__status session-status--${statusClass(detail.summary.status)}`;
  const dot = document.createElement("span");
  dot.className = `session-status__dot session-status__dot--${statusClass(detail.summary.status)}`;
  const statusText = document.createElement("span");
  statusText.textContent = detail.summary.status;
  status.appendChild(dot);
  status.appendChild(statusText);

  header.appendChild(headerText);
  header.appendChild(status);

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
  const filterText = filterBits.length ? ` \u00B7 ${filterBits.join(" \u00B7 ")}` : "";
  sessionCountEl.textContent = `${data.sessions.length} sessions \u00B7 ${formatDuration(data.total_duration_ns)} total time${filterText}`;
  statusLabelEl.textContent = filters.status || "all";

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
  sessionBreadcrumbEl.textContent = data.summary.label;
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
  const rows = sessionsEl.querySelectorAll(".sessions-row:not(.sessions-row--header)");
  if (rows[sessionIndex]) {
    rows[sessionIndex].scrollIntoView({ block: "nearest" });
  }
};

const moveMessage = (delta) => {
  if (!detailEl || !detailEl.querySelectorAll) return;
  const rows = detailEl.querySelectorAll(".conversation__row:not(.conversation__row--header)");
  if (!rows.length) return;
  const maxIndex = rows.length - 1;
  selectedMessageIndex = Math.min(Math.max(selectedMessageIndex + delta, 0), maxIndex);
  const row = rows[selectedMessageIndex];
  if (row) {
    row.scrollIntoView({ block: "nearest" });
    row.click();
  }
};

const toggleFilters = () => {
  filtersVisible = !filtersVisible;
  sessionsFiltersEl.hidden = !filtersVisible;
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
        toggleFilters();
        if (filtersVisible) sortSelect.focus();
      }
      break;
    case "f":
      if (currentView === "overview") {
        toggleFilters();
        if (filtersVisible) statusSelect.focus();
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

showMoreButton.addEventListener("click", () => {
  visibleSessionCount += 38;
  if (overviewState) renderSessions(overviewState);
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
