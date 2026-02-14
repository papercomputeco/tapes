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
const analyticsViewEl = document.getElementById("analytics-view");
const sessionBreadcrumbEl = document.getElementById("session-breadcrumb");
const backButton = document.getElementById("back-button");
const analyticsBackButton = document.getElementById("analytics-back-button");
const statusLabelEl = document.getElementById("status-label");
const sessionsMoreEl = document.getElementById("sessions-more");
const showMoreButton = document.getElementById("show-more-button");
const sessionsFiltersEl = document.getElementById("sessions-filters");
const analyticsSummaryEl = document.getElementById("analytics-summary");
const analyticsHeatmapEl = document.getElementById("analytics-heatmap");
const analyticsToolsEl = document.getElementById("analytics-tools");
const analyticsDurationEl = document.getElementById("analytics-duration");
const analyticsCostEl = document.getElementById("analytics-cost");
const analyticsModelsEl = document.getElementById("analytics-models");
const analyticsProvidersEl = document.getElementById("analytics-providers");
const analyticsSubtitleEl = document.getElementById("analytics-subtitle");
const analyticsPeriodEl = document.getElementById("analytics-period");
const analyticsInsightsEl = document.getElementById("analytics-insights");
const analyticsLoadingEl = document.getElementById("analytics-loading");
const analyticsContentEl = document.getElementById("analytics-content");
const heatmapLabelsEl = document.getElementById("heatmap-labels");
const insightsGoalsEl = document.getElementById("insights-goals");
const insightsOutcomesEl = document.getElementById("insights-outcomes");
const insightsFrictionEl = document.getElementById("insights-friction");
const insightsTypesEl = document.getElementById("insights-types");
const insightsSummariesEl = document.getElementById("insights-summaries");
const dayDetailEl = document.getElementById("day-detail");
const dayDetailDateEl = document.getElementById("day-detail-date");
const dayDetailMetricsEl = document.getElementById("day-detail-metrics");
const dayDetailSessionsEl = document.getElementById("day-detail-sessions");
const dayDetailPlaceholderEl = document.getElementById("day-detail-placeholder");
const dayPrevButton = document.getElementById("day-prev");
const dayNextButton = document.getElementById("day-next");
const dayDetailCloseButton = document.getElementById("day-detail-close");
const heatmapHintEl = document.getElementById("heatmap-hint");
const projectSelect = document.getElementById("project-select");

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
  project: "",
  period: "30d",
  periodEnabled: false,
};

let overviewState = null;
let sessionDetailState = null;
let analyticsState = null;
let facetsState = null;
let selectedSessionId = null;
let selectedMessageIndex = 0;
let sessionIndex = 0;
let currentView = "overview";
let visibleSessionCount = 38;
let filtersVisible = false;
let selectedDayDate = null;
let dayDetailState = null;
let sessionEntryView = null;
let heatmapSelectableDays = [];
let activeAnalyticsTab = "activity";

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
  if (filters.project) params.set("project", filters.project);
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

const buildSessionRow = (session, index, onClick) => {
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

  const project = document.createElement("div");
  project.className = "session-project";
  project.textContent = session.project || "";

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
  row.appendChild(project);
  row.appendChild(model);
  row.appendChild(duration);
  row.appendChild(tokens);
  row.appendChild(cost);
  row.appendChild(tools);
  row.appendChild(msgs);
  row.appendChild(status);

  row.addEventListener("click", onClick);
  return row;
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
    "<div>#</div><div>label</div><div>project</div><div>model</div><div>dur</div><div>tokens</div><div>cost</div><div>tools</div><div>msgs</div><div>status</div>";
  sessionsEl.appendChild(header);

  const visible = data.sessions.slice(0, visibleSessionCount);
  visible.forEach((session, index) => {
    const row = buildSessionRow(session, index, () => loadSession(session.id));
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

  if (detail.summary.project) {
    const headerProject = document.createElement("div");
    headerProject.className = "detail__project";
    headerProject.textContent = detail.summary.project;
    headerText.appendChild(headerProject);
  }

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

// ── Analytics rendering ──

const renderAnalyticsSummary = (data) => {
  analyticsSummaryEl.innerHTML = "";
  const items = [
    { label: "total sessions", value: data.total_sessions },
    { label: "avg cost/session", value: formatCost(data.avg_session_cost) },
    { label: "avg duration", value: formatDuration(data.avg_duration_ns) },
    { label: "models tracked", value: data.model_performance ? data.model_performance.length : 0 },
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
    analyticsSummaryEl.appendChild(card);
  });
};

const renderHeatmap = (data) => {
  analyticsHeatmapEl.innerHTML = "";
  const days = data.activity_by_day || [];
  if (days.length === 0) {
    analyticsHeatmapEl.textContent = "no activity data";
    heatmapLabelsEl.innerHTML = "";
    heatmapSelectableDays = [];
    return;
  }
  const maxSessions = Math.max(...days.map((d) => d.sessions), 1);
  heatmapSelectableDays = days.slice(-9).reverse();
  days.forEach((day) => {
    const cell = document.createElement("div");
    cell.className = "heatmap-cell";
    if (day.sessions === 0) {
      cell.classList.add("heatmap-cell--empty");
    } else {
      const intensity = day.sessions / maxSessions;
      if (intensity >= 0.7) {
        cell.classList.add("heatmap-cell--high");
      } else if (intensity >= 0.3) {
        cell.classList.add("heatmap-cell--med");
      } else {
        cell.classList.add("heatmap-cell--low");
      }
      cell.addEventListener("click", () => {
        selectHeatmapDay(day.date);
      });
    }
    if (selectedDayDate === day.date) {
      cell.classList.add("heatmap-cell--selected");
    }
    cell.title = `${day.date}: ${day.sessions} sessions, ${formatCost(day.cost)}`;
    analyticsHeatmapEl.appendChild(cell);
  });
  renderHeatmapLabels();
};

const renderHeatmapLabels = () => {
  heatmapLabelsEl.innerHTML = "";
  if (!heatmapSelectableDays.length) return;

  heatmapSelectableDays.forEach((day, index) => {
    const label = document.createElement("button");
    label.type = "button";
    label.className = "heatmap-label";
    if (selectedDayDate === day.date) {
      label.classList.add("heatmap-label--selected");
    }
    label.addEventListener("click", () => selectHeatmapDay(day.date));

    const indexEl = document.createElement("span");
    indexEl.className = "heatmap-label__index";
    indexEl.textContent = String(index + 1);

    const dateEl = document.createElement("span");
    dateEl.className = "heatmap-label__date";
    dateEl.textContent = day.date.slice(5);

    label.appendChild(indexEl);
    label.appendChild(dateEl);
    heatmapLabelsEl.appendChild(label);
  });
};

const renderHeatmapHint = (data) => {
  heatmapHintEl.innerHTML = "";
  const days = data.activity_by_day || [];
  if (days.length === 0) return;

  // Legend
  const legend = document.createElement("div");
  legend.className = "heatmap-hint__legend";
  const maxSessions = Math.max(...days.map((d) => d.sessions), 1);
  const levels = [
    { label: "none", opacity: 1, bg: "var(--border)" },
    { label: "low", opacity: 0.25, bg: "var(--green)" },
    { label: "med", opacity: 0.55, bg: "var(--green)" },
    { label: "high", opacity: 1, bg: "var(--green)" },
  ];
  levels.forEach((lvl) => {
    const item = document.createElement("span");
    const swatch = document.createElement("span");
    swatch.className = "heatmap-hint__swatch";
    swatch.style.background = lvl.bg;
    swatch.style.opacity = lvl.opacity;
    item.appendChild(swatch);
    item.appendChild(document.createTextNode(lvl.label));
    legend.appendChild(item);
  });
  heatmapHintEl.appendChild(legend);

  // Compute stats
  const activeDays = days.filter((d) => d.sessions > 0);
  const totalDays = days.length;
  const peakDay = activeDays.length > 0
    ? activeDays.reduce((a, b) => (b.sessions > a.sessions ? b : a))
    : null;
  const avgSessions = activeDays.length > 0
    ? (activeDays.reduce((sum, d) => sum + d.sessions, 0) / activeDays.length)
    : 0;

  // Current streak (count consecutive active days from the end)
  let streak = 0;
  for (let i = days.length - 1; i >= 0; i--) {
    if (days[i].sessions > 0) streak++;
    else break;
  }

  // Top tool from analytics data
  const topTools = data.top_tools || [];
  const topTool = topTools.length > 0 ? topTools[0] : null;
  const totalToolCalls = topTools.reduce((sum, t) => sum + t.count, 0);
  const totalErrors = topTools.reduce((sum, t) => sum + (t.error_count || 0), 0);

  const stats = document.createElement("div");
  stats.className = "heatmap-hint__stats";

  const lines = [];
  if (peakDay) {
    lines.push(`peak: <span>${peakDay.date.slice(5)}</span> with <span>${peakDay.sessions} sessions</span> (<span>${formatCost(peakDay.cost)}</span> spent)`);
  }
  lines.push(`<span>${activeDays.length}</span> of <span>${totalDays}</span> days active (<span>${Math.round((activeDays.length / totalDays) * 100)}%</span>)`);
  lines.push(`avg <span>${avgSessions.toFixed(1)}</span> sessions on active days`);
  lines.push(`current streak: <span>${streak} day${streak !== 1 ? "s" : ""}</span>`);
  if (topTool) {
    const topToolPct = totalToolCalls > 0 ? Math.round((topTool.count / totalToolCalls) * 100) : 0;
    lines.push(`<span>${topTool.name}</span> is most used (<span>${topToolPct}%</span> of all calls)`);
  }
  if (totalErrors > 0) {
    lines.push(`<span>${totalErrors}</span> tool errors across <span>${totalToolCalls}</span> total calls`);
  } else {
    lines.push(`no tool errors detected`);
  }
  lines.push(`<span>${topTools.length}</span> unique tools across <span>${totalToolCalls.toLocaleString()}</span> total calls`);

  stats.innerHTML = lines.map((l) => `<div>${l}</div>`).join("");
  heatmapHintEl.appendChild(stats);

  const prompt = document.createElement("div");
  prompt.className = "heatmap-hint__prompt";
  if (selectedDayDate) {
    const dayData = days.find((d) => d.date === selectedDayDate);
    const count = dayData ? dayData.sessions : 0;
    prompt.innerHTML = `filtering by <span style="color:var(--primary);font-weight:700">${selectedDayDate}</span> — ${count} session${count !== 1 ? "s" : ""} shown below`;
  } else {
    prompt.textContent = "click a cell to drill into that day's sessions";
  }
  heatmapHintEl.appendChild(prompt);
};

const TOOLS_LIMIT = 10;

const renderTopTools = (data) => {
  analyticsToolsEl.innerHTML = "";
  const tools = data.top_tools || [];
  if (tools.length === 0) {
    analyticsToolsEl.textContent = "no tool data";
    return;
  }
  const sorted = [...tools].sort((a, b) => (b.sessions || 0) - (a.sessions || 0));
  const maxSessions = sorted.length > 0 ? (sorted[0].sessions || 1) : 1;
  const needsExpand = sorted.length > TOOLS_LIMIT;
  let expanded = false;

  const container = document.createElement("div");
  const renderRows = () => {
    container.innerHTML = "";
    const visible = expanded ? sorted : sorted.slice(0, TOOLS_LIMIT);
    visible.forEach((tool) => {
      const sessions = tool.sessions || 0;
      const avg = sessions > 0 ? Math.round(tool.count / sessions) : 0;
      const row = document.createElement("div");
      row.className = "tool-row";
      const header = document.createElement("div");
      header.className = "tool-row__header";
      const name = document.createElement("span");
      name.className = "tool-row__name";
      name.textContent = tool.name;
      const meta = document.createElement("span");
      meta.className = "tool-row__meta";
      meta.textContent = `${sessions} sessions · ${avg}/s`;
      if (tool.error_count > 0) {
        meta.textContent += ` · ${tool.error_count} err`;
        meta.classList.add("tool-row__meta--error");
      }
      header.appendChild(name);
      header.appendChild(meta);
      const bar = document.createElement("div");
      bar.className = "tool-row__bar";
      const fill = document.createElement("div");
      fill.className = "tool-row__fill";
      fill.style.width = `${Math.max(Math.round((sessions / maxSessions) * 100), 2)}%`;
      if (tool.error_count > 0) {
        const errorFill = document.createElement("div");
        errorFill.className = "tool-row__fill--error";
        errorFill.style.width = `${Math.round((tool.error_count / tool.count) * 100)}%`;
        fill.appendChild(errorFill);
      }
      bar.appendChild(fill);
      row.appendChild(header);
      row.appendChild(bar);
      container.appendChild(row);
    });
    if (needsExpand) {
      const toggle = document.createElement("div");
      toggle.className = "histogram__toggle";
      toggle.textContent = expanded
        ? "show less"
        : `+${sorted.length - TOOLS_LIMIT} more`;
      toggle.addEventListener("click", () => {
        expanded = !expanded;
        renderRows();
      });
      container.appendChild(toggle);
    }
  };
  renderRows();
  analyticsToolsEl.appendChild(container);
};

const renderHistogram = (el, buckets) => {
  el.innerHTML = "";
  if (!buckets || buckets.length === 0) {
    el.textContent = "no data";
    return;
  }
  const maxCount = Math.max(...buckets.map((b) => b.count), 1);
  buckets.forEach((bucket) => {
    const row = document.createElement("div");
    row.className = "histogram__row";
    const label = document.createElement("div");
    label.className = "histogram__label";
    label.textContent = bucket.label;
    const barWrap = document.createElement("div");
    barWrap.className = "histogram__bar-wrap";
    const bar = document.createElement("div");
    bar.className = "histogram__bar";
    bar.style.width = `${Math.max(Math.round((bucket.count / maxCount) * 100), 2)}%`;
    barWrap.appendChild(bar);
    const count = document.createElement("div");
    count.className = "histogram__count";
    count.textContent = bucket.count;
    row.appendChild(label);
    row.appendChild(barWrap);
    row.appendChild(count);
    el.appendChild(row);
  });
};

const renderModelComparison = (data) => {
  analyticsModelsEl.innerHTML = "";
  const models = data.model_performance || [];
  if (models.length === 0) {
    analyticsModelsEl.textContent = "no model data";
    return;
  }
  const table = document.createElement("div");
  table.className = "model-table";
  const header = document.createElement("div");
  header.className = "model-table__row model-table__row--header";
  header.innerHTML = "<div>model</div><div>sessions</div><div>avg cost</div><div>avg dur</div><div>avg tokens</div><div>success</div>";
  table.appendChild(header);
  models.forEach((model) => {
    const row = document.createElement("div");
    row.className = "model-table__row";
    const nameEl = document.createElement("div");
    nameEl.className = "model-table__name";
    nameEl.textContent = model.model;
    nameEl.style.color = colorForModel(model.model);
    const sessEl = document.createElement("div");
    sessEl.textContent = model.sessions;
    const costEl = document.createElement("div");
    costEl.textContent = formatCost(model.avg_cost);
    const durEl = document.createElement("div");
    durEl.textContent = formatDuration(model.avg_duration_ns);
    const tokEl = document.createElement("div");
    tokEl.textContent = formatTokens(model.avg_tokens);
    const successEl = document.createElement("div");
    successEl.textContent = formatPercent(model.success_rate);
    successEl.style.color = model.success_rate >= 0.8 ? "var(--green)" : model.success_rate >= 0.5 ? "var(--orange)" : "var(--primary)";
    row.appendChild(nameEl);
    row.appendChild(sessEl);
    row.appendChild(costEl);
    row.appendChild(durEl);
    row.appendChild(tokEl);
    row.appendChild(successEl);
    table.appendChild(row);
  });
  analyticsModelsEl.appendChild(table);
};

const renderProviderSplit = (data) => {
  analyticsProvidersEl.innerHTML = "";
  const providers = data.provider_breakdown || {};
  const entries = Object.entries(providers).sort((a, b) => b[1] - a[1]);
  if (entries.length === 0) {
    analyticsProvidersEl.textContent = "no provider data";
    return;
  }
  const total = entries.reduce((sum, [, count]) => sum + count, 0);
  const providerColors = { anthropic: "var(--pink)", openai: "var(--green)", google: "var(--orange)" };
  const bar = document.createElement("div");
  bar.className = "provider__bar";
  entries.forEach(([name, count]) => {
    const segment = document.createElement("span");
    segment.className = "provider__segment";
    segment.style.width = `${(count / total) * 100}%`;
    segment.style.background = providerColors[name] || "var(--blue)";
    bar.appendChild(segment);
  });
  analyticsProvidersEl.appendChild(bar);
  const legend = document.createElement("div");
  legend.className = "provider__legend";
  entries.forEach(([name, count]) => {
    const item = document.createElement("div");
    item.className = "provider__legend-item";
    const dot = document.createElement("span");
    dot.className = "provider__dot";
    dot.style.background = providerColors[name] || "var(--blue)";
    const text = document.createElement("span");
    text.textContent = `${name} ${Math.round((count / total) * 100)}% (${count})`;
    item.appendChild(dot);
    item.appendChild(text);
    legend.appendChild(item);
  });
  analyticsProvidersEl.appendChild(legend);
};

const selectHeatmapDay = (dateStr) => {
  if (selectedDayDate === dateStr) {
    closeDayDetail();
    return;
  }
  selectedDayDate = dateStr;
  if (analyticsState) {
    renderHeatmap(analyticsState);
    renderHeatmapHint(analyticsState);
  }
  loadDayDetail(dateStr);
};

const closeDayDetail = () => {
  selectedDayDate = null;
  dayDetailState = null;
  dayDetailEl.hidden = false;
  dayDetailDateEl.textContent = "day detail";
  dayDetailMetricsEl.innerHTML = "";
  dayDetailSessionsEl.innerHTML = "";
  dayDetailPlaceholderEl.hidden = false;
  dayPrevButton.disabled = true;
  dayNextButton.disabled = true;
  if (analyticsState) {
    renderHeatmap(analyticsState);
    renderHeatmapHint(analyticsState);
  }
};

const navigateDay = (delta) => {
  if (!analyticsState || !selectedDayDate) return;
  const days = (analyticsState.activity_by_day || []).filter((d) => d.sessions > 0);
  const currentIdx = days.findIndex((d) => d.date === selectedDayDate);
  if (currentIdx < 0) return;
  const nextIdx = currentIdx + delta;
  if (nextIdx < 0 || nextIdx >= days.length) return;
  selectHeatmapDay(days[nextIdx].date);
};

const toLocalISO = (date) => {
  const off = date.getTimezoneOffset();
  const sign = off <= 0 ? "+" : "-";
  const hh = String(Math.floor(Math.abs(off) / 60)).padStart(2, "0");
  const mm = String(Math.abs(off) % 60).padStart(2, "0");
  const y = date.getFullYear();
  const mo = String(date.getMonth() + 1).padStart(2, "0");
  const da = String(date.getDate()).padStart(2, "0");
  const h = String(date.getHours()).padStart(2, "0");
  const mi = String(date.getMinutes()).padStart(2, "0");
  const s = String(date.getSeconds()).padStart(2, "0");
  return `${y}-${mo}-${da}T${h}:${mi}:${s}${sign}${hh}:${mm}`;
};

const loadDayDetail = async (dateStr) => {
  const fromDate = new Date(dateStr + "T00:00:00");
  const toDate = new Date(dateStr + "T00:00:00");
  toDate.setDate(toDate.getDate() + 1);
  const from = toLocalISO(fromDate);
  const to = toLocalISO(toDate);

  dayDetailEl.hidden = false;
  dayDetailEl.scrollIntoView({ behavior: "smooth", block: "nearest" });
  dayDetailDateEl.textContent = dateStr;
  dayDetailPlaceholderEl.hidden = true;
  dayPrevButton.disabled = false;
  dayNextButton.disabled = false;
  dayDetailMetricsEl.innerHTML = "";
  dayDetailSessionsEl.innerHTML = '<div style="padding:12px;color:var(--muted);font-size:11px;">loading...</div>';

  const res = await fetch(`/api/overview?from=${from}&to=${to}`);
  const data = await res.json();
  dayDetailState = data;

  const sessions = data.sessions || [];
  const totalCost = data.total_cost || 0;
  const avgCost = sessions.length ? totalCost / sessions.length : 0;
  const completed = sessions.filter((s) => s.status === "completed").length;
  const successRate = sessions.length ? completed / sessions.length : 0;

  dayDetailMetricsEl.innerHTML = "";
  const metrics = [
    { label: "sessions", value: sessions.length },
    { label: "total cost", value: formatCost(totalCost) },
    { label: "avg cost", value: formatCost(avgCost) },
    { label: "success rate", value: formatPercent(successRate) },
  ];
  metrics.forEach((m) => {
    const card = document.createElement("div");
    card.className = "day-detail__metric";
    const label = document.createElement("div");
    label.className = "day-detail__metric-label";
    label.textContent = m.label;
    const value = document.createElement("div");
    value.className = "day-detail__metric-value";
    value.textContent = m.value;
    card.appendChild(label);
    card.appendChild(value);
    dayDetailMetricsEl.appendChild(card);
  });

  dayDetailSessionsEl.innerHTML = "";
  if (sessions.length === 0) {
    dayDetailSessionsEl.innerHTML = '<div style="padding:12px;color:var(--muted);font-size:11px;">no sessions</div>';
    return;
  }

  const header = document.createElement("div");
  header.className = "sessions-row sessions-row--header";
  header.innerHTML =
    "<div>#</div><div>label</div><div>model</div><div>dur</div><div>tokens</div><div>cost</div><div>tools</div><div>msgs</div><div>status</div>";
  dayDetailSessionsEl.appendChild(header);

  sessions.forEach((session, index) => {
    const row = buildSessionRow(session, index, () => {
      sessionEntryView = "analytics";
      loadSession(session.id);
    });
    dayDetailSessionsEl.appendChild(row);
  });
};

const goalLabels = {
  debug_investigate: { label: "Debug / Investigate", desc: "Diagnosing errors, tracing issues, reading logs" },
  implement_feature: { label: "Implement Feature", desc: "Building new functionality from scratch" },
  fix_bug: { label: "Fix Bug", desc: "Correcting broken behavior in existing code" },
  write_script_tool: { label: "Write Script / Tool", desc: "One-off scripts, automation, or CLI tools" },
  refactor_code: { label: "Refactor Code", desc: "Restructuring code without changing behavior" },
  configure_system: { label: "Configure System", desc: "Setting up infra, CI/CD, environment configs" },
  create_pr_commit: { label: "Create PR / Commit", desc: "Preparing code for review and submission" },
  analyze_data: { label: "Analyze Data", desc: "Querying, exploring, or summarizing data" },
  understand_codebase: { label: "Understand Codebase", desc: "Reading code to learn how something works" },
  write_tests: { label: "Write Tests", desc: "Adding unit, integration, or e2e tests" },
  write_docs: { label: "Write Docs", desc: "Creating or updating documentation" },
  deploy_infra: { label: "Deploy / Infra", desc: "Deploying code or managing infrastructure" },
  warmup_minimal: { label: "Warmup / Minimal", desc: "Brief session with little substantive work" },
};

const outcomeLabels = {
  fully_achieved: { label: "Fully Achieved", desc: "Goal completed successfully" },
  mostly_achieved: { label: "Mostly Achieved", desc: "Goal met with minor gaps remaining" },
  partially_achieved: { label: "Partially Achieved", desc: "Some progress but significant work remains" },
  not_achieved: { label: "Not Achieved", desc: "Goal was not accomplished" },
};

const sessionTypeLabels = {
  single_task: { label: "Single Task", desc: "Focused on one specific objective" },
  multi_task: { label: "Multi-Task", desc: "Tackled several distinct objectives" },
  iterative_refinement: { label: "Iterative Refinement", desc: "Repeated cycles of adjustment and improvement" },
  exploration: { label: "Exploration", desc: "Open-ended investigation or learning" },
};

const frictionLabels = {
  wrong_approach: { label: "Wrong Approach", desc: "Went down an incorrect path before correcting" },
  buggy_code: { label: "Buggy Code", desc: "Generated code that had bugs needing fixes" },
  misunderstood_request: { label: "Misunderstood Request", desc: "Misinterpreted what the user was asking" },
  tool_failure: { label: "Tool Failure", desc: "A tool call failed or returned unexpected results" },
  unclear_requirements: { label: "Unclear Requirements", desc: "Ambiguous or incomplete requirements" },
  scope_creep: { label: "Scope Creep", desc: "Task expanded beyond the original ask" },
  environment_issue: { label: "Environment Issue", desc: "Problems with setup, deps, or config" },
};

const renderFacetInsights = (data) => {
  if (!data) {
    analyticsInsightsEl.hidden = true;
    return;
  }
  analyticsInsightsEl.hidden = false;

  // Goal distribution
  renderDistributionBars(insightsGoalsEl, data.goal_distribution, "var(--blue)", goalLabels);

  // Outcome distribution
  const outcomeColors = {
    fully_achieved: "var(--green)",
    mostly_achieved: "var(--blue)",
    partially_achieved: "var(--orange)",
    not_achieved: "var(--primary)",
  };
  renderDistributionBarsColored(insightsOutcomesEl, data.outcome_distribution, outcomeColors, outcomeLabels);

  // Friction points
  renderFrictionList(insightsFrictionEl, data.top_friction, frictionLabels);

  // Session types
  renderDistributionBars(insightsTypesEl, data.session_types, "var(--pink)", sessionTypeLabels);

  // Summaries go on separate tab
  renderSummariesList(insightsSummariesEl, data.recent_summaries);
};

const COLLAPSED_LIMIT = 5;

const renderDistributionBars = (el, distribution, color, labelMap) => {
  el.innerHTML = "";
  if (!distribution) {
    el.textContent = "no data";
    return;
  }
  const entries = Object.entries(distribution).sort((a, b) => b[1] - a[1]);
  const maxCount = entries.length > 0 ? entries[0][1] : 1;
  const needsExpand = entries.length > COLLAPSED_LIMIT;
  let expanded = false;

  const container = document.createElement("div");
  const renderRows = () => {
    container.innerHTML = "";
    const visible = expanded ? entries : entries.slice(0, COLLAPSED_LIMIT);
    visible.forEach(([key, count]) => {
      const info = labelMap?.[key];
      const row = document.createElement("div");
      row.className = "histogram__row";
      const labelEl = document.createElement("div");
      labelEl.className = "histogram__label";
      labelEl.textContent = info?.label || key.replace(/_/g, " ");
      if (info?.desc) labelEl.title = info.desc;
      const barWrap = document.createElement("div");
      barWrap.className = "histogram__bar-wrap";
      const bar = document.createElement("div");
      bar.className = "histogram__bar";
      bar.style.width = `${Math.max(Math.round((count / maxCount) * 100), 2)}%`;
      bar.style.background = color;
      if (info?.desc) bar.title = info.desc;
      barWrap.appendChild(bar);
      const countEl = document.createElement("div");
      countEl.className = "histogram__count";
      countEl.textContent = count;
      row.appendChild(labelEl);
      row.appendChild(barWrap);
      row.appendChild(countEl);
      if (info?.desc) {
        const descEl = document.createElement("div");
        descEl.className = "histogram__desc";
        descEl.textContent = info.desc;
        row.appendChild(descEl);
      }
      container.appendChild(row);
    });
    if (needsExpand) {
      const toggle = document.createElement("div");
      toggle.className = "histogram__toggle";
      toggle.textContent = expanded
        ? "show less"
        : `+${entries.length - COLLAPSED_LIMIT} more`;
      toggle.addEventListener("click", () => {
        expanded = !expanded;
        renderRows();
      });
      container.appendChild(toggle);
    }
  };
  renderRows();
  el.appendChild(container);
};

const renderDistributionBarsColored = (el, distribution, colors, labelMap) => {
  el.innerHTML = "";
  if (!distribution) {
    el.textContent = "no data";
    return;
  }
  const entries = Object.entries(distribution).sort((a, b) => b[1] - a[1]);
  const total = entries.reduce((sum, [, count]) => sum + count, 0) || 1;
  const bar = document.createElement("div");
  bar.className = "provider__bar";
  entries.forEach(([key, count]) => {
    const info = labelMap?.[key];
    const segment = document.createElement("span");
    segment.className = "provider__segment";
    segment.style.width = `${(count / total) * 100}%`;
    segment.style.background = colors[key] || "var(--muted)";
    segment.title = `${info?.label || key.replace(/_/g, " ")} (${count})${info?.desc ? " — " + info.desc : ""}`;
    bar.appendChild(segment);
  });
  el.appendChild(bar);
  const legend = document.createElement("div");
  legend.className = "provider__legend";
  entries.forEach(([key, count]) => {
    const info = labelMap?.[key];
    const displayLabel = info?.label || key.replace(/_/g, " ");
    const item = document.createElement("div");
    item.className = "provider__legend-item";
    const dot = document.createElement("span");
    dot.className = "provider__dot";
    dot.style.background = colors[key] || "var(--muted)";
    const text = document.createElement("span");
    text.textContent = `${displayLabel} ${Math.round((count / total) * 100)}% (${count})`;
    item.appendChild(dot);
    item.appendChild(text);
    if (info?.desc) {
      const desc = document.createElement("span");
      desc.className = "legend__desc";
      desc.textContent = info.desc;
      item.appendChild(desc);
    }
    legend.appendChild(item);
  });
  el.appendChild(legend);
};

const renderFrictionList = (el, friction, labelMap) => {
  el.innerHTML = "";
  if (!friction || friction.length === 0) {
    el.textContent = "no friction data";
    return;
  }
  friction.forEach((item) => {
    const key = item.type || item.name || "";
    const info = labelMap?.[key];
    const row = document.createElement("div");
    row.className = "friction-item";
    const badge = document.createElement("span");
    badge.className = "friction-item__badge";
    badge.textContent = item.count;
    const label = document.createElement("span");
    label.className = "friction-item__label";
    label.textContent = info?.label || key.replace(/_/g, " ");
    row.appendChild(badge);
    row.appendChild(label);
    if (info?.desc) {
      const desc = document.createElement("span");
      desc.className = "friction-item__desc";
      desc.textContent = info.desc;
      row.appendChild(desc);
    }
    el.appendChild(row);
  });
};

const SUMMARIES_LIMIT = 6;

const renderSummariesList = (el, summaries) => {
  el.innerHTML = "";
  if (!summaries || summaries.length === 0) {
    el.textContent = "no summaries";
    return;
  }
  const filtered = summaries.filter(
    (s) => (s.goal || s.underlying_goal || "").trim() || (s.summary || s.brief_summary || "").trim()
  );
  const needsExpand = filtered.length > SUMMARIES_LIMIT;
  let expanded = false;

  const container = document.createElement("div");
  const renderCards = () => {
    container.innerHTML = "";
    const visible = expanded ? filtered : filtered.slice(0, SUMMARIES_LIMIT);
    visible.forEach((s) => {
      const card = document.createElement("div");
      card.className = "summary-card";
      const goal = document.createElement("div");
      goal.className = "summary-card__goal";
      goal.textContent = s.goal || s.underlying_goal || "";
      const summary = document.createElement("div");
      summary.className = "summary-card__text";
      summary.textContent = s.summary || s.brief_summary || "";
      card.appendChild(goal);
      card.appendChild(summary);
      container.appendChild(card);
    });
    if (needsExpand) {
      const toggle = document.createElement("div");
      toggle.className = "histogram__toggle";
      toggle.textContent = expanded
        ? "show less"
        : `+${filtered.length - SUMMARIES_LIMIT} more`;
      toggle.addEventListener("click", () => {
        expanded = !expanded;
        renderCards();
      });
      container.appendChild(toggle);
    }
  };
  renderCards();
  el.appendChild(container);
};

const renderAnalyticsPeriodControls = () => {
  analyticsPeriodEl.innerHTML = "";
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
      renderAnalyticsPeriodControls();
      loadAnalytics();
    });
    analyticsPeriodEl.appendChild(button);
  });
};

const loadAnalytics = async () => {
  const isRefresh = analyticsState !== null;
  const scrollY = window.scrollY;

  selectedDayDate = null;
  dayDetailState = null;
  dayDetailEl.hidden = false;
  dayDetailDateEl.textContent = "day detail";
  dayDetailMetricsEl.innerHTML = "";
  dayDetailSessionsEl.innerHTML = "";
  dayDetailPlaceholderEl.hidden = false;
  dayPrevButton.disabled = true;
  dayNextButton.disabled = true;

  if (!isRefresh) {
    analyticsLoadingEl.hidden = true;
    analyticsContentEl.hidden = false;
    analyticsSubtitleEl.textContent = "loading analytics...";
    renderAnalyticsSkeletons();
  }

  const res = await fetch(`/api/analytics?${buildParams()}`);
  const data = await res.json();
  analyticsState = data;
  analyticsSubtitleEl.textContent = `${data.total_sessions} sessions analyzed`;
  renderAnalyticsSummary(data);
  renderHeatmap(data);
  renderHeatmapHint(data);
  renderTopTools(data);
  renderHistogram(analyticsDurationEl, data.duration_buckets);
  renderHistogram(analyticsCostEl, data.cost_buckets);
  renderModelComparison(data);
  renderProviderSplit(data);
  renderAnalyticsPeriodControls();

  // Load AI insights via facets
  loadFacetInsights();

  analyticsLoadingEl.hidden = true;
  analyticsContentEl.hidden = false;

  if (isRefresh) {
    requestAnimationFrame(() => window.scrollTo(0, scrollY));
  }
};

let facetPollTimer = null;

const loadFacetInsights = async () => {
  if (facetPollTimer) {
    clearInterval(facetPollTimer);
    facetPollTimer = null;
  }

  try {
    const res = await fetch("/api/facets");
    const data = await res.json();
    facetsState = data;

    const hasData = data &&
      (Object.keys(data.goal_distribution || {}).length > 0 ||
       Object.keys(data.outcome_distribution || {}).length > 0);

    if (hasData) {
      renderFacetInsights(data);
    } else {
      // Check if backfill is in progress
      const statusRes = await fetch("/api/facets/status");
      const status = await statusRes.json();

      if (status.total > 0) {
        insightsGoalsEl.innerHTML = "";
        insightsOutcomesEl.innerHTML = "";
        insightsFrictionEl.innerHTML = "";
        insightsTypesEl.innerHTML = "";
        insightsSummariesEl.innerHTML = "";

        const progress = document.createElement("div");
        progress.className = "insights-progress";
        progress.textContent = `analyzing sessions... ${status.done} of ${status.total}`;
        insightsGoalsEl.appendChild(progress);

        // Poll for progress
        facetPollTimer = setInterval(async () => {
          try {
            const pollStatus = await fetch("/api/facets/status");
            const pollData = await pollStatus.json();
            const progressEl = insightsGoalsEl.querySelector(".insights-progress");
            if (progressEl) {
              progressEl.textContent = `analyzing sessions... ${pollData.done} of ${pollData.total}`;
            }
            if (pollData.done >= pollData.total) {
              clearInterval(facetPollTimer);
              facetPollTimer = null;
              const sy = window.scrollY;
              const facetRes = await fetch("/api/facets");
              const facetData = await facetRes.json();
              facetsState = facetData;
              renderFacetInsights(facetData);
              requestAnimationFrame(() => window.scrollTo(0, sy));
            }
          } catch {
            clearInterval(facetPollTimer);
            facetPollTimer = null;
          }
        }, 3000);
      }
    }
  } catch {
    // facets not available
  }
};

const loadOverview = async () => {
  const scrollY = window.scrollY;
  const isRefresh = overviewState !== null;
  const res = await fetch(`/api/overview?${buildParams()}`);
  const data = await res.json();
  overviewState = data;
  const filterBits = [];
  if (filters.status) filterBits.push(`status ${filters.status}`);
  if (filters.project) filterBits.push(`project ${filters.project}`);
  if (filters.periodEnabled) {
    const label = filters.period === "90d" ? "3M" : filters.period === "180d" ? "6M" : "30d";
    filterBits.push(`period ${label}`);
  }
  const filterText = filterBits.length ? ` \u00B7 ${filterBits.join(" \u00B7 ")}` : "";
  sessionCountEl.textContent = `${data.sessions.length} sessions \u00B7 ${formatDuration(data.total_duration_ns)} total time${filterText}`;
  statusLabelEl.textContent = filters.status || "all";

  // Populate project dropdown from session data
  const projects = [...new Set(data.sessions.map((s) => s.project).filter(Boolean))].sort();
  projectSelect.innerHTML = '<option value="">all</option>';
  projects.forEach((p) => {
    const opt = document.createElement("option");
    opt.value = p;
    opt.textContent = p;
    projectSelect.appendChild(opt);
  });
  projectSelect.value = filters.project;

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
  if (isRefresh) {
    requestAnimationFrame(() => window.scrollTo(0, scrollY));
  }
};

const loadSession = async (sessionId, keepMessage) => {
  if (!sessionEntryView) {
    sessionEntryView = currentView;
  }
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
  overviewViewEl.hidden = view !== "overview";
  sessionViewEl.hidden = view !== "session";
  analyticsViewEl.hidden = view !== "analytics";
};

const setAnalyticsTab = (tab) => {
  activeAnalyticsTab = tab;
  const panels = {
    activity: document.getElementById("tab-activity"),
    distribution: document.getElementById("tab-distribution"),
    insights: document.getElementById("tab-insights"),
    summaries: document.getElementById("tab-summaries"),
  };
  Object.entries(panels).forEach(([key, el]) => {
    if (el) el.hidden = key !== tab;
  });
  document.querySelectorAll(".analytics-tab").forEach((btn) => {
    btn.classList.toggle("analytics-tab--active", btn.dataset.tab === tab);
  });
};

const backToOverview = () => {
  const returnTo = sessionEntryView;
  selectedSessionId = null;
  selectedMessageIndex = 0;
  sessionDetailState = null;
  sessionEntryView = null;
  detailEl.innerHTML = "";
  if (returnTo === "analytics") {
    setView("analytics");
  } else {
    setView("overview");
    window.history.pushState({}, "", "/");
    if (overviewState) {
      renderSessions(overviewState);
    }
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
    case "1":
    case "2":
    case "3":
    case "4":
    case "5":
    case "6":
    case "7":
    case "8":
    case "9":
      if (currentView === "analytics" && activeAnalyticsTab === "activity" && heatmapSelectableDays.length) {
        const idx = Number(event.key) - 1;
        const day = heatmapSelectableDays[idx];
        if (day) {
          selectHeatmapDay(day.date);
        }
      }
      break;
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
    case "ArrowLeft":
      if (currentView === "analytics" && selectedDayDate) {
        navigateDay(-1);
      }
      break;
    case "ArrowRight":
      if (currentView === "analytics" && selectedDayDate) {
        navigateDay(1);
      }
      break;
    case "Escape":
      if (currentView === "analytics" && selectedDayDate) {
        closeDayDetail();
      }
      break;
    case "Enter":
      if (currentView === "overview" && overviewState && overviewState.sessions[sessionIndex]) {
        loadSession(overviewState.sessions[sessionIndex].id);
      }
      break;
    case "a":
      if (currentView === "overview") {
        filters.periodEnabled = true;
        setView("analytics");
        window.history.pushState({}, "", "/analytics");
        loadAnalytics().catch(console.error);
      } else if (currentView === "analytics") {
        setView("overview");
        window.history.pushState({}, "", "/");
      }
      break;
    case "h":
      if (currentView === "session") {
        backToOverview();
      } else if (currentView === "analytics") {
        if (selectedDayDate) {
          closeDayDetail();
        } else {
          setView("overview");
          window.history.pushState({}, "", "/");
        }
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
  if (params.get("project")) filters.project = params.get("project");
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
projectSelect.addEventListener("change", () => {
  filters.project = projectSelect.value;
  loadOverview();
});

showMoreButton.addEventListener("click", () => {
  visibleSessionCount += 38;
  if (overviewState) renderSessions(overviewState);
});

// ── Skeleton loading placeholders ──

const renderSkeletonMetrics = () => {
  metricsEl.innerHTML = "";
  for (let i = 0; i < 4; i++) {
    const card = document.createElement("div");
    card.className = "skeleton-metric";
    const label = document.createElement("div");
    label.className = "skeleton skeleton-metric__label";
    const value = document.createElement("div");
    value.className = "skeleton skeleton-metric__value";
    card.appendChild(label);
    card.appendChild(value);
    metricsEl.appendChild(card);
  }
};

const renderSkeletonSessions = () => {
  sessionsEl.innerHTML = "";
  for (let i = 0; i < 8; i++) {
    const row = document.createElement("div");
    row.className = "skeleton-row";
    const sm = document.createElement("div");
    sm.className = "skeleton skeleton-row__cell--sm";
    const lg = document.createElement("div");
    lg.className = "skeleton skeleton-row__cell";
    const md = document.createElement("div");
    md.className = "skeleton skeleton-row__cell--md";
    const sm2 = document.createElement("div");
    sm2.className = "skeleton skeleton-row__cell--sm";
    row.appendChild(sm);
    row.appendChild(lg);
    row.appendChild(md);
    row.appendChild(sm2);
    sessionsEl.appendChild(row);
  }
};

const renderSkeletonCharts = () => {
  [modelsEl, statusEl].forEach((el) => {
    el.innerHTML = "";
    for (let i = 0; i < 3; i++) {
      const bar = document.createElement("div");
      bar.className = "skeleton-bar";
      const label = document.createElement("div");
      label.className = "skeleton skeleton-bar__label";
      const track = document.createElement("div");
      track.className = "skeleton skeleton-bar__track";
      bar.appendChild(label);
      bar.appendChild(track);
      el.appendChild(bar);
    }
  });
};

const renderAnalyticsSkeletons = () => {
  // Summary cards skeleton
  analyticsSummaryEl.innerHTML = "";
  for (let i = 0; i < 4; i++) {
    const card = document.createElement("div");
    card.className = "skeleton-metric";
    const label = document.createElement("div");
    label.className = "skeleton skeleton-metric__label";
    const value = document.createElement("div");
    value.className = "skeleton skeleton-metric__value";
    card.appendChild(label);
    card.appendChild(value);
    analyticsSummaryEl.appendChild(card);
  }

  // Heatmap skeleton
  analyticsHeatmapEl.innerHTML = "";
  for (let i = 0; i < 30; i++) {
    const cell = document.createElement("div");
    cell.className = "skeleton heatmap-cell";
    cell.style.opacity = "0.3";
    analyticsHeatmapEl.appendChild(cell);
  }
  heatmapLabelsEl.innerHTML = "";
  heatmapHintEl.innerHTML = "";

  // Tools skeleton
  analyticsToolsEl.innerHTML = "";
  for (let i = 0; i < 5; i++) {
    const bar = document.createElement("div");
    bar.className = "skeleton-bar";
    const label = document.createElement("div");
    label.className = "skeleton skeleton-bar__label";
    const track = document.createElement("div");
    track.className = "skeleton skeleton-bar__track";
    bar.appendChild(label);
    bar.appendChild(track);
    analyticsToolsEl.appendChild(bar);
  }

  // Distribution skeletons
  [analyticsDurationEl, analyticsCostEl].forEach((el) => {
    el.innerHTML = "";
    for (let i = 0; i < 4; i++) {
      const bar = document.createElement("div");
      bar.className = "skeleton-bar";
      const label = document.createElement("div");
      label.className = "skeleton skeleton-bar__label";
      const track = document.createElement("div");
      track.className = "skeleton skeleton-bar__track";
      bar.appendChild(label);
      bar.appendChild(track);
      el.appendChild(bar);
    }
  });

  // Model table skeleton
  analyticsModelsEl.innerHTML = "";
  for (let i = 0; i < 3; i++) {
    const row = document.createElement("div");
    row.className = "skeleton-row";
    const sm = document.createElement("div");
    sm.className = "skeleton skeleton-row__cell--sm";
    const lg = document.createElement("div");
    lg.className = "skeleton skeleton-row__cell";
    const md = document.createElement("div");
    md.className = "skeleton skeleton-row__cell--md";
    row.appendChild(sm);
    row.appendChild(lg);
    row.appendChild(md);
    analyticsModelsEl.appendChild(row);
  }

  // Provider skeleton
  analyticsProvidersEl.innerHTML = "";
  const provBar = document.createElement("div");
  provBar.className = "skeleton skeleton-bar__track";
  provBar.style.height = "16px";
  analyticsProvidersEl.appendChild(provBar);
};

renderSkeletonMetrics();
renderSkeletonCharts();
renderSkeletonSessions();

loadOverview().catch((err) => {
  sessionCountEl.textContent = "failed to load data";
  console.error(err);
});

window.addEventListener("keydown", handleKey);
backButton.addEventListener("click", backToOverview);
analyticsBackButton.addEventListener("click", () => {
  setView("overview");
  window.history.pushState({}, "", "/");
});
dayPrevButton.addEventListener("click", () => navigateDay(-1));
dayNextButton.addEventListener("click", () => navigateDay(1));
dayDetailCloseButton.addEventListener("click", closeDayDetail);
document.querySelectorAll(".analytics-tab").forEach((btn) => {
  btn.addEventListener("click", () => setAnalyticsTab(btn.dataset.tab));
});

window.addEventListener("popstate", () => {
  if (window.location.pathname.startsWith("/session/")) {
    const sessionId = window.location.pathname.replace("/session/", "");
    if (sessionId) {
      loadSession(sessionId, true);
      return;
    }
  }
  if (window.location.pathname === "/analytics") {
    filters.periodEnabled = true;
    setView("analytics");
    loadAnalytics().catch(console.error);
    return;
  }
  backToOverview();
});

if (window.location.pathname.startsWith("/session/")) {
  const sessionId = window.location.pathname.replace("/session/", "");
  if (sessionId) {
    loadSession(sessionId);
  }
} else if (window.location.pathname === "/analytics") {
  filters.periodEnabled = true;
  setView("analytics");
  loadAnalytics().catch(console.error);
} else {
  setView("overview");
}
