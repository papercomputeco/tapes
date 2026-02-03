const metricsEl = document.getElementById("metrics");
const sessionsEl = document.getElementById("sessions");
const modelsEl = document.getElementById("models");
const detailEl = document.getElementById("detail");
const sessionCountEl = document.getElementById("session-count");

const formatCost = (value) => `$${value.toFixed(3)}`;
const formatTokens = (value) => {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return `${value}`;
};
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

const statusClass = (status) => {
  if (status === "completed") return "status--completed";
  if (status === "failed") return "status--failed";
  return "status--abandoned";
};

const renderMetrics = (data) => {
  metricsEl.innerHTML = "";
  const items = [
    { label: "total spend", value: formatCost(data.total_cost), sub: `avg ${formatCost(data.total_cost / Math.max(data.sessions.length, 1))}` },
    { label: "tokens used", value: formatTokens(data.total_tokens), sub: `${formatTokens(data.input_tokens)} in / ${formatTokens(data.output_tokens)} out` },
    { label: "agent time", value: formatDuration(data.total_duration_ns), sub: `${formatDuration(data.total_duration_ns / Math.max(data.sessions.length, 1))} avg` },
    { label: "tool calls", value: data.total_tool_calls, sub: `${(data.total_tool_calls / Math.max(data.sessions.length, 1)).toFixed(1)} avg` },
    { label: "success rate", value: `${Math.round(data.success_rate * 100)}%`, sub: `${data.completed}/${data.sessions.length}` },
  ];

  items.forEach((item) => {
    const card = document.createElement("div");
    card.className = "metric";
    card.innerHTML = `
      <div class="metric__label">${item.label}</div>
      <div class="metric__value">${item.value}</div>
      <div class="metric__sub">${item.sub}</div>
    `;
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
    const percent = Math.round((model.total_cost / max) * 100);
    row.innerHTML = `
      <div>
        <div class="model-row__name">${model.model}</div>
        <div class="model-row__bar"><div class="model-row__fill" style="width: ${percent}%"></div></div>
      </div>
      <div>${formatCost(model.total_cost)}</div>
    `;
    modelsEl.appendChild(row);
  });
};

const renderSessions = (data) => {
  sessionsEl.innerHTML = "";
  const header = document.createElement("div");
  header.className = "table__row table__header";
  header.innerHTML = "<div>label</div><div>model</div><div>duration</div><div>cost</div><div>status</div>";
  sessionsEl.appendChild(header);

  data.sessions.forEach((session) => {
    const row = document.createElement("div");
    row.className = "table__row";
    row.innerHTML = `
      <div>${session.label}</div>
      <div>${session.model || "unknown"}</div>
      <div>${formatDuration(session.duration_ns)}</div>
      <div>${formatCost(session.total_cost)}</div>
      <div class="status ${statusClass(session.status)}">${session.status}</div>
    `;
    row.addEventListener("click", () => loadSession(session.id));
    sessionsEl.appendChild(row);
  });
};

const renderSessionDetail = (detail) => {
  detailEl.innerHTML = "";
  const meta = document.createElement("div");
  meta.className = "detail__meta";
  meta.innerHTML = `
    <div class="detail__meta-item">model<span>${detail.summary.model || "unknown"}</span></div>
    <div class="detail__meta-item">duration<span>${formatDuration(detail.summary.duration_ns)}</span></div>
    <div class="detail__meta-item">input cost<span>${formatCost(detail.summary.input_cost)}</span></div>
    <div class="detail__meta-item">output cost<span>${formatCost(detail.summary.output_cost)}</span></div>
  `;

  const timeline = document.createElement("div");
  timeline.className = "timeline";
  detail.messages.forEach((msg) => {
    const row = document.createElement("div");
    row.className = "timeline__row";
    row.innerHTML = `
      <div>${new Date(msg.timestamp).toLocaleTimeString()}</div>
      <div>${msg.role}</div>
      <div>${formatTokens(msg.total_tokens)} tok</div>
      <div>${formatCost(msg.total_cost)}</div>
    `;
    timeline.appendChild(row);
  });

  detailEl.appendChild(meta);
  detailEl.appendChild(timeline);
};

const loadOverview = async () => {
  const res = await fetch("/api/overview");
  const data = await res.json();
  sessionCountEl.textContent = `${data.sessions.length} sessions`;
  renderMetrics(data);
  renderModels(data);
  renderSessions(data);
};

const loadSession = async (sessionId) => {
  const res = await fetch(`/api/session/${sessionId}`);
  const data = await res.json();
  renderSessionDetail(data);
};

loadOverview().catch((err) => {
  sessionCountEl.textContent = "failed to load data";
  console.error(err);
});
