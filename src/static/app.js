const summaryPills = document.getElementById("summaryPills");
const searchInput = document.getElementById("searchInput");
const searchResults = document.getElementById("searchResults");
const graphSvg = document.getElementById("graphSvg");
const inspector = document.getElementById("inspector");
const chatLog = document.getElementById("chatLog");
const chatForm = document.getElementById("chatForm");
const chatInput = document.getElementById("chatInput");
const sqlBox = document.getElementById("sqlBox");
const resultsTable = document.getElementById("resultsTable");
const queryMode = document.getElementById("queryMode");
const promptRow = document.getElementById("promptRow");
const detailToggle = document.getElementById("detailToggle");

let currentHighlights = new Set();
let currentCenterNodeId = null;
let currentIncludeDetails = false;
const palette = {
  "Customer": "#f59e0b",
  "Sales Order": "#2563eb",
  "Sales Order Item": "#60a5fa",
  "Product": "#10b981",
  "Plant": "#14b8a6",
  "Delivery": "#8b5cf6",
  "Billing Document": "#ef4444",
  "Billing Item": "#f87171",
  "Journal Entry": "#111827",
  "Payment": "#ec4899",
  "Address": "#7c3aed",
  "Delivery Item": "#a78bfa",
  "Schedule Line": "#0f766e",
  "Storage Location": "#14b8a6",
  "Product Plant": "#84cc16",
};

async function getJSON(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) throw new Error(`Request failed: ${response.status}`);
  return response.json();
}

async function loadSummary() {
  const summary = await getJSON("/api/summary");
  summaryPills.innerHTML = Object.entries(summary)
    .map(([key, value]) => `<span class="pill">${key.replaceAll("_", " ")}: ${value}</span>`)
    .join("");
}

function appendMessage(role, text, extra = "") {
  const article = document.createElement("article");
  article.className = `message ${role}`;
  article.innerHTML = `<p>${text}</p>${extra ? `<small>${extra}</small>` : ""}`;
  chatLog.appendChild(article);
  chatLog.scrollTop = chatLog.scrollHeight;
}

function renderTable(rows) {
  if (!rows.length) {
    resultsTable.innerHTML = "<tr><td>No rows returned.</td></tr>";
    return;
  }
  const columns = Object.keys(rows[0]);
  resultsTable.innerHTML =
    `<tr>${columns.map((column) => `<th>${column}</th>`).join("")}</tr>` +
    rows.map((row) => `<tr>${columns.map((column) => `<td>${row[column] ?? ""}</td>`).join("")}</tr>`).join("");
}

function renderInspector(node) {
  const metadata = Object.entries(node.metadata || {})
    .map(([key, value]) => `<dt>${key}</dt><dd>${value ?? ""}</dd>`)
    .join("");
  inspector.innerHTML = `<h3>${node.label}</h3><span class="chip">${node.entity_type}</span><dl>${metadata}</dl>`;
  inspector.appendChild(detailToggle);
}

function polarPoint(cx, cy, radius, angle) {
  return { x: cx + radius * Math.cos(angle), y: cy + radius * Math.sin(angle) };
}

function renderGraph(payload) {
  const { nodes, edges, center, include_details, hidden_count } = payload;
  const nodeMap = new Map(nodes.map((node) => [node.node_id, node]));
  currentCenterNodeId = center;
  currentIncludeDetails = Boolean(include_details);
  const positions = new Map();
  positions.set(center, { x: 450, y: 310 });

  nodes.filter((node) => node.node_id !== center).forEach((node, index, list) => {
    const angle = (Math.PI * 2 * index) / Math.max(list.length, 1);
    const radius = 190 + (index % 3) * 56;
    positions.set(node.node_id, polarPoint(450, 310, radius, angle));
  });

  const edgeMarkup = edges.map((edge) => {
    const source = positions.get(edge.source_id);
    const target = positions.get(edge.target_id);
    if (!source || !target) return "";
    const reason = edge.metadata?.reason || "";
    const edgeClass = reason.toLowerCase().includes("direct")
      ? "direct"
      : edge.relation_type.startsWith("ORDER_TO_") || edge.relation_type.startsWith("DELIVERY_TO_") || edge.relation_type.startsWith("BILLING_TO_") || edge.relation_type.startsWith("JOURNAL_TO_")
        ? "inferred"
        : "direct";
    const showLabel = edge.relation_type.includes("ORDER_TO") || edge.relation_type.includes("DELIVERY_TO") || edge.relation_type.includes("BILLING_TO") || edge.relation_type.includes("JOURNAL_TO") || edge.relation_type === "HAS_ITEM" || edge.relation_type === "HAS_DELIVERY_ITEM" || edge.relation_type === "HAS_BILLING_ITEM";
    const isHighlighted = currentHighlights.has(edge.source_id) && currentHighlights.has(edge.target_id);
    return `<line class="edge-line ${edgeClass} ${isHighlighted ? "highlighted-edge" : ""}" x1="${source.x}" y1="${source.y}" x2="${target.x}" y2="${target.y}"></line>
      ${showLabel ? `<text class="edge-label" x="${(source.x + target.x) / 2}" y="${(source.y + target.y) / 2}">${edge.relation_type}</text>` : ""}`;
  }).join("");

  const nodeMarkup = nodes.map((node) => {
    const position = positions.get(node.node_id);
    const fill = palette[node.entity_type] || "#94a3b8";
    const highlighted = currentHighlights.has(node.node_id) ? "highlighted" : "";
    return `<g class="node" data-node-id="${node.node_id}">
      <circle class="node-circle ${highlighted}" cx="${position.x}" cy="${position.y}" r="${node.node_id === center ? 30 : 20}" fill="${fill}"></circle>
      <text class="node-label" x="${position.x}" y="${position.y + (node.node_id === center ? 48 : 36)}">${node.label}</text>
    </g>`;
  }).join("");

  graphSvg.innerHTML = edgeMarkup + nodeMarkup;
  detailToggle.hidden = !hidden_count && !currentIncludeDetails;
  detailToggle.textContent = currentIncludeDetails ? "Hide Granular Detail" : hidden_count ? `Show Granular Detail (${hidden_count} hidden)` : "Show Granular Detail";
  graphSvg.querySelectorAll(".node").forEach((element) => {
    element.addEventListener("click", async () => {
      const nodeId = element.getAttribute("data-node-id");
      renderInspector(nodeMap.get(nodeId));
      renderGraph(await getJSON(`/api/neighborhood/${encodeURIComponent(nodeId)}?include_details=false`));
    });
  });
}

async function selectNode(nodeId, includeDetails = false) {
  const [detail, neighborhood] = await Promise.all([
    getJSON(`/api/node/${encodeURIComponent(nodeId)}`),
    getJSON(`/api/neighborhood/${encodeURIComponent(nodeId)}?include_details=${includeDetails}`),
  ]);
  renderInspector(detail);
  renderGraph(neighborhood);
}

let searchTimer;
searchInput.addEventListener("input", () => {
  clearTimeout(searchTimer);
  const value = searchInput.value.trim();
  if (!value) {
    searchResults.classList.remove("visible");
    searchResults.innerHTML = "";
    return;
  }
  searchTimer = setTimeout(async () => {
    const rows = await getJSON(`/api/search?q=${encodeURIComponent(value)}`);
    searchResults.innerHTML = rows.map((row) => `<div class="search-item" data-node-id="${row.node_id}"><strong>${row.label}</strong><br><small>${row.entity_type} | ${row.node_id}</small></div>`).join("");
    searchResults.classList.add("visible");
    searchResults.querySelectorAll(".search-item").forEach((item) => {
      item.addEventListener("click", async () => {
        searchResults.classList.remove("visible");
        await selectNode(item.getAttribute("data-node-id"));
      });
    });
  }, 220);
});

searchInput.addEventListener("keydown", async (event) => {
  if (event.key !== "Enter") return;
  event.preventDefault();
  const first = searchResults.querySelector(".search-item");
  if (first) await selectNode(first.getAttribute("data-node-id"));
});

document.addEventListener("click", (event) => {
  if (!searchResults.contains(event.target) && event.target !== searchInput) searchResults.classList.remove("visible");
});

chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const message = chatInput.value.trim();
  if (!message) return;
  appendMessage("user", message);
  chatInput.value = "";

  const payload = await getJSON("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message }),
  });

  currentHighlights = new Set(payload.highlights || []);
  appendMessage("assistant", payload.answer, `Mode: ${payload.mode}`);
  sqlBox.textContent = payload.sql || "-- no SQL";
  queryMode.textContent = payload.mode === "deterministic" ? "Grounded Query" : payload.mode;
  renderTable(payload.rows || []);
  if (payload.highlights && payload.highlights.length) await selectNode(payload.highlights[0]);
});

promptRow.querySelectorAll(".prompt-chip").forEach((button) => {
  button.addEventListener("click", () => {
    chatInput.value = button.textContent;
    chatForm.requestSubmit();
  });
});

detailToggle.addEventListener("click", async () => {
  if (!currentCenterNodeId) return;
  await selectNode(currentCenterNodeId, !currentIncludeDetails);
});

async function init() {
  await loadSummary();
  await selectNode("billing_document:90504219");
}

init().catch((error) => appendMessage("assistant", `Startup failed: ${error.message}`));
