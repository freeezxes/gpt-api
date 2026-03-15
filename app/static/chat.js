const state = {
  messages: [],
  lastResponseId: null,
  objects: [],
  pending: false,
};

const elements = {
  statusIndicator: document.getElementById("status-indicator"),
  statusCopy: document.getElementById("status-copy"),
  statusDetails: document.getElementById("status-details"),
  storeId: document.getElementById("store-id"),
  objectId: document.getElementById("object-id"),
  timezone: document.getElementById("timezone"),
  startTime: document.getElementById("start-time"),
  endTime: document.getElementById("end-time"),
  model: document.getElementById("model"),
  useHistory: document.getElementById("use-history"),
  lastResponseId: document.getElementById("last-response-id"),
  reloadObjects: document.getElementById("reload-objects"),
  detectTimezone: document.getElementById("detect-timezone"),
  clearChat: document.getElementById("clear-chat"),
  messages: document.getElementById("messages"),
  composer: document.getElementById("composer"),
  question: document.getElementById("question"),
  sendButton: document.getElementById("send-button"),
  promptChips: Array.from(document.querySelectorAll(".prompt-chip")),
  template: document.getElementById("message-template"),
};

function formatClock(date = new Date()) {
  return new Intl.DateTimeFormat("ru-RU", {
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function setStatus(kind, copy, details) {
  elements.statusIndicator.className = `status-pill ${kind}`;
  elements.statusIndicator.textContent = kind === "ok" ? "ready" : kind === "error" ? "error" : "checking";
  elements.statusCopy.textContent = copy;
  elements.statusDetails.textContent = details;
}

function syncLastResponseId() {
  elements.lastResponseId.textContent = state.lastResponseId || "none";
}

function renderMessages() {
  elements.messages.innerHTML = "";

  if (state.messages.length === 0) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.innerHTML = `
      <strong>Чат готов к тесту.</strong><br />
      Выбери store, при необходимости зону, и задай вопрос в свободной форме.
    `;
    elements.messages.appendChild(empty);
    return;
  }

  for (const message of state.messages) {
    const node = elements.template.content.firstElementChild.cloneNode(true);
    node.classList.add(message.role);
    if (message.error) {
      node.classList.add("error");
    }

    node.querySelector(".message-role").textContent =
      message.role === "user" ? "Ты" : message.error ? "Ошибка" : "Ассистент";
    node.querySelector(".message-time").textContent = message.time;
    node.querySelector(".message-body").textContent = message.text;

    const meta = node.querySelector(".message-meta");
    meta.innerHTML = "";
    for (const item of message.meta || []) {
      const pill = document.createElement("span");
      pill.className = "meta-pill";
      pill.innerHTML = item;
      meta.appendChild(pill);
    }

    elements.messages.appendChild(node);
  }

  elements.messages.scrollTop = elements.messages.scrollHeight;
}

function appendMessage({ role, text, meta = [], error = false }) {
  state.messages.push({ role, text, meta, error, time: formatClock() });
  renderMessages();
}

function replaceLastMessage(update) {
  const last = state.messages[state.messages.length - 1];
  if (!last) {
    appendMessage(update);
    return;
  }
  state.messages[state.messages.length - 1] = { ...last, ...update, time: last.time };
  renderMessages();
}

function buildMetaFromResponse(data) {
  const meta = [];
  if (data.model) {
    meta.push(`model: <code>${escapeHtml(data.model)}</code>`);
  }
  if (data.response_id) {
    meta.push(`response: <code>${escapeHtml(data.response_id)}</code>`);
  }
  if (data.context?.tools_used?.length) {
    meta.push(`tools: <code>${escapeHtml(data.context.tools_used.join(", "))}</code>`);
  }
  if (data.context?.object_name) {
    meta.push(`object: <strong>${escapeHtml(data.context.object_name)}</strong>`);
  } else {
    meta.push(`scope: <strong>store-wide</strong>`);
  }
  return meta;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function localInputToIso(value) {
  if (!value) {
    return null;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString();
}

async function loadStatus() {
  setStatus("muted", "Проверяю backend...", "Проверка /health/dependencies");
  try {
    const storeId = Number(elements.storeId.value || 5);
    const response = await fetch(`./health/dependencies?store_id=${storeId}`);
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.detail || "health check failed");
    }

    if (data.tracker_reachable) {
      setStatus(
        "ok",
        `Backend готов, найдено зон: ${data.tracker_objects_found ?? 0}`,
        `Model default: ${data.default_model}. Tracker: ${data.tracker_api_base_url}`
      );
    } else {
      setStatus("error", "Tracker недоступен", data.tracker_error || "Unknown tracker error");
    }
  } catch (error) {
    setStatus("error", "Не удалось проверить backend", error.message);
  }
}

async function loadObjects() {
  const storeId = Number(elements.storeId.value || 0);
  if (!storeId) {
    return;
  }

  elements.reloadObjects.disabled = true;
  elements.reloadObjects.textContent = "Загрузка...";

  try {
    const response = await fetch(`./api/store-objects?store_id=${storeId}`);
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.detail || "Не удалось загрузить зоны");
    }

    state.objects = data;
    renderObjectOptions();
    await loadStatus();
  } catch (error) {
    state.objects = [];
    renderObjectOptions();
    setStatus("error", "Не загрузились зоны магазина", error.message);
  } finally {
    elements.reloadObjects.disabled = false;
    elements.reloadObjects.textContent = "Обновить зоны";
  }
}

function renderObjectOptions() {
  const current = elements.objectId.value;
  elements.objectId.innerHTML = "";

  const storeOption = document.createElement("option");
  storeOption.value = "";
  storeOption.textContent = "Весь магазин";
  elements.objectId.appendChild(storeOption);

  for (const object of state.objects) {
    const option = document.createElement("option");
    option.value = object.id;
    option.textContent = `${object.id} · ${object.name}`;
    elements.objectId.appendChild(option);
  }

  elements.objectId.value = current || "";
}

function resetConversation() {
  state.messages = [];
  state.lastResponseId = null;
  syncLastResponseId();
  renderMessages();
}

async function sendMessage() {
  if (state.pending) {
    return;
  }

  const question = elements.question.value.trim();
  if (!question) {
    elements.question.focus();
    return;
  }

  const startTime = localInputToIso(elements.startTime.value);
  const endTime = localInputToIso(elements.endTime.value);

  if ((startTime && !endTime) || (!startTime && endTime)) {
    appendMessage({
      role: "assistant",
      text: "Нужно указать и start_time, и end_time одновременно.",
      error: true,
    });
    return;
  }

  const payload = {
    store_id: Number(elements.storeId.value),
    question,
    timezone: elements.timezone.value.trim() || "UTC",
  };

  if (elements.objectId.value) {
    payload.object_id = Number(elements.objectId.value);
  }
  if (startTime && endTime) {
    payload.start_time = startTime;
    payload.end_time = endTime;
  }
  if (elements.model.value.trim()) {
    payload.model = elements.model.value.trim();
  }
  if (elements.useHistory.checked && state.lastResponseId) {
    payload.previous_response_id = state.lastResponseId;
  }

  appendMessage({ role: "user", text: question });
  appendMessage({
    role: "assistant",
    text: "Думаю над ответом",
    meta: ["<span class='loading-dots'>Запрос к backend</span>"],
  });

  state.pending = true;
  elements.sendButton.disabled = true;
  elements.question.value = "";

  try {
    const response = await fetch("./api/object-chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.detail || "Request failed");
    }

    state.lastResponseId = data.response_id || state.lastResponseId;
    syncLastResponseId();
    replaceLastMessage({
      role: "assistant",
      text: data.answer,
      meta: buildMetaFromResponse(data),
      error: false,
    });
  } catch (error) {
    replaceLastMessage({
      role: "assistant",
      text: error.message,
      meta: ["backend error"],
      error: true,
    });
  } finally {
    state.pending = false;
    elements.sendButton.disabled = false;
    elements.question.focus();
  }
}

function primeTimezone() {
  const browserTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  if (browserTimezone) {
    elements.timezone.value = browserTimezone;
  }
}

elements.reloadObjects.addEventListener("click", loadObjects);
elements.detectTimezone.addEventListener("click", primeTimezone);
elements.clearChat.addEventListener("click", resetConversation);
elements.storeId.addEventListener("change", () => {
  resetConversation();
  loadObjects();
});

elements.promptChips.forEach((chip) => {
  chip.addEventListener("click", () => {
    elements.question.value = chip.dataset.prompt || "";
    elements.question.focus();
  });
});

elements.composer.addEventListener("submit", async (event) => {
  event.preventDefault();
  await sendMessage();
});

elements.question.addEventListener("keydown", async (event) => {
  if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
    event.preventDefault();
    await sendMessage();
  }
});

primeTimezone();
syncLastResponseId();
renderMessages();
loadObjects();
