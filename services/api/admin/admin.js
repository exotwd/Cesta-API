(() => {
  "use strict";

  const state = {
    token: localStorage.getItem("cesta_admin_token") || "",
    user: null,
    view: "dashboard",
    entities: [],
    entity: "stops",
    entityPage: 1,
    entityPages: 0,
    entityPageSize: 50,
    map: null,
    mapLayer: null,
    mapStops: [],
    sources: []
  };

  const $ = (selector) => document.querySelector(selector);
  const $$ = (selector) => [...document.querySelectorAll(selector)];

  function iconRefresh() {
    if (window.lucide) window.lucide.createIcons();
  }

  async function api(path, options = {}) {
    const headers = new Headers(options.headers || {});
    if (state.token) headers.set("Authorization", `Bearer ${state.token}`);
    if (options.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
    const response = await fetch(path, { ...options, headers });
    const payload = await response.json().catch(() => ({}));
    if (response.status === 401 || response.status === 403) {
      if (path !== "/auth/login") showLogin("Administrator access is required.");
      throw new Error(payload.message || "Authentication failed");
    }
    if (!response.ok) throw new Error(payload.message || `Request failed (${response.status})`);
    return payload;
  }

  function showLogin(message = "") {
    state.token = "";
    state.user = null;
    localStorage.removeItem("cesta_admin_token");
    $("#app-shell").classList.add("hidden");
    $("#login-screen").classList.remove("hidden");
    $("#login-error").textContent = message;
  }

  function showApp() {
    $("#login-screen").classList.add("hidden");
    $("#app-shell").classList.remove("hidden");
    $("#user-name").textContent = state.user.display_name || "Administrator";
    $("#user-email").textContent = state.user.email;
    $("#user-avatar").textContent = (state.user.display_name || state.user.email || "A")[0].toUpperCase();
    iconRefresh();
  }

  function toast(message, type = "") {
    const item = document.createElement("div");
    item.className = `toast ${type}`;
    item.textContent = message;
    $("#toast-region").append(item);
    setTimeout(() => item.remove(), 4200);
  }

  function setApiStatus(ok, label = ok ? "Connected" : "Unavailable") {
    const chip = $("#api-status");
    chip.classList.toggle("error", !ok);
    chip.lastChild.textContent = label;
  }

  function formatNumber(value) {
    return new Intl.NumberFormat("en-US").format(Number(value || 0));
  }

  function formatBytes(value) {
    const bytes = Number(value || 0);
    if (!bytes) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    return `${(bytes / 1024 ** index).toFixed(index ? 1 : 0)} ${units[index]}`;
  }

  function formatDate(value) {
    if (!value) return "—";
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function compactValue(value) {
    if (value === null || value === undefined) return "—";
    if (typeof value === "boolean") return value ? "Yes" : "No";
    if (typeof value === "object") return JSON.stringify(value);
    if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}T/.test(value)) return formatDate(value);
    return String(value);
  }

  function badge(value) {
    const normalized = String(value || "").toLowerCase();
    const type = ["success", "complete", "completed", "enabled", "ok"].some((item) => normalized.includes(item))
      ? "success"
      : ["error", "failed", "disabled", "unresolved"].some((item) => normalized.includes(item))
        ? "danger"
        : ["warning", "running", "pending", "partial"].some((item) => normalized.includes(item))
          ? "warning"
          : "";
    return `<span class="badge ${type}">${escapeHtml(value ?? "—")}</span>`;
  }

  function renderTable(container, rows, columns, options = {}) {
    const target = typeof container === "string" ? $(container) : container;
    if (!rows?.length) {
      target.innerHTML = `<div class="empty-state">${escapeHtml(options.empty || "No records found")}</div>`;
      return;
    }
    const selectedColumns = columns?.length ? columns : Object.keys(rows[0]);
    target.innerHTML = `
      <table>
        <thead><tr>${selectedColumns.map((column) => `<th>${escapeHtml(column.replaceAll("_", " "))}</th>`).join("")}</tr></thead>
        <tbody>
          ${rows.map((row, index) => `
            <tr class="${options.clickable ? "clickable-row" : ""}" data-row-index="${index}">
              ${selectedColumns.map((column) => {
                const value = row[column];
                const text = compactValue(value);
                const display = options.badgeColumns?.includes(column)
                  ? badge(text)
                  : `<span class="cell-value" title="${escapeHtml(text)}">${escapeHtml(text)}</span>`;
                return `<td>${display}</td>`;
              }).join("")}
            </tr>
          `).join("")}
        </tbody>
      </table>`;
    if (options.clickable) {
      target.querySelectorAll("tbody tr").forEach((rowElement) => {
        rowElement.addEventListener("click", () => openDetail(options.title || "Record", rows[Number(rowElement.dataset.rowIndex)]));
      });
    }
  }

  function openDetail(title, record, subtitle = "") {
    $("#detail-title").textContent = title;
    $("#detail-subtitle").textContent = subtitle;
    $("#detail-json").textContent = JSON.stringify(record, null, 2);
    $("#detail-dialog").showModal();
  }

  async function loadEntities() {
    const payload = await api("/admin/data");
    state.entities = payload.entities || [];
    const select = $("#entity-select");
    select.innerHTML = state.entities.map((entity) => `<option value="${escapeHtml(entity.key)}">${escapeHtml(entity.label)}</option>`).join("");
    if (state.entities.some((entity) => entity.key === state.entity)) select.value = state.entity;
  }

  async function loadDashboard() {
    $("#dashboard-metrics").innerHTML = '<div class="metric skeleton"></div>'.repeat(4);
    const [stats, quality] = await Promise.all([
      api("/admin/database/stats"),
      api("/admin/data-quality")
    ]);
    setApiStatus(Boolean(stats.database_available));
    const tableLookup = Object.fromEntries((stats.tables || []).map((table) => [table.table, table]));
    $("#dashboard-metrics").innerHTML = [
      metric("Stops", tableLookup.stops?.rows, `${tableLookup.stops?.total_size_pretty || "—"} stored`),
      metric("Routes", tableLookup.routes?.rows, `${tableLookup.routes?.total_size_pretty || "—"} stored`),
      metric("Trips", tableLookup.trips?.rows, `${tableLookup.trips?.total_size_pretty || "—"} stored`),
      metric("Stop times", tableLookup.stop_times?.rows, stats.database?.total_size_pretty || "Database size unavailable")
    ].join("");
    renderTable("#dashboard-tables", (stats.tables || []).slice(0, 12), ["table", "rows", "total_size_pretty"]);
    renderImportActivity("#dashboard-imports", (stats.latest_imports || []).slice(0, 6));
    $("#dashboard-sources").innerHTML = (stats.source_feeds || []).map((source) => `
      <div class="source-item">
        <div><strong>${escapeHtml(source.name)}</strong><span>${formatNumber(source.counts?.stops)} stops · ${formatNumber(source.counts?.trips)} trips</span></div>
        <span class="badge">${escapeHtml(source.type)}</span>
      </div>`).join("") || '<div class="empty-state">No source feeds</div>';
    $("#dashboard-quality").innerHTML = [
      qualityStat(quality.unresolved_stops, "Unresolved active stops"),
      qualityStat(quality.duplicate_stop_groups, "Duplicate stop groups"),
      qualityStat((quality.latest_issues || []).length, "Recent validation issues")
    ].join("");
    iconRefresh();
  }

  function metric(label, value, detail) {
    return `<div class="metric"><span class="metric-label">${escapeHtml(label)}</span><strong class="metric-value">${formatNumber(value)}</strong><span class="metric-detail">${escapeHtml(detail)}</span></div>`;
  }

  function qualityStat(value, label) {
    return `<div class="quality-stat"><strong>${formatNumber(value)}</strong><span>${escapeHtml(label)}</span></div>`;
  }

  function renderImportActivity(container, imports) {
    $(container).innerHTML = imports.map((item) => `
      <button class="activity-item text-button import-detail" data-id="${escapeHtml(item.id)}">
        <div><strong>${escapeHtml(item.source)}</strong><span>${formatDate(item.started_at)}</span></div>
        ${badge(item.status)}
      </button>`).join("") || '<div class="empty-state">No import runs</div>';
    $$(container + " .import-detail").forEach((button) => {
      button.addEventListener("click", () => loadImportDetail(button.dataset.id));
    });
  }

  async function loadEntityRows(resetPage = false) {
    if (resetPage) state.entityPage = 1;
    state.entity = $("#entity-select").value || state.entity;
    state.entityPageSize = Number($("#entity-page-size").value || 50);
    const query = new URLSearchParams({
      page: String(state.entityPage),
      page_size: String(state.entityPageSize),
      q: $("#entity-search").value.trim()
    });
    $("#entity-table").innerHTML = '<div class="loading-state">Loading records…</div>';
    const payload = await api(`/admin/data/${encodeURIComponent(state.entity)}?${query}`);
    state.entityPages = payload.pagination?.total_pages || 0;
    $("#entity-title").textContent = payload.label || state.entity;
    $("#entity-count").textContent = `${formatNumber(payload.pagination?.total_rows)} records`;
    $("#page-status").textContent = state.entityPages ? `Page ${state.entityPage} of ${state.entityPages}` : "No pages";
    $("#page-previous").disabled = state.entityPage <= 1;
    $("#page-next").disabled = !state.entityPages || state.entityPage >= state.entityPages;
    const rows = payload.rows || [];
    const preferred = ["id", "name", "source_feed_id", "source_id", "status", "type", "mode", "short_name", "trip_id", "stop_id", "created_at"];
    const available = [...new Set(rows.flatMap((row) => Object.keys(row)))];
    const columns = [...preferred.filter((column) => available.includes(column)), ...available.filter((column) => !preferred.includes(column))].slice(0, 10);
    renderTable("#entity-table", rows, columns, { clickable: true, title: payload.label, empty: "No matching records" });
  }

  function ensureMap() {
    if (state.map || !window.L) return;
    state.map = L.map("stop-map", { preferCanvas: true }).setView([49.82, 15.48], 7);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap contributors"
    }).addTo(state.map);
    state.mapLayer = L.layerGroup().addTo(state.map);
  }

  async function loadMapStops() {
    ensureMap();
    if (!state.map) {
      toast("The map library could not be loaded.", "error");
      return;
    }
    const bounds = state.map.getBounds();
    const query = new URLSearchParams({
      q: $("#map-search").value.trim(),
      source_feed_id: $("#map-source").value,
      min_lat: String(bounds.getSouth()),
      min_lon: String(bounds.getWest()),
      max_lat: String(bounds.getNorth()),
      max_lon: String(bounds.getEast()),
      limit: "5000"
    });
    const payload = await api(`/admin/map/stops?${query}`);
    state.mapStops = payload.stops || [];
    state.mapLayer.clearLayers();
    const renderer = L.canvas({ padding: 0.5 });
    state.mapStops.forEach((stop) => {
      const marker = L.circleMarker([stop.lat, stop.lon], {
        renderer,
        radius: stop.platform_code ? 4 : 6,
        weight: 1,
        color: "#ffffff",
        fillColor: stop.coordinate_confidence === "unresolved" ? "#b42318" : "#1261a0",
        fillOpacity: 0.82
      }).bindPopup(`
        <div class="popup-title">${escapeHtml(stop.name)}</div>
        <div class="popup-meta">
          ${escapeHtml(stop.municipality || stop.region || "")}<br>
          ${stop.platform_code ? `Platform ${escapeHtml(stop.platform_code)}<br>` : ""}
          ${escapeHtml(stop.source_feed_id || "Unknown source")}<br>
          ${escapeHtml(stop.id)}
        </div>`);
      marker.stopRecord = stop;
      marker.addTo(state.mapLayer);
    });
    $("#map-result-count").textContent = `${formatNumber(state.mapStops.length)} stops${payload.truncated ? " · result limit reached" : ""}`;
    $("#map-result-list").innerHTML = state.mapStops.slice(0, 250).map((stop, index) => `
      <button class="map-result" data-index="${index}">
        <strong>${escapeHtml(stop.name)}${stop.platform_code ? ` · ${escapeHtml(stop.platform_code)}` : ""}</strong>
        <span>${escapeHtml(stop.municipality || stop.region || stop.source_feed_id || "")}</span>
      </button>`).join("") || '<div class="empty-state">No stops in the visible area</div>';
    $$("#map-result-list .map-result").forEach((button) => {
      button.addEventListener("click", () => {
        const stop = state.mapStops[Number(button.dataset.index)];
        state.map.setView([stop.lat, stop.lon], Math.max(state.map.getZoom(), 14));
        state.mapLayer.eachLayer((layer) => {
          if (layer.stopRecord?.id === stop.id) layer.openPopup();
        });
      });
    });
  }

  async function loadImports() {
    const payload = await api("/admin/imports");
    const imports = payload.imports || [];
    renderTable("#imports-table", imports, ["source", "status", "started_at", "finished_at", "id"], {
      clickable: true,
      title: "Import run",
      badgeColumns: ["status"],
      empty: "No import runs"
    });
  }

  async function loadImportDetail(id) {
    try {
      const payload = await api(`/admin/imports/${encodeURIComponent(id)}`);
      openDetail("Import run", payload, id);
    } catch (error) {
      toast(error.message, "error");
    }
  }

  async function loadQuality() {
    const [quality, unresolved] = await Promise.all([
      api("/admin/data-quality"),
      api("/admin/unmatched-stops")
    ]);
    const severity = Object.fromEntries((quality.validation_issue_counts || []).map((item) => [item.severity, item.count]));
    $("#quality-metrics").innerHTML = [
      metric("Errors", severity.error || 0, "Validation errors"),
      metric("Warnings", severity.warning || 0, "Validation warnings"),
      metric("Unresolved stops", quality.unresolved_stops || 0, "Active stops needing coordinates")
    ].join("");
    renderTable("#quality-codes", quality.issue_codes || [], ["code", "severity", "count"], { badgeColumns: ["severity"] });
    renderTable("#quality-issues", quality.latest_issues || [], ["severity", "code", "message", "source_feed_id", "created_at"], {
      clickable: true,
      title: "Validation issue",
      badgeColumns: ["severity"]
    });
    renderTable("#unresolved-stops", unresolved.stops || [], ["name", "municipality", "source_ids", "coordinate_confidence", "id"], {
      clickable: true,
      title: "Unresolved stop",
      badgeColumns: ["coordinate_confidence"]
    });
  }

  async function loadSources() {
    const payload = await api("/admin/source-feeds");
    state.sources = payload.sources || [];
    const sourceOptions = '<option value="">All sources</option>' + state.sources.map((source) => `<option value="${escapeHtml(source.id)}">${escapeHtml(source.name || source.id)}</option>`).join("");
    $("#map-source").innerHTML = sourceOptions;
    $("#source-editor-list").innerHTML = state.sources.map((source, index) => `
      <form class="source-editor" data-index="${index}">
        <div class="source-identity">
          <strong>${escapeHtml(source.name || source.id)}</strong>
          <span>${escapeHtml(source.id)} · ${escapeHtml(source.type || "source")}</span>
        </div>
        <label class="field">URL<input name="url" value="${escapeHtml(source.url || "")}"></label>
        <label class="field">Priority<input name="priority" type="number" value="${escapeHtml(source.priority ?? 100)}"></label>
        <label class="toggle-field"><input name="enabled" type="checkbox" ${source.enabled !== false ? "checked" : ""}>Enabled</label>
        <button class="button" type="submit"><i data-lucide="save"></i>Save</button>
      </form>`).join("") || '<div class="empty-state">No source feeds</div>';
    $$(".source-editor").forEach((form) => {
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        const source = state.sources[Number(form.dataset.index)];
        const formData = new FormData(form);
        try {
          await api(`/admin/source-feeds/${encodeURIComponent(source.id)}`, {
            method: "PATCH",
            body: JSON.stringify({
              url: String(formData.get("url") || ""),
              priority: Number(formData.get("priority")),
              enabled: formData.get("enabled") === "on"
            })
          });
          toast(`${source.name || source.id} updated`);
          await loadSources();
        } catch (error) {
          toast(error.message, "error");
        }
      });
    });
    iconRefresh();
  }

  async function loadView(view) {
    try {
      if (view === "dashboard") await loadDashboard();
      if (view === "data") {
        if (!state.entities.length) await loadEntities();
        await loadEntityRows();
      }
      if (view === "map") {
        ensureMap();
        setTimeout(() => state.map?.invalidateSize(), 20);
        if (!state.sources.length) await loadSources();
        if (!state.mapStops.length) await loadMapStops();
      }
      if (view === "imports") await loadImports();
      if (view === "quality") await loadQuality();
      if (view === "sources") await loadSources();
      setApiStatus(true);
    } catch (error) {
      setApiStatus(false, "Request failed");
      toast(error.message, "error");
    }
  }

  function navigate(view) {
    const labels = {
      dashboard: ["Overview", "Transport database status and recent activity"],
      data: ["Data browser", "Search and inspect every managed entity"],
      map: ["Stop map", "Inspect imported stop coordinates and source coverage"],
      imports: ["Imports", "Pipeline history and source summaries"],
      quality: ["Data quality", "Validation issues and unresolved records"],
      sources: ["Source feeds", "Feed configuration and import priority"]
    };
    state.view = view;
    $$(".view").forEach((section) => section.classList.toggle("active", section.id === `view-${view}`));
    $$(".nav-item").forEach((button) => button.classList.toggle("active", button.dataset.view === view));
    $("#page-title").textContent = labels[view][0];
    $("#page-subtitle").textContent = labels[view][1];
    $(".sidebar").classList.remove("open");
    loadView(view);
  }

  function debounce(callback, delay = 300) {
    let timeout;
    return (...args) => {
      clearTimeout(timeout);
      timeout = setTimeout(() => callback(...args), delay);
    };
  }

  function bindEvents() {
    $("#login-form").addEventListener("submit", async (event) => {
      event.preventDefault();
      $("#login-error").textContent = "";
      try {
        const payload = await api("/auth/login", {
          method: "POST",
          body: JSON.stringify({
            email: $("#login-email").value.trim(),
            password: $("#login-password").value,
            device_name: "Cesta data admin"
          })
        });
        state.token = payload.access_token;
        localStorage.setItem("cesta_admin_token", state.token);
        state.user = payload.user;
        if (!state.user.roles?.some((role) => role === "admin" || role === "data_admin")) {
          showLogin("This account does not have an administrator role.");
          return;
        }
        showApp();
        await loadEntities();
        await loadSources();
        navigate("dashboard");
      } catch (error) {
        $("#login-error").textContent = error.message;
      }
    });
    $("#logout-button").addEventListener("click", () => showLogin());
    $("#mobile-menu").addEventListener("click", () => $(".sidebar").classList.toggle("open"));
    $$(".nav-item").forEach((button) => button.addEventListener("click", () => navigate(button.dataset.view)));
    $$("[data-jump]").forEach((button) => button.addEventListener("click", () => navigate(button.dataset.jump)));
    $("#refresh-button").addEventListener("click", () => loadView(state.view));
    $("#entity-select").addEventListener("change", () => loadEntityRows(true));
    $("#entity-page-size").addEventListener("change", () => loadEntityRows(true));
    $("#entity-reload").addEventListener("click", () => loadEntityRows());
    $("#entity-search").addEventListener("input", debounce(() => loadEntityRows(true), 350));
    $("#page-previous").addEventListener("click", () => {
      if (state.entityPage > 1) {
        state.entityPage -= 1;
        loadEntityRows();
      }
    });
    $("#page-next").addEventListener("click", () => {
      if (state.entityPage < state.entityPages) {
        state.entityPage += 1;
        loadEntityRows();
      }
    });
    $("#map-load").addEventListener("click", loadMapStops);
    $("#map-search").addEventListener("keydown", (event) => {
      if (event.key === "Enter") loadMapStops();
    });
    $("#map-source").addEventListener("change", loadMapStops);
    $("#detail-close").addEventListener("click", () => $("#detail-dialog").close());
    $("#command-close").addEventListener("click", () => $("#command-dialog").close());
    $("#show-import-command").addEventListener("click", () => $("#command-dialog").showModal());
  }

  async function bootstrap() {
    bindEvents();
    iconRefresh();
    if (!state.token) {
      showLogin();
      return;
    }
    try {
      state.user = await api("/auth/me");
      if (!state.user.roles?.some((role) => role === "admin" || role === "data_admin")) {
        showLogin("This account does not have an administrator role.");
        return;
      }
      showApp();
      await Promise.all([loadEntities(), loadSources()]);
      navigate("dashboard");
    } catch (error) {
      showLogin(error.message);
    }
  }

  bootstrap();
})();
