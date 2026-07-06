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
    entityTotalRows: 0,
    entityRows: [],
    detailRecord: null,
    detailContext: null,
    detailHistory: [],
    detailRequestId: 0,
    map: null,
    mapStops: [],
    sources: [],
    routingConfiguration: null,
    routingDefaults: null,
    routingDatabaseAvailable: false
  };

  const $ = (selector) => document.querySelector(selector);
  const $$ = (selector) => [...document.querySelectorAll(selector)];
  const ENTITY_UI = {
    import_runs: { group: "Imports", description: "Import executions, status and source summaries.", columns: ["source", "status", "started_at", "finished_at", "id"] },
    source_feeds: { group: "Sources", description: "Configured timetable and realtime data sources.", columns: ["name", "type", "mode_scope", "priority", "enabled", "id"] },
    agencies: { group: "Sources", description: "Transport agencies supplied by imported feeds.", columns: ["name", "timezone", "source_feed_id", "source_id", "id"] },
    operators: { group: "Sources", description: "Operators linked to imported services.", columns: ["name", "source_feed_id", "source_id", "id"] },
    stop_areas: { group: "Network", description: "Parent station and interchange areas.", columns: ["name", "id", "created_at"] },
    stops: { group: "Network", description: "Stops, stations, platforms, coordinates and source ownership.", columns: ["name", "municipality", "platform_code", "modes", "coordinate_confidence", "source_feed_id", "id"] },
    stop_source_ids: { group: "Network", description: "Original feed identifiers retained for each stop.", columns: ["stop_id", "source_feed_id", "original_source_id", "priority", "confidence"] },
    routes: { group: "Schedule", description: "Public transport routes and their mode and operator metadata.", columns: ["short_name", "long_name", "mode", "source_feed_id", "source_id", "is_active", "id"] },
    trips: { group: "Schedule", description: "Scheduled vehicle journeys associated with routes and services.", columns: ["headsign", "route_id", "service_id", "source_feed_id", "source_id", "id"] },
    stop_times: { group: "Schedule", description: "Ordered arrival and departure times for every trip.", columns: ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time", "platform"] },
    calendars: { group: "Schedule", description: "Regular service-day calendars and validity ranges.", columns: ["service_id", "start_date", "end_date", "monday", "friday", "saturday", "sunday"] },
    calendar_dates: { group: "Schedule", description: "Service additions and removals on specific dates.", columns: ["service_id", "date", "exception_type", "source_feed_id"] },
    transfers: { group: "Network", description: "Walking and interchange links between stops.", columns: ["from_stop_id", "to_stop_id", "min_transfer_seconds", "distance_meters", "confidence", "source"] },
    shapes: { group: "Network", description: "Geographic points defining route paths.", columns: ["shape_id", "shape_pt_sequence", "source_feed_id"] },
    realtime_updates: { group: "Realtime", description: "Latest delays, cancellations, platforms and vehicle updates.", columns: ["trip_id", "route_id", "stop_id", "delay_seconds", "cancellation_status", "source", "fetched_at"] },
    data_source_syncs: { group: "Realtime", description: "Health and freshness of automatic public-data synchronization.", columns: ["source_id", "data_kind", "status", "source_timestamp", "last_success_at", "records_written", "error_message"] },
    route_geometries: { group: "Network", description: "Current PID line geometries and validity dates from the official GeoJSON feed.", columns: ["source_route_id", "route_id", "validity", "source_feed_id", "fetched_at"] },
    manual_stop_matches: { group: "Quality", description: "Administrator-reviewed stop coordinate and identity matches.", columns: ["stop_id", "target_stop_id", "confidence", "note", "created_at", "id"] },
    validation_issues: { group: "Quality", description: "Importer and database validation findings.", columns: ["severity", "code", "message", "affected_entity", "source_feed_id", "created_at"] },
    offline_packages: { group: "Distribution", description: "Generated offline transport data packages.", columns: ["name_cs", "version", "valid_from", "valid_until", "size_bytes", "id"] },
    ticket_products_mock: { group: "Development", description: "Clearly separated mock ticket products used for development.", columns: ["name_cs", "provider", "mock", "id"] },
    users: { group: "Accounts", description: "User accounts without password hashes.", columns: ["email", "display_name", "status", "created_at", "id"] },
    user_profiles: { group: "Accounts", description: "User preferences and profile attributes.", columns: ["user_id", "locale", "created_at", "updated_at"] },
    saved_places: { group: "Accounts", description: "Locations saved by users.", columns: ["name", "type", "user_id", "updated_at", "id"] },
    favorite_stops: { group: "Accounts", description: "Stops saved as user favorites.", columns: ["user_id", "stop_id", "created_at"] },
    favorite_routes: { group: "Accounts", description: "Routes saved as user favorites.", columns: ["user_id", "route_id", "created_at"] },
    notification_preferences: { group: "Accounts", description: "Per-user notification settings.", columns: ["user_id", "type", "enabled", "updated_at"] },
    user_sessions: { group: "Accounts", description: "Active and revoked account sessions without token hashes.", columns: ["user_id", "device_name", "expires_at", "revoked_at", "created_at", "id"] },
    user_roles: { group: "Accounts", description: "Roles assigned to user accounts.", columns: ["user_id", "role", "created_at"] }
  };

  const ROUTING_FIELDS = [
    { group: "Candidate generation", key: "max_direct_candidates", label: "Direct candidate limit", type: "number", min: 1, max: 500, step: 1, unit: "journeys", detail: "Maximum direct services fetched before deduplication and scoring. Raise this for dense corridors; high values increase database work." },
    { group: "Candidate generation", key: "max_transfer_candidates", label: "Transfer candidate limit", type: "number", min: 1, max: 1000, step: 1, unit: "journeys", detail: "Maximum one-transfer combinations fetched before deduplication. This should normally exceed the direct limit because transfer combinations are more numerous." },
    { group: "Candidate generation", key: "min_transfer_seconds", label: "Minimum transfer time", type: "number", min: 60, max: 3600, step: 30, unit: "seconds", detail: "Rejects transfers with less time between arrival and onward departure. Lower values find tighter connections but increase missed-connection risk." },
    { group: "Candidate generation", key: "max_transfer_wait_seconds", label: "Maximum transfer wait", type: "number", min: 300, max: 21600, step: 300, unit: "seconds", detail: "Rejects transfer combinations whose interchange wait exceeds this value. Must be at least the minimum transfer time." },
    { group: "Candidate generation", key: "transfer_search_timeout_seconds", label: "Transfer query timeout", type: "number", min: 1, max: 60, step: 1, unit: "seconds", detail: "Maximum database time allowed for transfer discovery. On timeout, direct results are still returned with a warning." },
    { group: "Candidate generation", key: "next_day_search_from_seconds", label: "Next-day search threshold", type: "number", min: 0, max: 86399, step: 300, unit: "seconds after midnight", detail: "At or after this service-day time, today and tomorrow are searched together. 64800 is 18:00." },
    { group: "Ranking score", key: "arrival_time_weight", label: "Arrival-time weight", type: "number", min: 0, max: 10, step: 0.1, unit: "multiplier", detail: "Weight applied to absolute arrival time. A value of 1 makes earlier arrival the primary objective; 0 removes it from the score." },
    { group: "Ranking score", key: "duration_weight", label: "In-vehicle/elapsed duration weight", type: "number", min: 0, max: 10, step: 0.1, unit: "multiplier", detail: "Weight applied to journey duration. Raising it favors shorter travel once departed, even if a journey leaves later and arrives later." },
    { group: "Ranking score", key: "transfer_penalty_seconds", label: "Penalty per transfer", type: "number", min: 0, max: 14400, step: 60, unit: "score-seconds", detail: "Adds this many score-seconds for every transfer. Keep at 0 to let faster transfer journeys outrank slower direct services without a transfer penalty." },
    { group: "Result selection", key: "max_results", label: "Maximum returned results", type: "number", min: 1, max: 20, step: 1, unit: "journeys", detail: "Final response size after scoring, diversity guarantees, dominance pruning and deduplication." },
    { group: "Result selection", key: "preserve_simplest", label: "Reserve the simplest journey", type: "boolean", detail: "Keeps the best journey with the fewest transfers in the final set even when it is not the highest-scoring option." },
    { group: "Result selection", key: "preserve_each_transfer_count", label: "Represent each transfer count", type: "boolean", detail: "Reserves the best candidate for every available transfer count before remaining result slots are filled." },
    { group: "Result selection", key: "preserve_carrier_diversity", label: "Preserve carrier diversity", type: "boolean", detail: "Reserves the best option from each carrier combination. This keeps potentially cheaper operators visible when verified fares are unavailable." },
    { group: "Result selection", key: "remove_dominated", label: "Remove dominated journeys", type: "boolean", detail: "Drops a journey when another departs no earlier, arrives no later, uses no more transfers, and is strictly better on at least one of those dimensions." },
    { group: "Result selection", key: "dominate_only_same_carrier", label: "Prune only within the same carrier", type: "boolean", detail: "Limits dominance pruning to identical carrier combinations. Disable only if temporal efficiency is more important than retaining operator alternatives." }
  ];

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

  function humanizeField(value) {
    return String(value)
      .replaceAll("_", " ")
      .replace(/\b\w/g, (character) => character.toUpperCase());
  }

  function formatServiceTime(value) {
    const seconds = Number(value);
    if (!Number.isFinite(seconds)) return String(value);
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const remaining = seconds % 60;
    return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}${remaining ? `:${String(remaining).padStart(2, "0")}` : ""}`;
  }

  function renderCellValue(column, value) {
    if (value === null || value === undefined || value === "") {
      return '<span class="missing-value">Missing</span>';
    }
    if (typeof value === "boolean") return badge(value ? "Yes" : "No");
    if (Array.isArray(value)) {
      if (!value.length) return '<span class="muted-value">None</span>';
      return `<span class="value-list">${value.slice(0, 3).map((item) => `<span>${escapeHtml(item)}</span>`).join("")}${value.length > 3 ? `<small>+${value.length - 3}</small>` : ""}</span>`;
    }
    if (typeof value === "object") {
      const keys = Object.keys(value);
      return `<span class="object-value" title="${escapeHtml(JSON.stringify(value))}">${formatNumber(keys.length)} fields</span>`;
    }
    if (["arrival_time", "departure_time", "first_service_time", "last_service_time", "min_transfer_seconds", "duration_seconds"].includes(column) && Number.isFinite(Number(value))) {
      return `<span class="time-value" title="${escapeHtml(String(value))} seconds">${escapeHtml(formatServiceTime(value))}</span>`;
    }
    const text = compactValue(value);
    if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}T/.test(value)) {
      return `<span class="cell-value" title="${escapeHtml(value)}">${escapeHtml(formatDate(value))}</span>`;
    }
    if (column === "color" || column === "text_color") {
      const color = String(value).replace(/^#/, "");
      return `<span class="color-value"><span style="background:#${escapeHtml(color)}"></span>${escapeHtml(color)}</span>`;
    }
    const className = column === "id" || column.endsWith("_id") || column === "source_id" ? "cell-value id-value" : "cell-value";
    return `<span class="${className}" title="${escapeHtml(text)}">${escapeHtml(text)}</span>`;
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
        <thead><tr>${selectedColumns.map((column) => `<th>${escapeHtml(humanizeField(column))}</th>`).join("")}${options.rowAction ? '<th class="row-action-heading">Record</th>' : ""}</tr></thead>
        <tbody>
          ${rows.map((row, index) => `
            <tr class="${options.clickable ? "clickable-row" : ""}" data-row-index="${index}">
              ${selectedColumns.map((column) => {
                const value = row[column];
                const text = compactValue(value);
                const display = options.badgeColumns?.includes(column)
                  ? badge(text)
                  : renderCellValue(column, value);
                return `<td>${display}</td>`;
              }).join("")}
              ${options.rowAction ? '<td class="row-action"><button class="text-button record-view" type="button">View</button></td>' : ""}
            </tr>
          `).join("")}
        </tbody>
      </table>`;
    if (options.clickable) {
      target.querySelectorAll("tbody tr").forEach((rowElement) => {
        rowElement.addEventListener("click", () => {
          const record = rows[Number(rowElement.dataset.rowIndex)];
          if (options.onRow) {
            options.onRow(record);
          } else {
            openDetail(
              options.title || "Record",
              record,
              record.name || record.short_name || record.headsign || record.id || "",
              options.entity || ""
            );
          }
        });
      });
    }
  }

  function renderDetailFields(record) {
    state.detailRecord = record;
    $("#detail-fields").innerHTML = Object.entries(record).map(([key, value]) => `
      <div class="record-field">
        <span>${escapeHtml(humanizeField(key))}</span>
        <div>${renderCellValue(key, value)}</div>
      </div>`).join("");
    $("#detail-json").textContent = JSON.stringify(record, null, 2);
  }

  function relatedEntityTitle(entity) {
    return state.entities.find((item) => item.key === entity)?.label
      || humanizeField(entity.replace(/s$/, ""));
  }

  function recordSubtitle(record) {
    return record.name
      || record.short_name
      || record.headsign
      || record.stop_name
      || record.id
      || record.stop_id
      || "";
  }

  function openDetail(title, record, subtitle = "", entity = "") {
    state.detailHistory = [];
    showDetailContext({ title, record, subtitle, entity });
    $("#detail-dialog").showModal();
  }

  function showDetailContext(context) {
    state.detailContext = context;
    $("#detail-title").textContent = context.title;
    $("#detail-subtitle").textContent = context.subtitle || recordSubtitle(context.record);
    $("#detail-back").classList.toggle("hidden", !state.detailHistory.length);
    $("#detail-summary").classList.add("hidden");
    $("#detail-summary").innerHTML = "";
    renderDetailFields(context.record);
    loadRelatedData(context);
    iconRefresh();
  }

  function openRelatedRecord(entity, record, idField = "id") {
    const id = record[idField];
    if (!entity || !id) return;
    if (state.detailContext) state.detailHistory.push(state.detailContext);
    showDetailContext({
      title: relatedEntityTitle(entity),
      record: { ...record, id: record.id || id },
      subtitle: recordSubtitle(record),
      entity
    });
  }

  function renderRelatedTimeline(section, target) {
    target.innerHTML = `
      <div class="stop-timeline">
        ${(section.rows || []).map((stop, index) => `
          <button class="timeline-stop" type="button" data-index="${index}">
            <span class="timeline-marker">${escapeHtml(stop.stop_sequence)}</span>
            <span class="timeline-time">
              <strong>${escapeHtml(formatServiceTime(stop.departure_time))}</strong>
              ${stop.arrival_time !== stop.departure_time ? `<small>arr. ${escapeHtml(formatServiceTime(stop.arrival_time))}</small>` : ""}
            </span>
            <span class="timeline-place">
              <strong>${escapeHtml(stop.stop_name || stop.stop_id)}</strong>
              <small>${escapeHtml([stop.municipality, stop.platform || stop.platform_code ? `Platform ${stop.platform || stop.platform_code}` : ""].filter(Boolean).join(" · "))}</small>
            </span>
            <span class="timeline-link"><i data-lucide="chevron-right"></i></span>
          </button>`).join("")}
      </div>`;
    target.querySelectorAll(".timeline-stop").forEach((button) => {
      button.addEventListener("click", () => openRelatedRecord("stops", section.rows[Number(button.dataset.index)], "stop_id"));
    });
  }

  function renderRelatedCalendar(section, target) {
    const calendar = section.calendar;
    const weekdays = calendar
      ? ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
      : [];
    target.innerHTML = `
      ${calendar ? `
        <div class="service-calendar">
          <div class="calendar-range"><span>Valid from</span><strong>${escapeHtml(calendar.start_date)}</strong></div>
          <div class="calendar-range"><span>Valid until</span><strong>${escapeHtml(calendar.end_date)}</strong></div>
          <div class="weekday-list">
            ${weekdays.map((day) => `<span class="${calendar[day] ? "active" : ""}">${escapeHtml(day.slice(0, 3))}</span>`).join("")}
          </div>
        </div>` : '<div class="related-empty compact-empty">No regular calendar record.</div>'}
      ${(section.calendar_dates || []).length ? `
        <div class="calendar-exceptions">
          <strong>Calendar exceptions</strong>
          <div>${section.calendar_dates.map((item) => `<span>${escapeHtml(item.date)} · ${item.exception_type === 1 ? "Added" : "Removed"}</span>`).join("")}</div>
        </div>` : ""}`;
  }

  function renderRelatedData(payload) {
    const summary = $("#detail-summary");
    const summaryItems = payload.summary || [];
    summary.classList.toggle("hidden", !summaryItems.length);
    summary.innerHTML = summaryItems.map((item) => `
      <div><span>${escapeHtml(item.label)}</span><strong>${escapeHtml(compactValue(item.value))}</strong></div>`).join("");

    const target = $("#detail-related");
    const sections = payload.sections || [];
    if (!sections.length) {
      target.innerHTML = '<div class="related-empty">No related records were found.</div>';
      return;
    }
    target.innerHTML = sections.map((section, index) => `
      <section class="related-section">
        <div class="related-section-header">
          <div>
            <h3>${escapeHtml(section.label)}</h3>
            <p>${escapeHtml(section.description || "")}</p>
          </div>
          <span>${formatNumber(section.total)}${section.truncated ? "+" : ""}</span>
        </div>
        <div class="related-section-body" data-section-index="${index}"></div>
      </section>`).join("");
    sections.forEach((section, index) => {
      const sectionTarget = target.querySelector(`[data-section-index="${index}"]`);
      if (section.display === "timeline") {
        renderRelatedTimeline(section, sectionTarget);
      } else if (section.display === "calendar") {
        renderRelatedCalendar(section, sectionTarget);
      } else {
        renderTable(sectionTarget, section.rows || [], section.columns || [], {
          clickable: Boolean(section.entity),
          rowAction: Boolean(section.entity),
          entity: section.entity,
          title: section.label,
          onRow: section.entity
            ? (record) => openRelatedRecord(section.entity, record, section.id_field || "id")
            : null,
          empty: "No related records"
        });
      }
    });
    iconRefresh();
  }

  async function loadRelatedData(context) {
    const target = $("#detail-related");
    const id = context.record.id;
    if (!["stops", "routes", "trips"].includes(context.entity) || !id) {
      target.innerHTML = '<div class="related-empty">Related data is available for stops, routes and trips.</div>';
      return;
    }
    const requestId = ++state.detailRequestId;
    target.innerHTML = '<div class="related-loading"><i data-lucide="loader-circle"></i><span>Loading related data...</span></div>';
    iconRefresh();
    try {
      const payload = await api(`/admin/related/${encodeURIComponent(context.entity)}/${encodeURIComponent(id)}`);
      if (requestId !== state.detailRequestId || state.detailContext !== context) return;
      if (payload.database_available === false) {
        target.innerHTML = '<div class="related-empty">The transport database is unavailable.</div>';
        return;
      }
      if (payload.record) {
        context.record = payload.record;
        context.subtitle = recordSubtitle(payload.record);
        $("#detail-subtitle").textContent = context.subtitle;
        renderDetailFields(payload.record);
      }
      renderRelatedData(payload);
    } catch (error) {
      if (requestId !== state.detailRequestId) return;
      target.innerHTML = `<div class="related-empty error-state">${escapeHtml(error.message)}</div>`;
    }
  }

  async function loadEntities() {
    const payload = await api("/admin/data");
    state.entities = payload.entities || [];
    const select = $("#entity-select");
    const groups = new Map();
    state.entities.forEach((entity) => {
      const group = ENTITY_UI[entity.key]?.group || "Other";
      if (!groups.has(group)) groups.set(group, []);
      groups.get(group).push(entity);
    });
    select.innerHTML = [...groups.entries()].map(([group, entities]) => `
      <optgroup label="${escapeHtml(group)}">
        ${entities.map((entity) => `<option value="${escapeHtml(entity.key)}">${escapeHtml(entity.label)}</option>`).join("")}
      </optgroup>`).join("");
    if (state.entities.some((entity) => entity.key === state.entity)) select.value = state.entity;
    updateEntityContext();
  }

  function updateEntityContext() {
    state.entity = $("#entity-select").value || state.entity;
    const entity = state.entities.find((item) => item.key === state.entity);
    const ui = ENTITY_UI[state.entity] || {};
    $("#entity-description").textContent = ui.description || "Browse imported and application records.";
    $("#entity-search").placeholder = `Search ${String(entity?.label || state.entity).toLowerCase()} by any value`;
    $("#entity-open-map").classList.toggle("hidden", entity?.key !== "stops");
  }

  function entityColumns(rows) {
    const available = [...new Set(rows.flatMap((row) => Object.keys(row)))];
    if ($("#entity-column-mode").value === "all") return available;
    const preferred = ENTITY_UI[state.entity]?.columns || ["id", "name", "source_feed_id", "status", "type", "created_at"];
    const selected = preferred.filter((column) => available.includes(column));
    return selected.length ? selected : available.slice(0, 8);
  }

  function renderEntityRows(label = $("#entity-title").textContent) {
    renderTable("#entity-table", state.entityRows, entityColumns(state.entityRows), {
      clickable: true,
      rowAction: true,
      title: label,
      entity: state.entity,
      badgeColumns: ["status", "severity", "coordinate_confidence", "confidence"],
      empty: $("#entity-search").value.trim() ? "No records match this search" : "No records are available"
    });
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
    updateEntityContext();
    state.entityPageSize = Number($("#entity-page-size").value || 50);
    const query = new URLSearchParams({
      page: String(state.entityPage),
      page_size: String(state.entityPageSize),
      q: $("#entity-search").value.trim()
    });
    $("#entity-table").innerHTML = '<div class="loading-state">Loading records…</div>';
    const payload = await api(`/admin/data/${encodeURIComponent(state.entity)}?${query}`);
    state.entityPages = payload.pagination?.total_pages || 0;
    state.entityTotalRows = payload.pagination?.total_rows || 0;
    $("#entity-title").textContent = payload.label || state.entity;
    $("#entity-count").textContent = `${formatNumber(state.entityTotalRows)} records`;
    $("#page-status").textContent = state.entityPages ? `Page ${state.entityPage} of ${state.entityPages}` : "No pages";
    $("#page-previous").disabled = state.entityPage <= 1;
    $("#page-next").disabled = !state.entityPages || state.entityPage >= state.entityPages;
    $("#page-first").disabled = state.entityPage <= 1;
    $("#page-last").disabled = !state.entityPages || state.entityPage >= state.entityPages;
    state.entityRows = payload.rows || [];
    const rangeStart = state.entityRows.length ? (state.entityPage - 1) * state.entityPageSize + 1 : 0;
    const rangeEnd = state.entityRows.length ? rangeStart + state.entityRows.length - 1 : 0;
    $("#entity-range").textContent = state.entityRows.length
      ? `${formatNumber(rangeStart)}-${formatNumber(rangeEnd)} of ${formatNumber(state.entityTotalRows)}`
      : "No records";
    $("#entity-search-clear").classList.toggle("hidden", !$("#entity-search").value);
    renderEntityRows(payload.label || state.entity);
  }

  function projectCoordinate(lat, lon, zoom) {
    const worldSize = 256 * (2 ** zoom);
    const latitude = Math.max(-85.051129, Math.min(85.051129, Number(lat)));
    const sinLatitude = Math.sin(latitude * Math.PI / 180);
    return {
      x: ((Number(lon) + 180) / 360) * worldSize,
      y: (0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI)) * worldSize
    };
  }

  function unprojectCoordinate(x, y, zoom) {
    const worldSize = 256 * (2 ** zoom);
    const longitude = (x / worldSize) * 360 - 180;
    const latitude = Math.atan(Math.sinh(Math.PI * (1 - (2 * y) / worldSize))) * 180 / Math.PI;
    return { lat: latitude, lon: longitude };
  }

  function resizeMapCanvas() {
    if (!state.map) return false;
    const rect = state.map.canvas.getBoundingClientRect();
    if (!rect.width || !rect.height) return false;
    const pixelRatio = Math.min(window.devicePixelRatio || 1, 2);
    const width = Math.round(rect.width);
    const height = Math.round(rect.height);
    if (state.map.width !== width || state.map.height !== height || state.map.pixelRatio !== pixelRatio) {
      state.map.width = width;
      state.map.height = height;
      state.map.pixelRatio = pixelRatio;
      state.map.canvas.width = Math.round(width * pixelRatio);
      state.map.canvas.height = Math.round(height * pixelRatio);
    }
    state.map.context.setTransform(pixelRatio, 0, 0, pixelRatio, 0, 0);
    return true;
  }

  function mapBounds() {
    if (!state.map || !resizeMapCanvas()) return null;
    const center = projectCoordinate(state.map.centerLat, state.map.centerLon, state.map.zoom);
    const northWest = unprojectCoordinate(center.x - state.map.width / 2, center.y - state.map.height / 2, state.map.zoom);
    const southEast = unprojectCoordinate(center.x + state.map.width / 2, center.y + state.map.height / 2, state.map.zoom);
    return {
      minLat: southEast.lat,
      minLon: northWest.lon,
      maxLat: northWest.lat,
      maxLon: southEast.lon
    };
  }

  function loadMapTile(zoom, x, y) {
    if (!state.map) return null;
    const tileCount = 2 ** zoom;
    if (y < 0 || y >= tileCount) return null;
    const normalizedX = ((x % tileCount) + tileCount) % tileCount;
    const key = `${zoom}/${normalizedX}/${y}`;
    if (state.map.tiles.has(key)) return state.map.tiles.get(key);
    const image = new Image();
    const tile = { image, loaded: false, failed: false };
    state.map.tiles.set(key, tile);
    image.onload = () => {
      tile.loaded = true;
      renderMap();
    };
    image.onerror = () => {
      tile.failed = true;
    };
    image.src = `https://tile.openstreetmap.org/${zoom}/${normalizedX}/${y}.png`;
    return tile;
  }

  function positionMapPopup(stop) {
    const popup = $("#map-popup");
    if (!stop || !state.map || stop._mapX === undefined) {
      popup.classList.add("hidden");
      return;
    }
    popup.innerHTML = `
      <div class="popup-title">${escapeHtml(stop.name)}</div>
      <div class="popup-meta">
        ${escapeHtml(stop.municipality || stop.region || "")}<br>
        ${stop.platform_code ? `Platform ${escapeHtml(stop.platform_code)}<br>` : ""}
        ${escapeHtml(stop.source_feed_id || "Unknown source")}<br>
        ${escapeHtml(stop.id)}
      </div>`;
    popup.classList.remove("hidden");
    const popupWidth = Math.min(280, Math.max(210, popup.offsetWidth));
    const popupHeight = popup.offsetHeight;
    popup.style.left = `${Math.max(8, Math.min(state.map.width - popupWidth - 8, stop._mapX + 10))}px`;
    popup.style.top = `${Math.max(8, Math.min(state.map.height - popupHeight - 8, stop._mapY - popupHeight - 10))}px`;
  }

  function renderMap() {
    if (!state.map || !resizeMapCanvas()) return;
    const { context, width, height, zoom } = state.map;
    const center = projectCoordinate(state.map.centerLat, state.map.centerLon, zoom);
    const originX = center.x - width / 2;
    const originY = center.y - height / 2;

    context.clearRect(0, 0, width, height);
    context.fillStyle = "#dce5e9";
    context.fillRect(0, 0, width, height);

    const minTileX = Math.floor(originX / 256);
    const maxTileX = Math.floor((originX + width) / 256);
    const minTileY = Math.floor(originY / 256);
    const maxTileY = Math.floor((originY + height) / 256);
    for (let tileY = minTileY; tileY <= maxTileY; tileY += 1) {
      for (let tileX = minTileX; tileX <= maxTileX; tileX += 1) {
        const screenX = tileX * 256 - originX;
        const screenY = tileY * 256 - originY;
        const tile = loadMapTile(zoom, tileX, tileY);
        if (tile?.loaded) {
          context.drawImage(tile.image, screenX, screenY, 256, 256);
        } else {
          context.strokeStyle = "rgba(91, 107, 122, 0.16)";
          context.strokeRect(Math.round(screenX), Math.round(screenY), 256, 256);
        }
      }
    }

    state.mapStops.forEach((stop) => {
      const point = projectCoordinate(stop.lat, stop.lon, zoom);
      stop._mapX = point.x - originX;
      stop._mapY = point.y - originY;
      if (stop._mapX < -10 || stop._mapX > width + 10 || stop._mapY < -10 || stop._mapY > height + 10) return;
      const radius = stop.platform_code ? 4 : 6;
      context.beginPath();
      context.arc(stop._mapX, stop._mapY, radius, 0, Math.PI * 2);
      context.fillStyle = stop.coordinate_confidence === "unresolved" ? "#b42318" : "#1261a0";
      context.fill();
      context.lineWidth = 1.5;
      context.strokeStyle = "#ffffff";
      context.stroke();
    });
    positionMapPopup(state.map.selectedStop);
  }

  function showMapStop(stop) {
    if (!stop || !state.map) return;
    state.map.centerLat = Number(stop.lat);
    state.map.centerLon = Number(stop.lon);
    state.map.zoom = Math.max(state.map.zoom, 14);
    state.map.selectedStop = stop;
    renderMap();
  }

  function ensureMap() {
    if (state.map) return;
    const canvas = $("#map-canvas");
    const context = canvas?.getContext("2d");
    if (!canvas || !context) return;
    state.map = {
      canvas,
      context,
      centerLat: 49.82,
      centerLon: 15.48,
      zoom: 7,
      width: 0,
      height: 0,
      pixelRatio: 1,
      tiles: new Map(),
      selectedStop: null,
      drag: null
    };

    canvas.addEventListener("pointerdown", (event) => {
      const center = projectCoordinate(state.map.centerLat, state.map.centerLon, state.map.zoom);
      state.map.drag = { x: event.clientX, y: event.clientY, center, moved: false };
      canvas.setPointerCapture(event.pointerId);
      canvas.classList.add("dragging");
    });
    canvas.addEventListener("pointermove", (event) => {
      if (!state.map.drag) return;
      const deltaX = event.clientX - state.map.drag.x;
      const deltaY = event.clientY - state.map.drag.y;
      if (Math.abs(deltaX) + Math.abs(deltaY) > 3) state.map.drag.moved = true;
      const center = unprojectCoordinate(
        state.map.drag.center.x - deltaX,
        state.map.drag.center.y - deltaY,
        state.map.zoom
      );
      state.map.centerLat = center.lat;
      state.map.centerLon = center.lon;
      state.map.selectedStop = null;
      renderMap();
    });
    canvas.addEventListener("pointerup", (event) => {
      state.map.justDragged = state.map.drag?.moved || false;
      state.map.drag = null;
      canvas.releasePointerCapture(event.pointerId);
      canvas.classList.remove("dragging");
    });
    canvas.addEventListener("click", (event) => {
      if (state.map.justDragged) {
        state.map.justDragged = false;
        return;
      }
      const rect = canvas.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      const stop = state.mapStops
        .filter((item) => item._mapX !== undefined && Math.hypot(item._mapX - x, item._mapY - y) <= 10)
        .sort((a, b) => Math.hypot(a._mapX - x, a._mapY - y) - Math.hypot(b._mapX - x, b._mapY - y))[0];
      state.map.selectedStop = stop || null;
      renderMap();
    });
    canvas.addEventListener("wheel", (event) => {
      event.preventDefault();
      state.map.zoom = Math.max(3, Math.min(18, state.map.zoom + (event.deltaY < 0 ? 1 : -1)));
      state.map.selectedStop = null;
      renderMap();
    }, { passive: false });
    window.addEventListener("resize", renderMap);
    renderMap();
  }

  async function loadMapStops() {
    ensureMap();
    $("#map-result-count").textContent = "Loading stops...";
    $("#map-result-list").innerHTML = '<div class="loading-state">Loading stops...</div>';
    const search = $("#map-search").value.trim();
    const query = new URLSearchParams({
      q: search,
      limit: "5000"
    });
    const sourceFeedId = $("#map-source").value;
    if (sourceFeedId) query.set("source_feed_id", sourceFeedId);
    const bounds = mapBounds();
    if (bounds && !search) {
      query.set("min_lat", String(bounds.minLat));
      query.set("min_lon", String(bounds.minLon));
      query.set("max_lat", String(bounds.maxLat));
      query.set("max_lon", String(bounds.maxLon));
    }

    try {
      const payload = await api(`/admin/map/stops?${query}`);
      if (payload.database_available === false) {
        state.mapStops = [];
        renderMap();
        $("#map-result-count").textContent = "Database unavailable";
        $("#map-result-list").innerHTML = '<div class="empty-state">Connect the API to its transport database to load stops.</div>';
        return;
      }
      state.mapStops = payload.stops || [];
      if (state.map) state.map.selectedStop = null;
      if (search && state.mapStops.length && state.map) {
        state.map.centerLat = Number(state.mapStops[0].lat);
        state.map.centerLon = Number(state.mapStops[0].lon);
        state.map.zoom = Math.max(state.map.zoom, 12);
      }
      renderMap();
      $("#map-result-count").textContent = `${formatNumber(state.mapStops.length)} stops${payload.truncated ? " - result limit reached" : ""}`;
      $("#map-result-list").innerHTML = state.mapStops.slice(0, 250).map((stop, index) => `
        <button class="map-result" data-index="${index}">
          <strong>${escapeHtml(stop.name)}${stop.platform_code ? ` - ${escapeHtml(stop.platform_code)}` : ""}</strong>
          <span>${escapeHtml(stop.municipality || stop.region || stop.source_feed_id || "")}</span>
        </button>`).join("") || '<div class="empty-state">No stops in the visible area</div>';
      $$("#map-result-list .map-result").forEach((button) => {
        button.addEventListener("click", () => showMapStop(state.mapStops[Number(button.dataset.index)]));
      });
    } catch (error) {
      $("#map-result-count").textContent = "Stops could not be loaded";
      $("#map-result-list").innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
      throw error;
    }
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

  function renderValidationResults(validation) {
    if (!validation) {
      $("#validation-last-run").textContent = "No manual database validation has been recorded.";
      $("#validation-summary-badge").className = "badge";
      $("#validation-summary-badge").textContent = "Not run";
      $("#validation-checks").innerHTML = '<div class="empty-state compact-empty">Run validation to check missing data, source tracking and schedule consistency.</div>';
      return;
    }
    const failed = Number(validation.checks_failed || 0);
    const affected = Number(validation.affected_records || 0);
    $("#validation-last-run").textContent = `Last run ${formatDate(validation.finished_at)} · ${formatNumber(validation.checks_total)} checks · ${formatNumber(affected)} affected records`;
    $("#validation-summary-badge").className = `badge ${failed ? "danger" : "success"}`;
    $("#validation-summary-badge").textContent = failed ? `${failed} failed` : "All passed";
    const results = validation.results || [];
    $("#validation-checks").innerHTML = results.map((check) => `
      <button class="validation-check ${check.status === "passed" ? "passed" : "failed"}" type="button" data-entity="${escapeHtml(check.entity)}" data-code="${escapeHtml(check.code)}">
        <span class="validation-check-icon"><i data-lucide="${check.status === "passed" ? "check" : "alert-triangle"}"></i></span>
        <span class="validation-check-copy">
          <strong>${escapeHtml(check.description)}</strong>
          <small>${escapeHtml(humanizeField(check.entity))} · ${escapeHtml(check.code)}</small>
        </span>
        <span class="validation-check-count">${check.status === "passed" ? "Passed" : `${formatNumber(check.count)} records`}</span>
      </button>`).join("") || '<div class="empty-state compact-empty">No check results were returned.</div>';
    $$("#validation-checks .validation-check.failed").forEach((button) => {
      button.addEventListener("click", () => {
        $("#entity-select").value = "validation_issues";
        $("#entity-search").value = button.dataset.code;
        navigate("data");
      });
    });
    iconRefresh();
  }

  async function runDataValidation() {
    const button = $("#run-validation");
    const original = button.innerHTML;
    button.disabled = true;
    button.innerHTML = '<i data-lucide="loader-circle"></i>Validating...';
    button.classList.add("loading-button");
    iconRefresh();
    try {
      const payload = await api("/admin/data-quality/validate", { method: "POST" });
      if (payload.database_available === false) throw new Error(payload.message || "Database is unavailable");
      renderValidationResults(payload.validation);
      toast(`Validation complete: ${formatNumber(payload.validation?.checks_failed)} failed checks`);
      await loadQuality(payload.validation);
    } catch (error) {
      toast(error.message, "error");
    } finally {
      button.disabled = false;
      button.innerHTML = original;
      button.classList.remove("loading-button");
      iconRefresh();
    }
  }

  async function loadQuality(currentValidation = null) {
    const [quality, unresolved] = await Promise.all([
      api("/admin/data-quality"),
      api("/admin/unmatched-stops")
    ]);
    const severity = Object.fromEntries((quality.validation_issue_counts || []).map((item) => [item.severity, item.count]));
    $("#quality-metrics").innerHTML = [
      metric("Errors", severity.error || 0, "Validation errors"),
      metric("Warnings", severity.warning || 0, "Validation warnings"),
      metric("Unresolved stops", quality.unresolved_stops || 0, "Active stops needing coordinates"),
      metric("Duplicate groups", quality.duplicate_stop_groups || 0, "Same-name stops at one coordinate")
    ].join("");
    renderValidationResults(currentValidation || quality.last_database_validation);
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

  function routingFormValue(field) {
    const input = document.querySelector(`[name="${field.key}"]`);
    return field.type === "boolean" ? input.checked : Number(input.value);
  }

  function routingFormConfiguration() {
    return Object.fromEntries(ROUTING_FIELDS.map((field) => [field.key, routingFormValue(field)]));
  }

  function secondsAsClock(seconds) {
    const hours = Math.floor(seconds / 3600).toString().padStart(2, "0");
    const minutes = Math.floor((seconds % 3600) / 60).toString().padStart(2, "0");
    return `${hours}:${minutes}`;
  }

  function renderRoutingSummary() {
    const config = routingFormConfiguration();
    const formula = `${config.arrival_time_weight} × arrival + ${config.duration_weight} × duration + ${config.transfer_penalty_seconds}s × transfers`;
    const guarantees = [
      config.preserve_simplest && "simplest journey",
      config.preserve_each_transfer_count && "each transfer count",
      config.preserve_carrier_diversity && "each carrier combination"
    ].filter(Boolean);
    $("#routing-summary").innerHTML = `
      <div><span>Ranking formula</span><strong>${escapeHtml(formula)}</strong></div>
      <div><span>Transfer window</span><strong>${Math.round(config.min_transfer_seconds / 60)}–${Math.round(config.max_transfer_wait_seconds / 60)} min</strong></div>
      <div><span>Next-day search</span><strong>from ${secondsAsClock(config.next_day_search_from_seconds)}</strong></div>
      <div><span>Reserved coverage</span><strong>${escapeHtml(guarantees.join(", ") || "none")}</strong></div>
      <div><span>Dominance pruning</span><strong>${config.remove_dominated ? (config.dominate_only_same_carrier ? "same carrier only" : "across all carriers") : "disabled"}</strong></div>
      <div><span>Search envelope</span><strong>${config.max_direct_candidates} direct + ${config.max_transfer_candidates} transfer → ${config.max_results} results</strong></div>`;
  }

  function renderRoutingForm(payload) {
    state.routingConfiguration = payload.configuration;
    state.routingDefaults = payload.defaults;
    state.routingDatabaseAvailable = payload.database_available;
    const groups = [...new Set(ROUTING_FIELDS.map((field) => field.group))];
    $("#routing-groups").innerHTML = groups.map((group) => `
      <section class="panel routing-group">
        <div class="panel-header"><div><h2>${escapeHtml(group)}</h2><p>${group === "Candidate generation" ? "Controls database search breadth and valid interchange windows." : group === "Ranking score" ? "Defines the generalized score; lower scores rank first." : "Controls pruning, diversity, and the final response set."}</p></div></div>
        <div class="routing-field-grid">
          ${ROUTING_FIELDS.filter((field) => field.group === group).map((field) => {
            const value = payload.configuration[field.key];
            const defaultValue = payload.defaults[field.key];
            if (field.type === "boolean") return `
              <label class="routing-field routing-boolean">
                <span class="routing-field-heading"><strong>${escapeHtml(field.label)}</strong><input name="${field.key}" type="checkbox" ${value ? "checked" : ""}></span>
                <small>${escapeHtml(field.detail)}</small>
                <em>Default: ${defaultValue ? "enabled" : "disabled"}</em>
              </label>`;
            return `
              <label class="routing-field">
                <span class="routing-field-heading"><strong>${escapeHtml(field.label)}</strong><span>${escapeHtml(field.unit)}</span></span>
                <input name="${field.key}" type="number" value="${escapeHtml(value)}" min="${field.min}" max="${field.max}" step="${field.step}" required>
                <small>${escapeHtml(field.detail)}</small>
                <em>Allowed: ${field.min}–${field.max}. Default: ${defaultValue}.</em>
              </label>`;
          }).join("")}
        </div>
      </section>`).join("");
    const persisted = payload.database_available ? "Persisted configuration" : "Defaults only — database unavailable";
    const audit = payload.updated_at ? ` Last changed ${new Date(payload.updated_at).toLocaleString()}${payload.updated_by ? ` by ${payload.updated_by}` : ""}.` : "";
    $("#routing-status").classList.remove("warning");
    $("#routing-status").textContent = `${persisted}.${audit}`;
    $("#routing-save").disabled = !payload.database_available;
    $("#routing-reset").disabled = !payload.database_available;
    $$("#routing-form input").forEach((input) => input.addEventListener("input", () => {
      renderRoutingSummary();
      $("#routing-status").textContent = "Unsaved changes. Validate and activate to apply them.";
      $("#routing-status").classList.add("warning");
    }));
    renderRoutingSummary();
    iconRefresh();
  }

  async function loadRouting() {
    renderRoutingForm(await api("/admin/routing-algorithm"));
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
        setTimeout(renderMap, 20);
        if (!state.sources.length) await loadSources();
        if (!state.mapStops.length) await loadMapStops();
      }
      if (view === "imports") await loadImports();
      if (view === "quality") await loadQuality();
      if (view === "sources") await loadSources();
      if (view === "routing") await loadRouting();
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
      sources: ["Source feeds", "Feed configuration and import priority"],
      routing: ["Routing algorithm", "Candidate generation, ranking score, pruning, and diversity"]
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
    $("#routing-form").addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!event.currentTarget.reportValidity()) return;
      const button = $("#routing-save");
      button.disabled = true;
      try {
        const payload = await api("/admin/routing-algorithm", {
          method: "PUT",
          body: JSON.stringify(routingFormConfiguration())
        });
        renderRoutingForm(payload);
        $("#routing-status").classList.remove("warning");
        toast("Routing algorithm validated and activated");
      } catch (error) {
        toast(error.message, "error");
      } finally {
        button.disabled = !state.routingDatabaseAvailable;
      }
    });
    $("#routing-reset").addEventListener("click", async () => {
      if (!window.confirm("Reset every routing parameter to its documented default and activate it immediately?")) return;
      try {
        const payload = await api("/admin/routing-algorithm", { method: "DELETE" });
        renderRoutingForm(payload);
        $("#routing-status").classList.remove("warning");
        toast("Routing algorithm reset to defaults");
      } catch (error) {
        toast(error.message, "error");
      }
    });
    $("#entity-select").addEventListener("change", () => {
      $("#entity-search").value = "";
      loadEntityRows(true);
    });
    $("#entity-column-mode").addEventListener("change", () => renderEntityRows());
    $("#entity-page-size").addEventListener("change", () => loadEntityRows(true));
    $("#entity-reload").addEventListener("click", () => loadEntityRows());
    $("#entity-search").addEventListener("input", debounce(() => loadEntityRows(true), 350));
    $("#entity-search-clear").addEventListener("click", () => {
      $("#entity-search").value = "";
      loadEntityRows(true);
      $("#entity-search").focus();
    });
    $("#entity-open-map").addEventListener("click", () => {
      $("#map-search").value = $("#entity-search").value;
      navigate("map");
    });
    $("#page-first").addEventListener("click", () => {
      state.entityPage = 1;
      loadEntityRows();
    });
    $("#page-previous").addEventListener("click", () => {
      if (state.entityPage > 1) {
        state.entityPage -= 1;
        loadEntityRows();
      }
    });
    $("#page-last").addEventListener("click", () => {
      if (!state.entityPages) return;
      state.entityPage = state.entityPages;
      loadEntityRows();
    });
    $("#page-next").addEventListener("click", () => {
      if (state.entityPage < state.entityPages) {
        state.entityPage += 1;
        loadEntityRows();
      }
    });
    $("#map-load").addEventListener("click", loadMapStops);
    $("#map-zoom-in").addEventListener("click", () => {
      ensureMap();
      if (!state.map) return;
      state.map.zoom = Math.min(18, state.map.zoom + 1);
      state.map.selectedStop = null;
      renderMap();
    });
    $("#map-zoom-out").addEventListener("click", () => {
      ensureMap();
      if (!state.map) return;
      state.map.zoom = Math.max(3, state.map.zoom - 1);
      state.map.selectedStop = null;
      renderMap();
    });
    $("#map-search").addEventListener("keydown", (event) => {
      if (event.key === "Enter") loadMapStops();
    });
    $("#map-source").addEventListener("change", loadMapStops);
    $("#run-validation").addEventListener("click", runDataValidation);
    $("#detail-copy").addEventListener("click", async () => {
      if (!state.detailRecord) return;
      try {
        await navigator.clipboard.writeText(JSON.stringify(state.detailRecord, null, 2));
        toast("Record JSON copied");
      } catch {
        toast("Could not copy record JSON", "error");
      }
    });
    $("#detail-back").addEventListener("click", () => {
      const previous = state.detailHistory.pop();
      if (previous) showDetailContext(previous);
    });
    $("#detail-close").addEventListener("click", () => {
      state.detailRequestId += 1;
      state.detailHistory = [];
      state.detailContext = null;
      $("#detail-dialog").close();
    });
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
