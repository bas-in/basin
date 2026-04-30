// Basin dashboard renderer.
// Prefers window.__BASIN_RESULTS (populated by data/results.js, generated
// by dashboard/bundle.py) so the page works when index.html is opened
// directly via file://. Falls back to fetch() of the per-id JSON files
// when the bundle is absent — that path requires a local HTTP server
// because browsers block fetch over file://.
//
// All rendering is wrapped in try/catch per card so one bad payload does
// not break the page. No build step, no framework.

(function () {
  "use strict";

  const DATA_DIR = "data";

  // Display names for viability tests, used when the JSON file is missing.
  const VIABILITY_NAMES = {
    compression_ratio: "Audit-log Parquet vs CSV",
    idle_tenant_ram: "Idle tenant RAM footprint",
    predicate_pushdown: "Parquet predicate pushdown",
    tenant_deletion: "Tenant deletion latency",
    isolation_under_load: "Cross-tenant isolation under load",
    large_dataset_pointquery: "Large-dataset point query",
  };

  const SCALING_NAMES = {
    idle_tenants: "Idle-tenant fan-out",
    data_size: "Single-tenant data size scale-up",
    concurrency: "Concurrent client scaling",
    noisy_neighbor: "Noisy-neighbor isolation",
  };

  const COMPARE_NAMES = {
    postgres: "Basin vs Postgres",
  };

  const SERIES_COLORS = [
    "#2563eb", // primary blue
    "#9333ea", // secondary purple
    "#0ea5e9", // sky
    "#f59e0b", // amber
    "#16a34a", // green
    "#dc2626", // red
  ];

  // ---------- helpers ----------

  function el(tag, props = {}, children = []) {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(props)) {
      if (k === "class") node.className = v;
      else if (k === "html") node.innerHTML = v;
      else if (k === "text") node.textContent = v;
      else if (k.startsWith("data-")) node.setAttribute(k, v);
      else if (k.startsWith("aria-")) node.setAttribute(k, v);
      else if (k === "role") node.setAttribute("role", v);
      else node[k] = v;
    }
    const list = Array.isArray(children) ? children : [children];
    for (const c of list) {
      if (c == null || c === false) continue;
      node.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
    }
    return node;
  }

  function clear(node) {
    while (node.firstChild) node.removeChild(node.firstChild);
  }

  function parseGeneratedAt(value) {
    if (!value) return null;
    if (typeof value === "string" && value.startsWith("@")) {
      const secs = parseInt(value.slice(1), 10);
      if (Number.isFinite(secs)) return new Date(secs * 1000);
    }
    const d = new Date(value);
    return isNaN(d.getTime()) ? null : d;
  }

  function formatDate(d) {
    if (!d) return "—";
    const opts = {
      year: "numeric",
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    };
    return d.toLocaleString(undefined, opts);
  }

  function formatNumber(n, opts = {}) {
    if (n == null || !Number.isFinite(n)) return "—";
    const { unit = "", decimals } = opts;
    let str;
    const abs = Math.abs(n);
    if (decimals != null) {
      str = n.toFixed(decimals);
    } else if (Number.isInteger(n)) {
      str = n.toLocaleString();
    } else if (abs >= 1000) {
      str = n.toLocaleString(undefined, { maximumFractionDigits: 0 });
    } else if (abs >= 10) {
      str = n.toLocaleString(undefined, { maximumFractionDigits: 1 });
    } else {
      str = n.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    return unit ? `${str} ${unit}` : str;
  }

  function formatBytes(n) {
    if (!Number.isFinite(n)) return "—";
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
    if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MiB`;
    return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`;
  }

  function humanizeKey(k) {
    return k
      .replace(/_/g, " ")
      .replace(/\b\w/g, (c) => c.toUpperCase());
  }

  function formatDetailValue(key, val) {
    if (typeof val === "boolean") return val ? "true" : "false";
    if (val == null) return "—";
    if (typeof val === "number") {
      if (key.endsWith("_bytes")) return formatBytes(val);
      if (key.endsWith("_kib")) return `${formatNumber(val)} KiB`;
      if (key.endsWith("_mib")) return `${formatNumber(val)} MiB`;
      if (key.endsWith("_ms")) return `${formatNumber(val)} ms`;
      if (key === "rows") return val.toLocaleString();
      return formatNumber(val);
    }
    return String(val);
  }

  function barOpText(bar) {
    if (!bar || !bar.op) return "";
    const v = bar.value;
    switch (bar.op) {
      case "greater_than_or_equal":
        return `bar: ≥ ${formatNumber(v)}`;
      case "greater_than":
        return `bar: > ${formatNumber(v)}`;
      case "less_than_or_equal":
        return `bar: ≤ ${formatNumber(v)}`;
      case "less_than":
        return `bar: < ${formatNumber(v)}`;
      case "equal":
        return `bar: = ${formatNumber(v)}`;
      default:
        return `bar: ${bar.op} ${formatNumber(v)}`;
    }
  }

  function maxAbs(values) {
    return values.reduce((m, v) => (Number.isFinite(v) && Math.abs(v) > m ? Math.abs(v) : m), 0);
  }

  function spansOrders(values, threshold = 2) {
    const finite = values.filter((v) => Number.isFinite(v) && v > 0);
    if (finite.length < 2) return false;
    const min = Math.min(...finite);
    const max = Math.max(...finite);
    if (min <= 0) return false;
    return Math.log10(max / min) > threshold;
  }

  // Try the inlined bundle first. `path` looks like "data/<key>.json", so we
  // strip the prefix + suffix and look up the key in either `manifest` or
  // `reports`. A missing key throws notFound, mirroring the fetch path so
  // every caller's existing 404 handling keeps working.
  function fromBundle(path) {
    const bundle = (typeof window !== "undefined" && window.__BASIN_RESULTS) || null;
    if (!bundle) return undefined;
    const m = path.match(/^data\/(.+)\.json$/);
    if (!m) return undefined;
    const key = m[1];
    if (key === "manifest") return bundle.manifest;
    const reports = bundle.reports || {};
    return reports[key];
  }

  async function fetchJson(path) {
    const inlined = fromBundle(path);
    if (inlined !== undefined) {
      if (inlined === null) {
        const err = new Error("not_found");
        err.notFound = true;
        throw err;
      }
      return inlined;
    }
    if (typeof fetch !== "function" || location.protocol === "file:") {
      const err = new Error("not_found");
      err.notFound = true;
      err.message =
        "data/results.js missing — run `python3 dashboard/bundle.py` after `cargo test`";
      throw err;
    }
    const res = await fetch(path, { cache: "no-store" });
    if (res.status === 404) {
      const err = new Error("not_found");
      err.notFound = true;
      throw err;
    }
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${path}`);
    return res.json();
  }

  // ---------- pill ----------

  function statusPill(passed, label) {
    const cls = passed ? "pill pill--pass" : "pill pill--fail";
    const text = label || (passed ? "PASS" : "FAIL");
    return el("span", {
      class: cls,
      role: "status",
      "aria-label": `Test status: ${text}`,
    }, [
      el("span", { class: "pill__dot", "aria-hidden": "true" }),
      el("span", { text }),
    ]);
  }

  function neutralPill(text) {
    return el("span", { class: "pill pill--neutral" }, [
      el("span", { class: "pill__dot", "aria-hidden": "true" }),
      el("span", { text }),
    ]);
  }

  function errorPill(text = "ERROR") {
    return el("span", { class: "pill pill--error", role: "status" }, [
      el("span", { class: "pill__dot", "aria-hidden": "true" }),
      el("span", { text }),
    ]);
  }

  // ---------- generic card pieces ----------

  function cardHeader(name, claim, pill) {
    return el("div", { class: "card__header" }, [
      el("div", { class: "card__heading" }, [
        el("div", { class: "card__title", text: name }),
        claim ? el("div", { class: "card__subtitle", text: claim }) : null,
      ]),
      pill,
    ]);
  }

  function primaryMetricBlock(primary) {
    if (!primary) return null;
    return el("div", { class: "primary-metric" }, [
      el("div", { class: "primary-metric__label", text: primary.label || "Primary" }),
      el("div", { class: "primary-metric__value tabular" }, [
        el("span", { class: "primary-metric__number", text: formatNumber(primary.value) }),
        primary.unit
          ? el("span", { class: "primary-metric__unit", text: primary.unit })
          : null,
      ]),
      primary.bar
        ? el("div", { class: "primary-metric__bar", text: barOpText(primary.bar) })
        : null,
    ]);
  }

  function detailsStrip(details) {
    if (!details || typeof details !== "object") return null;
    const entries = Object.entries(details).filter(([, v]) => v != null);
    if (entries.length === 0) return null;
    return el(
      "div",
      { class: "details" },
      entries.map(([k, v]) =>
        el("div", { class: "details__item" }, [
          el("div", { class: "details__label", text: humanizeKey(k) }),
          el("div", { class: "details__value", text: formatDetailValue(k, v) }),
        ])
      )
    );
  }

  // Hand-rolled CSS bar showing a single value against a threshold.
  // direction: "lower-better" means we want value < threshold (PASS region 0..threshold).
  function thresholdBar({ valueLabel, value, valueUnit, thresholdLabel, threshold, direction }) {
    const max = Math.max(value, threshold) * 1.15;
    const valuePct = max > 0 ? Math.min(100, (value / max) * 100) : 0;
    const thresholdPct = max > 0 ? (threshold / max) * 100 : 0;
    const fmtVal = `${formatNumber(value)}${valueUnit ? " " + valueUnit : ""}`;
    const fmtThresh = `${formatNumber(threshold)}${valueUnit ? " " + valueUnit : ""}`;
    const passed =
      direction === "lower-better" ? value < threshold : value >= threshold;

    const track = el("div", { class: "bar-vis__track" }, [
      el("div", {
        class: "bar-vis__fill",
        style: `width: ${valuePct.toFixed(2)}%; background: ${passed ? "var(--bar-fill)" : "var(--fail)"};`,
      }),
      el("div", {
        class: "bar-vis__fill bar-vis__fill--threshold",
        style: `left: ${thresholdPct.toFixed(2)}%;`,
        "aria-label": "threshold marker",
      }),
    ]);

    return el("div", { class: "bar-vis" }, [
      el("div", { class: "bar-vis__row" }, [
        el("div", { class: "bar-vis__label", text: valueLabel }),
        track,
        el("div", { class: "bar-vis__value tabular", text: fmtVal }),
      ]),
      el("div", {
        class: "bar-vis__caption",
        text: `${thresholdLabel}: ${fmtThresh} (${direction === "lower-better" ? "lower wins" : "higher wins"})`,
      }),
    ]);
  }

  function comparativeBar({ aLabel, aValue, bLabel, bValue, unit }) {
    const max = Math.max(aValue, bValue);
    const aPct = max > 0 ? (aValue / max) * 100 : 0;
    const bPct = max > 0 ? (bValue / max) * 100 : 0;
    const fmt = (v) => {
      if (unit === "B" || unit === "bytes") return formatBytes(v);
      return `${formatNumber(v)}${unit ? " " + unit : ""}`;
    };
    return el("div", { class: "bar-vis" }, [
      el("div", { class: "bar-vis__row" }, [
        el("div", { class: "bar-vis__label", text: aLabel }),
        el("div", { class: "bar-vis__track" }, [
          el("div", {
            class: "bar-vis__fill",
            style: `width: ${aPct.toFixed(2)}%; background: var(--primary);`,
          }),
        ]),
        el("div", { class: "bar-vis__value tabular", text: fmt(aValue) }),
      ]),
      el("div", { class: "bar-vis__row" }, [
        el("div", { class: "bar-vis__label", text: bLabel }),
        el("div", { class: "bar-vis__track" }, [
          el("div", {
            class: "bar-vis__fill",
            style: `width: ${bPct.toFixed(2)}%; background: var(--secondary);`,
          }),
        ]),
        el("div", { class: "bar-vis__value tabular", text: fmt(bValue) }),
      ]),
    ]);
  }

  // ---------- chart helpers ----------

  function makeChart(canvas, config) {
    if (typeof Chart === "undefined") return null;
    // Chart.js global defaults that match our typography.
    Chart.defaults.font.family =
      'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif';
    Chart.defaults.font.size = 11;
    Chart.defaults.color = "#64748b";
    Chart.defaults.borderColor = "#e2e8f0";
    return new Chart(canvas.getContext("2d"), config);
  }

  function tinyBarChart(container, { labels, datasets, ariaLabel }) {
    const wrap = el("div", {
      class: "chart-wrap",
      role: "img",
      "aria-label": ariaLabel || "chart",
    });
    const canvas = el("canvas");
    wrap.appendChild(canvas);
    container.appendChild(wrap);
    makeChart(canvas, {
      type: "bar",
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        indexAxis: "x",
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "#0f172a",
            padding: 8,
            cornerRadius: 4,
            titleFont: { size: 11, weight: "600" },
            bodyFont: { size: 11 },
          },
        },
        scales: {
          x: {
            grid: { display: false },
            ticks: { color: "#64748b" },
          },
          y: {
            beginAtZero: true,
            grid: { color: "#e2e8f0" },
            ticks: {
              color: "#64748b",
              callback: (v) => {
                if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
                if (v >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
                return v;
              },
            },
          },
        },
      },
    });
    return wrap;
  }

  // ---------- viability cards ----------

  function viabilityCard(report) {
    const card = el("div", { class: "card" });
    card.appendChild(cardHeader(report.name, report.claim, statusPill(!!report.passed)));
    const primary = primaryMetricBlock(report.primary);
    if (primary) card.appendChild(primary);

    const details = report.details || {};

    // Per-id visualization
    const id = report.id;
    if (id === "compression_ratio") {
      tinyBarChart(card, {
        labels: ["CSV", "Parquet"],
        datasets: [
          {
            data: [details.csv_bytes ?? 0, details.parquet_bytes ?? 0],
            backgroundColor: ["#cbd5e1", "#2563eb"],
            borderRadius: 3,
            barPercentage: 0.6,
          },
        ],
        ariaLabel: "CSV vs Parquet bytes",
      });
    } else if (id === "predicate_pushdown") {
      tinyBarChart(card, {
        labels: ["Full scan", "Point query"],
        datasets: [
          {
            data: [details.full_scan_bytes ?? 0, details.point_query_bytes ?? 0],
            backgroundColor: ["#cbd5e1", "#2563eb"],
            borderRadius: 3,
            barPercentage: 0.6,
          },
        ],
        ariaLabel: "Full scan vs point query bytes read",
      });
    } else if (id === "idle_tenant_ram") {
      const value = details.per_tenant_kib ?? 0;
      const threshold = (report.primary && report.primary.bar && report.primary.bar.value) || 500;
      card.appendChild(
        thresholdBar({
          valueLabel: "per tenant",
          value,
          valueUnit: "KiB",
          thresholdLabel: "Bar",
          threshold,
          direction: "lower-better",
        })
      );
    } else if (id === "tenant_deletion") {
      const value = details.elapsed_ms ?? (report.primary && report.primary.value) ?? 0;
      const threshold = (report.primary && report.primary.bar && report.primary.bar.value) || 1000;
      card.appendChild(
        thresholdBar({
          valueLabel: "elapsed",
          value,
          valueUnit: "ms",
          thresholdLabel: "Bar",
          threshold,
          direction: "lower-better",
        })
      );
    } else if (id === "large_dataset_pointquery") {
      const value =
        details.point_query_ms ?? (report.primary && report.primary.value) ?? 0;
      const threshold = (report.primary && report.primary.bar && report.primary.bar.value) || 1000;
      card.appendChild(
        thresholdBar({
          valueLabel: "p50",
          value,
          valueUnit: "ms",
          thresholdLabel: "Bar",
          threshold,
          direction: "lower-better",
        })
      );
    } else if (id === "isolation_under_load") {
      const ops = details.ops ?? details.operations ?? "?";
      const tenants = details.tenants ?? "?";
      const leaks = details.leaks ?? 0;
      card.appendChild(
        el("div", { class: "indicator" }, [
          el("div", {
            class: leaks === 0 ? "indicator__check" : "indicator__check indicator__check--fail",
            "aria-hidden": "true",
          }),
          el("div", {
            class: "indicator__text tabular",
            text: `${leaks} leaks across ${ops.toLocaleString ? ops.toLocaleString() : ops} ops, ${tenants} tenants`,
          }),
        ])
      );
    }

    const detailsBlock = detailsStrip(details);
    if (detailsBlock) card.appendChild(detailsBlock);

    return card;
  }

  function placeholderCard(name, hint) {
    return el("div", { class: "card card--placeholder" }, [
      el("div", { class: "card__header" }, [
        el("div", { class: "card__heading" }, [
          el("div", { class: "card__title", text: name }),
          el("div", { class: "card__subtitle", text: "Not yet run." }),
        ]),
        neutralPill("PENDING"),
      ]),
      el("div", { class: "primary-metric__bar" }, [
        document.createTextNode("Run "),
        el("code", { text: hint }),
      ]),
    ]);
  }

  function errorCard(name, message) {
    return el("div", { class: "card card--error" }, [
      cardHeader(name, "Failed to render report", errorPill("ERROR")),
      el("div", { class: "primary-metric__bar muted", text: message }),
    ]);
  }

  // ---------- scaling cards ----------

  function buildDataTable(xAxis, series, rows) {
    const table = el("table", { class: "data-table" });
    const thead = el("thead");
    const headRow = el("tr");
    headRow.appendChild(el("th", { text: xAxis.label || xAxis.key }));
    for (const s of series) {
      headRow.appendChild(el("th", { text: s.label || s.key }));
    }
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = el("tbody");
    for (const row of rows) {
      const tr = el("tr");
      tr.appendChild(el("td", { text: formatNumber(row[xAxis.key]) }));
      for (const s of series) {
        tr.appendChild(el("td", { text: formatNumber(row[s.key]) }));
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    return el("div", { class: "data-table-wrap" }, [table]);
  }

  function scalingCard(report) {
    const card = el("div", { class: "card card--scaling" });

    const headerRow = el("div", { class: "card__header" }, [
      el("div", { class: "card__heading" }, [
        el("div", { class: "card__title", text: report.name }),
        report.claim ? el("div", { class: "card__subtitle", text: report.claim }) : null,
      ]),
      el("div", { style: "display: flex; gap: 8px; align-items: center;" }, [
        report.primary
          ? el("div", { class: "card__primary-inline" }, [
              el("span", {
                class: "card__primary-inline-label",
                text: report.primary.label || "primary",
              }),
              el("span", {
                class: "card__primary-inline-value",
                text: `${formatNumber(report.primary.value)}${report.primary.unit ? " " + report.primary.unit : ""}`,
              }),
            ])
          : null,
        statusPill(!!report.passed),
      ]),
    ]);
    card.appendChild(headerRow);

    const xAxis = report.x_axis || { key: "x", label: "x" };
    const series = report.series || [];
    const rows = report.rows || [];

    // Decide axes
    const xValues = rows.map((r) => r[xAxis.key]);
    const useLogX = spansOrders(xValues, 2);

    // Group series by unit for multi-Y-axis support
    const units = Array.from(new Set(series.map((s) => s.unit || "")));
    const yAxes = {};
    units.forEach((u, i) => {
      yAxes["y" + i] = {
        type: "linear",
        position: i === 0 ? "left" : "right",
        beginAtZero: true,
        grid: { color: i === 0 ? "#e2e8f0" : "transparent", drawOnChartArea: i === 0 },
        ticks: {
          color: "#64748b",
          callback: (v) => {
            if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
            if (Math.abs(v) >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
            return v;
          },
        },
        title: { display: !!u, text: u, color: "#64748b", font: { size: 11 } },
      };
    });

    const datasets = series.map((s, i) => {
      const unit = s.unit || "";
      const yAxisID = "y" + units.indexOf(unit);
      const color = SERIES_COLORS[i % SERIES_COLORS.length];
      return {
        label: s.label || s.key,
        data: rows.map((r) => ({ x: r[xAxis.key], y: r[s.key] })),
        borderColor: color,
        backgroundColor: color,
        borderWidth: 2,
        pointRadius: 3,
        pointHoverRadius: 5,
        tension: 0.15,
        yAxisID,
      };
    });

    const left = el("div");
    const right = el("div");

    const chartWrap = el("div", {
      class: "chart-wrap chart-wrap--tall",
      role: "img",
      "aria-label": `${report.name} chart`,
    });
    const canvas = el("canvas");
    chartWrap.appendChild(canvas);
    left.appendChild(chartWrap);

    const chartConfig = {
      type: "line",
      data: { datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: {
            position: "top",
            align: "start",
            labels: {
              boxWidth: 10,
              boxHeight: 10,
              usePointStyle: true,
              pointStyle: "rectRounded",
              color: "#0f172a",
              font: { size: 11 },
            },
          },
          tooltip: {
            backgroundColor: "#0f172a",
            padding: 8,
            cornerRadius: 4,
            titleFont: { size: 11, weight: "600" },
            bodyFont: { size: 11 },
          },
        },
        scales: {
          x: {
            type: useLogX ? "logarithmic" : "linear",
            title: {
              display: true,
              text: xAxis.label || xAxis.key,
              color: "#64748b",
              font: { size: 11 },
            },
            grid: { color: "#f1f5f9" },
            ticks: {
              color: "#64748b",
              callback: (v) => {
                if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
                if (Math.abs(v) >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
                return v;
              },
            },
          },
          ...yAxes,
        },
      },
    };

    right.appendChild(buildDataTable(xAxis, series, rows));

    const grid = el("div", { class: "scaling-grid" }, [left, right]);
    card.appendChild(grid);

    // Defer chart creation until canvas is in the DOM
    queueMicrotask(() => makeChart(canvas, chartConfig));

    return card;
  }

  // ---------- compare card ----------

  function compareCard(report) {
    const card = el("div", { class: "card" });
    card.appendChild(
      cardHeader(
        report.name || "Postgres comparison",
        report.claim,
        statusPill(true, "HEAD-TO-HEAD")
      )
    );

    const metrics = report.metrics || [];
    const wrap = el("div");

    for (const m of metrics) {
      const basinValue = Number(m.basin);
      const pgValue = Number(m.postgres);
      const better = m.better === "postgres" ? "postgres" : "basin";
      const max = Math.max(basinValue, pgValue) || 1;
      const basinPct = (basinValue / max) * 100;
      const pgPct = (pgValue / max) * 100;
      const fmt = (v) => {
        if (m.unit === "bytes" || m.unit === "B") return formatBytes(v);
        return `${formatNumber(v)}${m.unit ? " " + m.unit : ""}`;
      };

      wrap.appendChild(
        el("div", { class: "compare-metric" }, [
          el("div", { class: "compare-metric__header" }, [
            el("div", { class: "compare-metric__label", text: m.label }),
            m.ratio_text
              ? el("div", { class: "compare-metric__ratio tabular", text: m.ratio_text })
              : null,
          ]),
          el("div", { class: "compare-metric__bars" }, [
            el("div", { class: "compare-bar" }, [
              el("div", { class: "compare-bar__name", text: "Basin" }),
              el("div", { class: "compare-bar__track" }, [
                el("div", {
                  class: `compare-bar__fill compare-bar__fill--basin compare-bar__fill--${better === "basin" ? "winner" : "loser"}`,
                  style: `width: ${basinPct.toFixed(2)}%;`,
                }),
              ]),
              el("div", { class: "compare-bar__value tabular", text: fmt(basinValue) }),
            ]),
            el("div", { class: "compare-bar" }, [
              el("div", { class: "compare-bar__name", text: "Postgres" }),
              el("div", { class: "compare-bar__track" }, [
                el("div", {
                  class: `compare-bar__fill compare-bar__fill--postgres compare-bar__fill--${better === "postgres" ? "winner" : "loser"}`,
                  style: `width: ${pgPct.toFixed(2)}%;`,
                }),
              ]),
              el("div", { class: "compare-bar__value tabular", text: fmt(pgValue) }),
            ]),
          ]),
        ])
      );
    }

    card.appendChild(wrap);

    if (report.note) {
      card.appendChild(el("div", { class: "compare-note", text: report.note }));
    }
    return card;
  }

  function compareUnavailableCard(id, note) {
    return el("div", { class: "card card--placeholder" }, [
      el("div", { class: "card__header" }, [
        el("div", { class: "card__heading" }, [
          el("div", {
            class: "card__title",
            text: COMPARE_NAMES[id] || `Comparison: ${id}`,
          }),
          el("div", { class: "card__subtitle", text: "Comparison unavailable" }),
        ]),
        neutralPill("UNAVAILABLE"),
      ]),
      note
        ? el("div", { class: "primary-metric__bar muted", text: note })
        : null,
    ]);
  }

  // ---------- summary ----------

  const summary = {
    viability: { passed: 0, total: 0 },
    scaling: { passed: 0, total: 0 },
    compareCount: 0,
    compareAvailable: false,
    latest: null,
  };

  function updateSummaryUI() {
    const v = document.querySelector('[data-summary="viability"]');
    if (v) {
      v.querySelector("[data-summary-numerator]").textContent = summary.viability.passed;
      v.querySelector("[data-summary-denominator]").textContent = summary.viability.total;
    }
    const s = document.querySelector('[data-summary="scaling"]');
    if (s) {
      s.querySelector("[data-summary-numerator]").textContent = summary.scaling.passed;
      s.querySelector("[data-summary-denominator]").textContent = summary.scaling.total;
    }
    const c = document.querySelector('[data-summary="compare"]');
    if (c) {
      const t = c.querySelector("[data-summary-text]");
      if (summary.compareCount === 0) {
        t.textContent = "—";
      } else if (summary.compareAvailable) {
        t.textContent = `${summary.compareCount} head-to-head`;
      } else {
        t.textContent = "unavailable";
      }
    }
    const g = document.querySelector("[data-generated-at]");
    if (g) g.textContent = formatDate(summary.latest);
  }

  function recordGeneratedAt(report) {
    const d = parseGeneratedAt(report && report.generated_at);
    if (!d) return;
    if (!summary.latest || d > summary.latest) summary.latest = d;
  }

  // ---------- main ----------

  async function loadViability(ids, container) {
    summary.viability.total = ids.length;
    for (const id of ids) {
      const slot = el("div");
      container.appendChild(slot);
      try {
        const data = await fetchJson(`${DATA_DIR}/viability_${id}.json`);
        recordGeneratedAt(data);
        if (data.passed) summary.viability.passed += 1;
        let card;
        try {
          card = viabilityCard(data);
        } catch (e) {
          console.error(`viability ${id} render failed`, e);
          card = errorCard(data.name || VIABILITY_NAMES[id] || id, e.message || String(e));
        }
        slot.replaceWith(card);
      } catch (e) {
        if (e.notFound) {
          slot.replaceWith(
            placeholderCard(
              VIABILITY_NAMES[id] || id,
              `cargo test -p basin-integration-tests --test viability_${id} -- --nocapture`
            )
          );
        } else {
          console.error(`viability ${id} fetch failed`, e);
          slot.replaceWith(errorCard(VIABILITY_NAMES[id] || id, e.message || String(e)));
        }
      }
      updateSummaryUI();
    }
  }

  async function loadScaling(ids, container) {
    summary.scaling.total = ids.length;
    for (const id of ids) {
      const slot = el("div");
      container.appendChild(slot);
      try {
        const data = await fetchJson(`${DATA_DIR}/scaling_${id}.json`);
        recordGeneratedAt(data);
        if (data.passed) summary.scaling.passed += 1;
        let card;
        try {
          card = scalingCard(data);
        } catch (e) {
          console.error(`scaling ${id} render failed`, e);
          card = errorCard(data.name || SCALING_NAMES[id] || id, e.message || String(e));
        }
        slot.replaceWith(card);
      } catch (e) {
        if (e.notFound) {
          slot.replaceWith(
            placeholderCard(
              SCALING_NAMES[id] || id,
              `cargo test -p basin-integration-tests --test scaling_${id} -- --nocapture`
            )
          );
        } else {
          console.error(`scaling ${id} fetch failed`, e);
          slot.replaceWith(errorCard(SCALING_NAMES[id] || id, e.message || String(e)));
        }
      }
      updateSummaryUI();
    }
  }

  async function loadCompare(ids, container) {
    for (const id of ids) {
      const slot = el("div");
      container.appendChild(slot);
      try {
        const data = await fetchJson(`${DATA_DIR}/compare_${id}.json`);
        recordGeneratedAt(data);
        summary.compareCount += 1;
        let card;
        try {
          if (data.available === false) {
            card = compareUnavailableCard(id, data.note);
          } else {
            summary.compareAvailable = true;
            card = compareCard(data);
          }
        } catch (e) {
          console.error(`compare ${id} render failed`, e);
          card = errorCard(data.name || COMPARE_NAMES[id] || id, e.message || String(e));
        }
        slot.replaceWith(card);
      } catch (e) {
        if (e.notFound) {
          slot.replaceWith(
            placeholderCard(
              COMPARE_NAMES[id] || id,
              `cargo test -p basin-integration-tests --test compare_${id} -- --nocapture`
            )
          );
        } else {
          console.error(`compare ${id} fetch failed`, e);
          slot.replaceWith(errorCard(COMPARE_NAMES[id] || id, e.message || String(e)));
        }
      }
      updateSummaryUI();
    }
  }

  async function main() {
    const viabilityGrid = document.getElementById("viability-grid");
    const scalingStack = document.getElementById("scaling-stack");
    const compareStack = document.getElementById("compare-stack");

    let manifest;
    try {
      manifest = await fetchJson(`${DATA_DIR}/manifest.json`);
    } catch (e) {
      const msg =
        location.protocol === "file:"
          ? "Manifest could not be loaded. Run `python3 dashboard/bundle.py` after `cargo test` to regenerate dashboard/data/results.js, then reload."
          : `Manifest could not be loaded: ${e.message || e}`;
      const banner = el("div", { class: "card card--error" }, [
        cardHeader("Dashboard data unavailable", msg, errorPill("ERROR")),
      ]);
      viabilityGrid.appendChild(banner);
      return;
    }

    const v = Array.isArray(manifest.viability) ? manifest.viability : [];
    const s = Array.isArray(manifest.scaling) ? manifest.scaling : [];
    const c = Array.isArray(manifest.compare) ? manifest.compare : [];

    summary.viability.total = v.length;
    summary.scaling.total = s.length;
    updateSummaryUI();

    // Run sections in parallel
    await Promise.all([
      loadViability(v, viabilityGrid),
      loadScaling(s, scalingStack),
      loadCompare(c, compareStack),
    ]);

    updateSummaryUI();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", main);
  } else {
    main();
  }
})();
