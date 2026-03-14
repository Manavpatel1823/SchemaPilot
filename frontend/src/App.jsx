import { useEffect, useState } from "react";

const DEFAULT_RESULT_SOURCE = "/last_result.json";

function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function normalizeRows(rows) {
  if (!Array.isArray(rows)) return [];

  return rows.map((row) => {
    if (isRecord(row)) return row;
    return { value: row };
  });
}

function normalizeColumns(columns, rows) {
  if (Array.isArray(columns) && columns.length) {
    return columns.map((column) => String(column));
  }

  if (!rows.length) return [];
  return Object.keys(rows[0]);
}

function normalizeResult(result, index) {
  const data = isRecord(result?.data) ? result.data : {};
  const rows = normalizeRows(data.rows ?? result?.rows);
  const columns = normalizeColumns(data.columns ?? result?.columns, rows);

  return {
    label:
      typeof result?.label === "string" && result.label.trim()
        ? result.label
        : `Result ${index + 1}`,
    sql: typeof result?.sql === "string" ? result.sql : "",
    error: typeof result?.error === "string" ? result.error : "",
    chart: isRecord(result?.chart) ? result.chart : null,
    data: {
      columns,
      rows,
      row_count:
        typeof data.row_count === "number" ? data.row_count : rows.length,
    },
  };
}

function normalizePayload(payload) {
  if (Array.isArray(payload)) {
    return {
      question: "",
      plan: null,
      results: payload.map((result, index) => normalizeResult(result, index)),
    };
  }

  if (isRecord(payload)) {
    if (Array.isArray(payload.results)) {
      return {
        question: typeof payload.question === "string" ? payload.question : "",
        plan: payload.plan ?? null,
        results: payload.results.map((result, index) =>
          normalizeResult(result, index),
        ),
      };
    }

    if (
      payload.data ||
      payload.rows ||
      payload.columns ||
      payload.sql ||
      payload.error
    ) {
      return {
        question: typeof payload.question === "string" ? payload.question : "",
        plan: payload.plan ?? null,
        results: [normalizeResult(payload, 0)],
      };
    }
  }

  return {
    question: "",
    plan: null,
    results: [],
  };
}

function resolveResultSource(rawValue) {
  const value = (rawValue || "").trim();
  if (!value) return DEFAULT_RESULT_SOURCE;

  const publicMatch = value.match(/[\\/]frontend[\\/]public([\\/].+)$/i);
  if (publicMatch) {
    return `/${publicMatch[1].replace(/\\/g, "/").replace(/^\/+/, "")}`;
  }

  const nestedPublicMatch = value.match(/[\\/]public([\\/].+)$/i);
  if (nestedPublicMatch) {
    return `/${nestedPublicMatch[1].replace(/\\/g, "/").replace(/^\/+/, "")}`;
  }

  if (/^https?:\/\//i.test(value)) {
    return value;
  }

  if (/^file:\/\//i.test(value)) {
    throw new Error(
      "Browser fetch cannot read file:// paths. Use a public URL or choose the JSON file below.",
    );
  }

  if (/^(\/Users\/|\/home\/|[A-Za-z]:\\)/.test(value)) {
    throw new Error(
      "This looks like a local filesystem path outside frontend/public. Move the file into frontend/public or load it with the file picker.",
    );
  }

  if (value.startsWith("/")) {
    return value;
  }

  return `/${value.replace(/^\/+/, "")}`;
}

function withCacheBust(url) {
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}t=${Date.now()}`;
}

function DataTable({ columns, rows }) {
  if (!rows.length) {
    return <div style={{ color: "#666" }}>No tabular rows available.</div>;
  }

  return (
    <div
      style={{ overflowX: "auto", border: "1px solid #eee", borderRadius: 10 }}
    >
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th
                key={column}
                style={{
                  textAlign: "left",
                  padding: 8,
                  borderBottom: "1px solid #eee",
                }}
              >
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 50).map((row, rowIndex) => (
            <tr key={rowIndex}>
              {columns.map((column) => (
                <td
                  key={column}
                  style={{ padding: 8, borderBottom: "1px solid #f5f5f5" }}
                >
                  {String(row[column] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 50 && (
        <div style={{ padding: 8, color: "#666" }}>Showing first 50 rows</div>
      )}
    </div>
  );
}

function ChartView({ rows, chart }) {
  if (!rows?.length) {
    return <div style={{ color: "#666" }}>No rows to chart.</div>;
  }

  if (chart?.type === "bar" && chart?.x && chart?.y) {
    const numericRows = rows
      .map((row) => ({
        label: String(row[chart.x] ?? ""),
        value: Number(row[chart.y] ?? 0),
      }))
      .filter((row) => Number.isFinite(row.value));

    if (!numericRows.length) {
      return <div style={{ color: "#666" }}>Bar chart data is invalid.</div>;
    }

    const maxValue = Math.max(...numericRows.map((row) => row.value), 0);
    const chartWidth = 760;
    const chartHeight = 360;
    const margin = { top: 20, right: 24, bottom: 72, left: 64 };
    const innerWidth = chartWidth - margin.left - margin.right;
    const innerHeight = chartHeight - margin.top - margin.bottom;
    const barCount = numericRows.length;
    const slotWidth = innerWidth / Math.max(barCount, 1);
    const barWidth = Math.max(24, Math.min(64, slotWidth * 0.58));
    const tickCount = 5;
    const yTicks = Array.from({ length: tickCount + 1 }, (_, index) => {
      const value = (maxValue / tickCount) * (tickCount - index);
      const y = margin.top + (innerHeight * index) / tickCount;

      return {
        value: Number.isInteger(value) ? value : Number(value.toFixed(1)),
        y,
      };
    });

    return (
      <div
        style={{
          padding: 16,
          border: "1px solid #eee",
          borderRadius: 10,
          background: "#fcfcfc",
          overflowX: "auto",
        }}
      >
        <svg
          viewBox={`0 0 ${chartWidth} ${chartHeight}`}
          style={{ width: "100%", minWidth: chartWidth, height: "auto" }}
          role="img"
          aria-label={`${chart.y} by ${chart.x}`}
        >
          <rect
            x="0"
            y="0"
            width={chartWidth}
            height={chartHeight}
            fill="#fcfcfc"
            rx="12"
          />

          {yTicks.map((tick) => (
            <g key={`tick-${tick.value}`}>
              <line
                x1={margin.left}
                x2={chartWidth - margin.right}
                y1={tick.y}
                y2={tick.y}
                stroke="#dbe3f0"
                strokeDasharray="4 4"
              />
              <text
                x={margin.left - 10}
                y={tick.y + 4}
                textAnchor="end"
                fontSize="12"
                fill="#4b5563"
              >
                {tick.value}
              </text>
            </g>
          ))}

          <line
            x1={margin.left}
            x2={margin.left}
            y1={margin.top}
            y2={chartHeight - margin.bottom}
            stroke="#111827"
            strokeWidth="1.5"
          />
          <line
            x1={margin.left}
            x2={chartWidth - margin.right}
            y1={chartHeight - margin.bottom}
            y2={chartHeight - margin.bottom}
            stroke="#111827"
            strokeWidth="1.5"
          />

          {numericRows.map((row, index) => {
            const barHeight =
              maxValue === 0 ? 0 : (row.value / maxValue) * innerHeight;
            const x =
              margin.left + slotWidth * index + (slotWidth - barWidth) / 2;
            const y = margin.top + innerHeight - barHeight;
            const labelX = margin.left + slotWidth * index + slotWidth / 2;

            return (
              <g key={`${row.label}-${row.value}`}>
                <rect
                  x={x}
                  y={y}
                  width={barWidth}
                  height={barHeight}
                  rx="6"
                  fill="url(#barGradient)"
                />
                <text
                  x={labelX}
                  y={y - 8}
                  textAnchor="middle"
                  fontSize="12"
                  fill="#111827"
                >
                  {row.value}
                </text>
                <text
                  x={labelX}
                  y={chartHeight - margin.bottom + 18}
                  textAnchor="end"
                  fontSize="12"
                  fill="#374151"
                  transform={`rotate(-28 ${labelX} ${chartHeight - margin.bottom + 18})`}
                >
                  {row.label}
                </text>
              </g>
            );
          })}

          <text
            x={chartWidth / 2}
            y={chartHeight - 16}
            textAnchor="middle"
            fontSize="13"
            fill="#111827"
          >
            {chart.x}
          </text>
          <text
            x="18"
            y={chartHeight / 2}
            textAnchor="middle"
            fontSize="13"
            fill="#111827"
            transform={`rotate(-90 18 ${chartHeight / 2})`}
          >
            {chart.y}
          </text>

          <defs>
            <linearGradient id="barGradient" x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" stopColor="#38bdf8" />
              <stop offset="100%" stopColor="#2563eb" />
            </linearGradient>
          </defs>
        </svg>
      </div>
    );
  }

  return (
    <div style={{ color: "#666" }}>Chart not suggested; showing table.</div>
  );
}

export default function App() {
  const [resp, setResp] = useState(null);
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);
  const [sourceInput, setSourceInput] = useState(DEFAULT_RESULT_SOURCE);
  const [resolvedSource, setResolvedSource] = useState(DEFAULT_RESULT_SOURCE);

  async function loadResult(sourceOverride) {
    setLoading(true);
    setErr("");

    try {
      const nextSource = sourceOverride ?? sourceInput;
      const resolved = resolveResultSource(nextSource);
      setResolvedSource(resolved);

      const response = await fetch(withCacheBust(resolved));
      if (!response.ok) {
        throw new Error(`Could not load JSON from ${resolved}`);
      }

      const data = await response.json();
      setResp(normalizePayload(data));
    } catch (error) {
      setResp(null);
      setErr(error.message || String(error));
    } finally {
      setLoading(false);
    }
  }

  async function handleFilePick(event) {
    const file = event.target.files?.[0];
    if (!file) return;

    setLoading(true);
    setErr("");

    try {
      const fileText = await file.text();
      const data = JSON.parse(fileText);
      setResolvedSource(file.name);
      setResp(normalizePayload(data));
    } catch (error) {
      setResp(null);
      setErr(`Could not read JSON file: ${error.message || String(error)}`);
    } finally {
      event.target.value = "";
      setLoading(false);
    }
  }

  useEffect(() => {
    loadResult(DEFAULT_RESULT_SOURCE);
  }, []);

  return (
    <div
      style={{
        maxWidth: 1100,
        margin: "24px auto",
        padding: 16,
        fontFamily: "system-ui",
      }}
    >
      <h2>AI Analytics Agent</h2>

      <div
        style={{
          display: "grid",
          gap: 12,
          marginBottom: 16,
          padding: 16,
          border: "1px solid #eee",
          borderRadius: 12,
        }}
      >
        <label style={{ display: "grid", gap: 6 }}>
          <span style={{ fontWeight: 600 }}>JSON source</span>
          <input
            type="text"
            value={sourceInput}
            onChange={(event) => setSourceInput(event.target.value)}
            placeholder="/last_result.json or /Users/.../frontend/public/last_result.json"
            style={{
              width: "100%",
              boxSizing: "border-box",
              padding: "12px 14px",
              borderRadius: 10,
              border: "1px solid #ddd",
            }}
          />
        </label>

        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button
            onClick={() => loadResult()}
            disabled={loading}
            style={{
              padding: "12px 16px",
              borderRadius: 10,
              border: "1px solid #ddd",
              cursor: loading ? "default" : "pointer",
            }}
          >
            {loading ? "Loading..." : "Reload Result"}
          </button>

          <label
            style={{
              padding: "12px 16px",
              borderRadius: 10,
              border: "1px solid #ddd",
              cursor: "pointer",
            }}
          >
            Open Local JSON
            <input
              type="file"
              accept="application/json,.json"
              onChange={handleFilePick}
              style={{ display: "none" }}
            />
          </label>
        </div>

        <div style={{ color: "#666", fontSize: 14 }}>
          Resolved source: <code>{resolvedSource}</code>
        </div>
      </div>

      {err && <div style={{ marginTop: 12, color: "crimson" }}>{err}</div>}

      {!resp && !err && (
        <div style={{ color: "#666" }}>
          Save the JSON into <code>frontend/public</code>, or load a local JSON
          file here.
        </div>
      )}

      {resp?.question && (
        <div
          style={{
            marginTop: 18,
            padding: 14,
            border: "1px solid #eee",
            borderRadius: 12,
            background: "#fafafa",
          }}
        >
          <strong>Question:</strong> {resp.question}
        </div>
      )}

      {resp?.results?.length === 0 && !err && (
        <div style={{ marginTop: 18, color: "#666" }}>
          JSON loaded, but it does not contain any renderable results yet.
        </div>
      )}

      {resp?.results?.map((result, index) => (
        <div
          key={index}
          style={{
            marginTop: 18,
            padding: 14,
            border: "1px solid #eee",
            borderRadius: 12,
          }}
        >
          <h3 style={{ marginTop: 0 }}>{result.label}</h3>

          {result.error && (
            <div
              style={{
                marginBottom: 12,
                padding: 12,
                borderRadius: 10,
                background: "#fff5f5",
                color: "#b42318",
              }}
            >
              {result.error}
            </div>
          )}

          {result.sql && (
            <>
              <h4>SQL</h4>
              <pre
                style={{
                  background: "#0b1020",
                  color: "#e7e7e7",
                  padding: 12,
                  borderRadius: 10,
                  overflowX: "auto",
                }}
              >
                {result.sql}
              </pre>
            </>
          )}

          <h4>Chart</h4>
          <ChartView rows={result.data.rows} chart={result.chart} />

          <h4>Data</h4>
          <DataTable columns={result.data.columns} rows={result.data.rows} />
        </div>
      ))}

      {resp?.plan && (
        <details style={{ marginTop: 18 }}>
          <summary>Plan (debug)</summary>
          <pre
            style={{
              background: "#f7f7f7",
              padding: 12,
              borderRadius: 10,
              overflowX: "auto",
            }}
          >
            {JSON.stringify(resp.plan, null, 2)}
          </pre>
        </details>
      )}
    </div>
  );
}
