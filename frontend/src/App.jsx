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

function DataTable({ columns, rows }) {
  if (!rows.length) {
    return <div style={{ color: "#666" }}>No tabular rows available.</div>;
  }

  return (
    <table style={{ width: "100%", borderCollapse: "collapse" }}>
      <thead>
        <tr>
          {columns.map((column) => (
            <th key={column} style={{ borderBottom: "1px solid #eee" }}>
              {column}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.slice(0, 50).map((row, i) => (
          <tr key={i}>
            {columns.map((column) => (
              <td key={column}>{String(row[column] ?? "")}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function ChartView({ rows, chart }) {
  if (!rows?.length || !chart) {
    return <div style={{ color: "#666" }}>No rows to chart.</div>;
  }

  /* ---------------- BAR CHART ---------------- */

  if (chart.chart_type === "bar" && chart.x && chart.y) {
    const data = rows.map((r) => ({
      label: String(r[chart.x]),
      value: Number(r[chart.y]),
    }));

    const max = Math.max(...data.map((d) => d.value), 0);

    return (
      <div>
        {data.map((d, i) => (
          <div key={i} style={{ marginBottom: 6 }}>
            <div>{d.label}</div>
            <div
              style={{
                height: 20,
                width: `${(d.value / max) * 100}%`,
                background: "#3b82f6",
              }}
            />
          </div>
        ))}
      </div>
    );
  }

  /* ---------------- SCATTER ---------------- */

  if (chart.chart_type === "scatter" && chart.x && chart.y) {
    const points = rows.map((r) => ({
      x: Number(r[chart.x]),
      y: Number(r[chart.y]),
    }));

    const width = 500;
    const height = 350;

    const maxX = Math.max(...points.map((p) => p.x), 1);
    const maxY = Math.max(...points.map((p) => p.y), 1);

    return (
      <svg width={width} height={height} style={{ border: "1px solid #eee" }}>
        {points.map((p, i) => (
          <circle
            key={i}
            cx={(p.x / maxX) * (width - 40) + 20}
            cy={height - (p.y / maxY) * (height - 40) - 20}
            r={4}
            fill="#2563eb"
          />
        ))}
      </svg>
    );
  }

  /* ---------------- HISTOGRAM ---------------- */

  if (chart.chart_type === "histogram" && chart.x) {
    const values = rows.map((r) => Number(r[chart.x]));

    const bins = {};
    const binSize = 5;

    values.forEach((v) => {
      const b = Math.floor(v / binSize) * binSize;
      bins[b] = (bins[b] || 0) + 1;
    });

    const data = Object.entries(bins).map(([k, v]) => ({
      label: `${k}-${Number(k) + 4}`,
      value: v,
    }));

    const max = Math.max(...data.map((d) => d.value), 1);

    return (
      <div>
        {data.map((d, i) => (
          <div key={i} style={{ marginBottom: 6 }}>
            <div>{d.label}</div>
            <div
              style={{
                height: 20,
                width: `${(d.value / max) * 100}%`,
                background: "#10b981",
              }}
            />
          </div>
        ))}
      </div>
    );
  }

  return (
    <div style={{ color: "#666" }}>Chart not supported; showing table.</div>
  );
}

export default function App() {
  const [resp, setResp] = useState(null);

  useEffect(() => {
    fetch("/last_result.json")
      .then((r) => r.json())
      .then((data) => setResp(normalizePayload(data)));
  }, []);

  return (
    <div
      style={{ maxWidth: 1000, margin: "20px auto", fontFamily: "system-ui" }}
    >
      <h2>AI Analytics Agent</h2>

      {resp?.results?.map((result, i) => (
        <div key={i} style={{ marginTop: 30 }}>
          <h3>{result.label}</h3>

          <h4>SQL</h4>
          <pre>{result.sql}</pre>

          <h4>Chart</h4>
          <ChartView rows={result.data.rows} chart={result.chart} />

          <h4>Data</h4>
          <DataTable columns={result.data.columns} rows={result.data.rows} />
        </div>
      ))}
    </div>
  );
}
