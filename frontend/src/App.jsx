import { useState } from "react";
import {
  ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, Legend, CartesianGrid,
} from "recharts";

function DataTable({ columns, rows }) {
  return (
    <div style={{ overflowX: "auto", border: "1px solid #eee", borderRadius: 10 }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c} style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 50).map((r, i) => (
            <tr key={i}>
              {columns.map((c) => (
                <td key={c} style={{ padding: 8, borderBottom: "1px solid #f5f5f5" }}>
                  {String(r[c] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 50 && <div style={{ padding: 8, color: "#666" }}>Showing first 50 rows</div>}
    </div>
  );
}

function ChartView({ rows, chart }) {
  if (!rows?.length) return <div style={{ color: "#666" }}>No rows to chart.</div>;

  if (chart.type === "bar") {
    return (
      <div style={{ width: "100%", height: 360 }}>
        <ResponsiveContainer>
          <BarChart data={rows}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey={chart.x} />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey={chart.y} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  }

  if (chart.type === "grouped_bar") {
    return (
      <div style={{ width: "100%", height: 380 }}>
        <ResponsiveContainer>
          <BarChart data={rows}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey={chart.x} />
            <YAxis />
            <Tooltip />
            <Legend />
            {chart.ys.map((k) => <Bar key={k} dataKey={k} />)}
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  }

  if (chart.type === "histogram") {
    return <div style={{ color: "#666" }}>Histogram detected. Next step: binning.</div>;
  }

  return <div style={{ color: "#666" }}>Chart not suggested; showing table.</div>;
}

export default function App() {
  const [question, setQuestion] = useState("Which department has the highest number of enrolled students?");
  const [loading, setLoading] = useState(false);
  const [resp, setResp] = useState(null);
  const [err, setErr] = useState("");

  async function ask() {
    setErr("");
    setLoading(true);
    setResp(null);
    try {
      const r = await fetch("http://localhost:8000/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question }),
      });
      const data = await r.json();
      if (!r.ok || data.error || data.missing_info) {
        throw new Error(data.missing_info || data.error || "Request failed");
      }
      setResp(data);
    } catch (e) {
      setErr(e.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ maxWidth: 1100, margin: "24px auto", padding: 16, fontFamily: "system-ui" }}>
      <h2>AI Analytics Agent</h2>

      <div style={{ display: "flex", gap: 8 }}>
        <input
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          style={{ flex: 1, padding: 12, borderRadius: 10, border: "1px solid #ddd" }}
          placeholder="Ask a DB question..."
        />
        <button
          onClick={ask}
          disabled={loading}
          style={{ padding: "12px 16px", borderRadius: 10, border: "1px solid #ddd", cursor: "pointer" }}
        >
          {loading ? "Asking..." : "Ask"}
        </button>
      </div>

      {err && <div style={{ marginTop: 12, color: "crimson" }}>{err}</div>}

      {resp?.results?.map((r, idx) => (
        <div key={idx} style={{ marginTop: 18, padding: 14, border: "1px solid #eee", borderRadius: 12 }}>
          <h3 style={{ marginTop: 0 }}>{r.label || `Result ${idx + 1}`}</h3>

          {r.error ? (
            <pre>{JSON.stringify(r, null, 2)}</pre>
          ) : (
            <>
              <h4>SQL</h4>
              <pre style={{ background: "#0b1020", color: "#e7e7e7", padding: 12, borderRadius: 10, overflowX: "auto" }}>
                {r.sql}
              </pre>

              <h4>Chart</h4>
              <ChartView rows={r.data.rows} chart={r.chart} />

              <h4>Data</h4>
              <DataTable columns={r.data.columns} rows={r.data.rows} />
            </>
          )}
        </div>
      ))}

      {resp && (
        <details style={{ marginTop: 18 }}>
          <summary>Plan (debug)</summary>
          <pre style={{ background: "#f7f7f7", padding: 12, borderRadius: 10, overflowX: "auto" }}>
            {JSON.stringify(resp.plan, null, 2)}
          </pre>
        </details>
      )}
    </div>
  );
}