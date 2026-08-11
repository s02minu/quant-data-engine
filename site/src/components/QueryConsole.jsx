import React, { useState } from "react";

import { BARS_URL, USING_SAMPLE } from "../config.js";
import { runQuery } from "../duck.js";

// Presets read from BARS_URL — the bundled sample by default, or the live public mart
// once VITE_PUBLIC_BASE_URL is set. All run entirely in the browser.
const PRESETS = [
  {
    label: "BTC — price, ATR & realized vol",
    sql: `SELECT symbol, date, close, atr_14, realized_vol_30d
FROM read_parquet('${BARS_URL}')
WHERE symbol = 'BTCUSDT' AND source = 'binance'
ORDER BY date DESC
LIMIT 25;`,
  },
  {
    label: "Cross-venue basis: Binance vs Coinbase",
    sql: `SELECT b.date,
       b.close AS binance,
       c.close AS coinbase,
       round((b.close / c.close - 1) * 1e4, 1) AS basis_bps
FROM read_parquet('${BARS_URL}') b
JOIN read_parquet('${BARS_URL}') c
  ON b.date = c.date AND b.symbol = c.symbol
WHERE b.symbol = 'BTCUSDT' AND b.source = 'binance' AND c.source = 'coinbase'
ORDER BY b.date DESC
LIMIT 25;`,
  },
  {
    label: "Highest-volatility days across coins",
    sql: `SELECT symbol, date, realized_vol_30d, close
FROM read_parquet('${BARS_URL}')
WHERE source = 'binance'
ORDER BY realized_vol_30d DESC NULLS LAST
LIMIT 25;`,
  },
];

export default function QueryConsole() {
  const [sql, setSql] = useState(PRESETS[0].sql);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [running, setRunning] = useState(false);
  const [elapsed, setElapsed] = useState(null);

  async function execute() {
    setRunning(true);
    setError(null);
    const t0 = performance.now();
    try {
      const res = await runQuery(sql);
      setResult(res);
      setElapsed(((performance.now() - t0) / 1000).toFixed(2));
    } catch (e) {
      setError(String(e?.message || e));
      setResult(null);
    } finally {
      setRunning(false);
    }
  }

  return (
    <section className="section console-section" id="console">
      <h2>Query it live, in your browser</h2>
      <p className="section-lede">
        This runs a real DuckDB engine compiled to WebAssembly, right here on this page. It
        fetches Parquet straight from R2 and computes on your machine — there is no backend.
        {USING_SAMPLE ? " (Querying a bundled sample of BTC/ETH/SOL until the public bucket goes live.)" : ""}
      </p>

      <div className="presets">
        {PRESETS.map((p) => (
          <button key={p.label} className="preset" onClick={() => setSql(p.sql)}>
            {p.label}
          </button>
        ))}
      </div>

      <div className="console">
        <textarea
          className="sql"
          spellCheck={false}
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          rows={sql.split("\n").length + 1}
        />
        <div className="console-bar">
          <button className="btn primary" onClick={execute} disabled={running}>
            {running ? "Running…" : "▶ Run query"}
          </button>
          {elapsed && !error && (
            <span className="console-meta">
              {result?.rows.length ?? 0} rows · {elapsed}s · computed in your browser
            </span>
          )}
        </div>
      </div>

      {error && <div className="console-error">⚠ {error}</div>}

      {result && !error && (
        <div className="table-wrap">
          <table className="result">
            <thead>
              <tr>
                {result.columns.map((c) => (
                  <th key={c}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, i) => (
                <tr key={i}>
                  {result.columns.map((c) => (
                    <td key={c}>{formatCell(row[c])}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function formatCell(v) {
  if (v == null) return "";
  if (typeof v === "number") return Number.isInteger(v) ? v : Number(v.toFixed(4));
  // Arrow date/timestamp values render as ISO; trim to the date for readability.
  const s = String(v);
  return s.length > 10 && s.includes("T") ? s.slice(0, 10) : s;
}
