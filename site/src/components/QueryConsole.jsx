import React, { useEffect, useState } from "react";

import ResultChart from "./ResultChart.jsx";
import SqlEditor from "./SqlEditor.jsx";
import { BARS_URL, USING_SAMPLE } from "../config.js";
import { getDb, runQuery } from "../duck.js";
import { useTypewriter } from "../hooks.js";

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
  const [activePreset, setActivePreset] = useState(PRESETS[0].label);
  const [booting, setBooting] = useState(true);
  const [autoRan, setAutoRan] = useState(false);

  // Type the opening query out, so the first thing a visitor sees is the product
  // working rather than a claim that it works.
  const [typed, typingDone] = useTypewriter(PRESETS[0].sql, { cps: 110 });

  // DuckDB-WASM has to download and instantiate before anything can run; say so
  // rather than leaving a dead button.
  useEffect(() => {
    let alive = true;
    getDb()
      .then(() => alive && setBooting(false))
      .catch(() => alive && setBooting(false));
    return () => {
      alive = false;
    };
  }, []);

  // Once the engine is up and the query has finished typing, run it once.
  // Guarded on state rather than a ref: StrictMode's dev double-mount resets state
  // while a ref set during the discarded pass would suppress the retry forever,
  // leaving the hero sitting at "Ready" with nothing shown.
  useEffect(() => {
    if (booting || !typingDone || autoRan || running) return;
    setAutoRan(true);
    execute(PRESETS[0].sql);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [booting, typingDone, autoRan, running]);

  function onKeyDown(e) {
    // ⌘/Ctrl + Enter runs, the way every SQL console does.
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      if (!running) execute();
    }
  }

  async function execute(override) {
    const q = override ?? sql;
    setRunning(true);
    setError(null);
    const t0 = performance.now();
    try {
      const res = await runQuery(q);
      setResult(res);
      setElapsed(((performance.now() - t0) / 1000).toFixed(2));
    } catch (e) {
      setError(String(e?.message || e));
      setResult(null);
    } finally {
      setRunning(false);
    }
  }

  const typing = !typingDone;
  const status = booting
    ? "Starting DuckDB in your browser…"
    : running
      ? "Running…"
      : error
        ? "Query failed"
        : elapsed
          ? `${result?.rows.length ?? 0} rows · ${elapsed}s · computed on your machine`
          : "Ready";

  return (
    <div className="console-shell" id="console">
      <div className="console-chrome">
        <span className={`console-lamp${booting || running ? " busy" : ""}`} aria-hidden="true" />
        <span className="console-title">duckdb-wasm · your browser</span>
        <span className="console-status" role="status">
          {status}
        </span>
      </div>

      <div className="console">
        <SqlEditor
          value={typing ? typed : sql}
          readOnly={typing}
          onChange={(e) => {
            setSql(e.target.value);
            setActivePreset(null);
          }}
          onKeyDown={onKeyDown}
          label="SQL query"
        />
        {(running || booting) && <div className="console-scan" aria-hidden="true" />}
        <div className="console-bar">
          <button className="btn primary" onClick={() => execute()} disabled={running || booting}>
            {running ? "Running…" : "Run query"}
          </button>
          <span className="console-hint">
            <kbd>⌘</kbd>/<kbd>Ctrl</kbd>+<kbd>Enter</kbd>
          </span>
        </div>
      </div>

      <div className="presets" role="group" aria-label="Example queries">
        {PRESETS.map((p) => (
          <button
            key={p.label}
            className={`preset${activePreset === p.label ? " active" : ""}`}
            disabled={booting}
            onClick={() => {
              setSql(p.sql);
              setActivePreset(p.label);
              execute(p.sql);
            }}
          >
            {p.label}
          </button>
        ))}
      </div>

      {error && <div className="console-error">{error}</div>}

      {result && !error && <ResultChart key={elapsed} result={result} />}

      {result && !error && (
        <div className="table-wrap result-wrap">
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
                <tr key={i} className="row-in" style={{ animationDelay: `${Math.min(i, 14) * 22}ms` }}>
                  {result.columns.map((c) => (
                    <td key={c}>{formatCell(row[c], result.temporal?.has(c))}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <p className="console-foot">
        A real DuckDB engine compiled to WebAssembly, fetching Parquet over HTTP range
        requests. There is no backend — this page has no server to send your query to.
        {USING_SAMPLE ? " Reading a bundled BTC/ETH/SOL sample until the public bucket goes live." : ""}
      </p>
    </div>
  );
}

function formatCell(v, isTemporal) {
  if (v == null) return "";
  // Date/timestamp columns arrive as epoch milliseconds; show them as dates.
  if (isTemporal && typeof v === "number") {
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? String(v) : d.toISOString().slice(0, 10);
  }
  if (typeof v === "number") return Number.isInteger(v) ? v : Number(v.toFixed(4));
  // Anything already stringly-typed: trim ISO timestamps down to the date.
  const s = String(v);
  return s.length > 10 && s.includes("T") ? s.slice(0, 10) : s;
}
