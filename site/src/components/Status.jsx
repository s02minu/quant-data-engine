import React, { useEffect, useMemo, useState } from "react";

import ThemeToggle from "./ThemeToggle.jsx";
import { tokenize } from "./SqlEditor.jsx";
import { DQ_RUNS_URL, DQ_VIOLATIONS_URL, GITHUB_URL, USING_SAMPLE } from "../config.js";
import { runQuery } from "../duck.js";
import { compact } from "../format.js";
import { ageMs, healthOf, healthSummary, humanAge } from "../health.js";
import { useScrollProgress } from "../hooks.js";

// Every panel names the query behind it. A status page is normally an assertion you
// either believe or don't; this one publishes its own evidence, so the number and the
// SQL that produced it sit side by side and the visitor can re-run either.
function Evidence({ sql }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="evidence">
      <button className="evidence-toggle" onClick={() => setOpen(!open)} aria-expanded={open}>
        {open ? "hide query" : "how is this measured?"}
      </button>
      {open && (
        <pre className="evidence-sql">
          <code>
            {tokenize(sql).map((t, i) => (
              <span key={i} className={`tk-${t.t}`}>
                {t.v}
              </span>
            ))}
          </code>
        </pre>
      )}
    </div>
  );
}

// Freshness is the number that actually matters day to day, so it ticks rather than
// showing whatever it happened to be when the catalogue was generated.
function useNow(intervalMs = 1000) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now;
}

// Grading and age formatting come from health.js — the same module the home page
// uses, so the two surfaces cannot report different answers for the same source.

export default function Status({ catalogue }) {
  const [runs, setRuns] = useState(null);
  const [violations, setViolations] = useState([]);
  const [error, setError] = useState(null);
  const progress = useScrollProgress();
  const now = useNow();

  useEffect(() => {
    if (!DQ_RUNS_URL) {
      setError("The public bucket is not configured for this build.");
      return;
    }
    runQuery(`SELECT * FROM read_parquet('${DQ_RUNS_URL}') ORDER BY run_ts DESC`)
      .then((r) => setRuns(r.rows))
      .catch((e) => setError(String(e?.message || e)));

    // No violations file exists until something has actually failed, so a 404 here
    // is the healthy case, not an error.
    runQuery(`SELECT * FROM read_parquet('${DQ_VIOLATIONS_URL}') ORDER BY run_ts DESC LIMIT 200`)
      .then((r) => setViolations(r.rows))
      .catch(() => setViolations([]));
  }, []);

  const sources = catalogue?.sources ?? [];

  const rows = useMemo(
    () =>
      sources
        .map((s) => ({ ...s, age: ageMs(s, now), state: healthOf(s, now) }))
        .sort((a, b) => (b.age ?? 0) - (a.age ?? 0)),
    [sources, now],
  );

  const { late, warn, degraded } = healthSummary(sources, now);
  const lastRun = runs?.[0];
  const overall = late > 0 ? "late" : warn > 0 ? "warn" : "ok";

  return (
    <div className="app status-page">
      <nav className="nav">
        <div className="nav-inner">
          <a className="brand" href="/">
            <span className="brand-rule" />
            quant-data-engine
          </a>
          <div className="nav-right">
            <div className="nav-links">
              <a href="/">Home</a>
              <a href="/status" className="active" aria-current="true">
                Status
              </a>
              <a href={GITHUB_URL} target="_blank" rel="noreferrer">
                GitHub
              </a>
            </div>
            <ThemeToggle />
          </div>
        </div>
        <div className="nav-progress" style={{ width: `${progress * 100}%` }} aria-hidden="true" />
      </nav>

      <main className="status-main">
        <header className="status-head">
          <span className={`status-pill status-${overall}`}>
            <span className="status-dot" />
            {overall === "ok"
              ? `All ${rows.length} sources current`
              : `${degraded} of ${rows.length} sources behind schedule`}
          </span>
          <h1>System status</h1>
          <p className="lede">
            Every number here is computed in your browser from the same public Parquet the
            rest of the lake is served from. Nothing is asserted that you cannot re-run
            yourself.
          </p>
        </header>

        {error && <div className="console-error">{error}</div>}

        <section className="status-section">
          <div className="section-head">
            <h2>Source freshness</h2>
            <span className="rule" />
          </div>
          <p className="section-lede">
            Age counts up live from each source&apos;s newest observation. Sources are graded
            against their own cadence — a daily series and an 8-hourly one do not share a
            definition of &ldquo;late&rdquo;.
          </p>

          <div className="freshness">
            {rows.map((r) => (
              <div className={`fresh-row fresh-${r.state}`} key={r.name}>
                <span className="fresh-dot" aria-hidden="true" />
                <span className="fresh-name">{r.name}</span>
                <span className="fresh-group">{r.group}</span>
                <span className="fresh-rows fig">{compact(r.rows || 0)}</span>
                <span className="fresh-age fig">{humanAge(r.age)}</span>
              </div>
            ))}
          </div>
          <Evidence
            sql={`-- freshness comes from the published catalogue\nSELECT name, "group", rows, freshness\nFROM read_json_auto('catalogue.json')\nORDER BY freshness;`}
          />
        </section>

        <section className="status-section">
          <div className="section-head">
            <h2>Quality checks</h2>
            <span className="rule" />
          </div>
          <p className="section-lede">
            Every night the pipeline re-checks freshness, null tolerance, bitemporal
            ordering and microstructure sanity against each source&apos;s registry contract.
            Runs are recorded whether or not anything failed — a clean night is a result,
            not an absence.
          </p>

          {runs === null && !error && <p className="status-loading">Reading the history…</p>}

          {runs && runs.length > 0 && (
            <>
              <div className="run-strip">
                {runs
                  .slice()
                  .reverse()
                  .map((r, i) => (
                    <span
                      key={i}
                      className={`run-cell ${r.n_error > 0 ? "run-error" : r.n_warn > 0 ? "run-warn" : "run-ok"}`}
                      title={`${r.run_date}: ${r.n_violations} violation(s)`}
                    />
                  ))}
              </div>
              <div className="run-meta">
                <span>
                  {runs.length} run{runs.length === 1 ? "" : "s"} recorded
                </span>
                {lastRun && (
                  <span>
                    last: {lastRun.run_date} · {lastRun.n_violations} violation
                    {lastRun.n_violations === 1 ? "" : "s"}
                  </span>
                )}
              </div>
              {runs.length < 5 && (
                <p className="status-note">
                  Collecting since {runs[runs.length - 1]?.run_date}. The trend needs a few
                  more nights before it says anything useful.
                </p>
              )}
            </>
          )}

          <Evidence
            sql={`SELECT run_date, n_violations, n_error, n_warn\nFROM read_parquet('${DQ_RUNS_URL ?? "…/quality/dq_runs.parquet"}')\nORDER BY run_ts DESC;`}
          />
        </section>

        {violations.length > 0 && (
          <section className="status-section">
            <div className="section-head">
              <h2>Recent violations</h2>
              <span className="rule" />
            </div>
            <div className="table-wrap">
              <table className="result">
                <thead>
                  <tr>
                    <th>date</th>
                    <th>group</th>
                    <th>source</th>
                    <th>check</th>
                    <th>severity</th>
                    <th>detail</th>
                  </tr>
                </thead>
                <tbody>
                  {violations.slice(0, 50).map((v, i) => (
                    <tr key={i}>
                      <td>{v.run_date}</td>
                      <td>{v.group}</td>
                      <td>{v.source}</td>
                      <td>{v.check}</td>
                      <td className={v.severity === "error" ? "sev-error" : "sev-warn"}>
                        {v.severity}
                      </td>
                      <td className="detail-cell">{v.detail}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {USING_SAMPLE && (
          <p className="notice">
            This build is not pointed at the public bucket, so there is no live history to
            read.
          </p>
        )}
      </main>
    </div>
  );
}
