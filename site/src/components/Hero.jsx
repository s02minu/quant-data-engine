import React from "react";

import { GITHUB_URL } from "../config.js";
import { compact } from "../format.js";

function Stat({ value, label }) {
  return (
    <div className="stat">
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  );
}

export default function Hero({ catalogue }) {
  const sources = catalogue?.sources ?? [];
  const datasets = catalogue?.datasets ?? [];
  const totalRows = sources.reduce((a, s) => a + (s.rows || 0), 0);
  const redistributable = sources.filter((s) => s.redistributable).length;

  return (
    <header className="hero">
      <nav className="nav">
        <a className="brand" href="#top">
          <span className="brand-dot" />
          quant-data-engine
        </a>
        <div className="nav-links">
          <a href="#architecture">Architecture</a>
          <a href="#console">Live query</a>
          <a href="#catalogue">Catalogue</a>
          <a href={GITHUB_URL} target="_blank" rel="noreferrer">
            GitHub ↗
          </a>
        </div>
      </nav>

      <div className="hero-inner" id="top">
        <div className="badge">serve files, not queries · zero egress</div>
        <h1>
          An open financial data lakehouse
          <br />
          <span className="accent">you query yourself.</span>
        </h1>
        <p className="lede">
          Clean, validated market &amp; macro data — crypto OHLCV, tick microstructure,
          the volatility complex, rates, positioning, and a bitemporal economic calendar —
          published as Parquet on Cloudflare R2. Point your own DuckDB at it: no signup,
          no server, no egress fees.
        </p>

        <div className="stats">
          <Stat value={sources.length || "—"} label="sources" />
          <Stat value={datasets.length || "—"} label="datasets" />
          <Stat value={totalRows ? compact(totalRows) : "—"} label="rows" />
          <Stat value={redistributable || "—"} label="redistributable" />
        </div>

        <div className="cta">
          <a className="btn primary" href="#console">
            Run SQL in your browser →
          </a>
          <a className="btn ghost" href="#catalogue">
            Browse the catalogue
          </a>
        </div>
      </div>
    </header>
  );
}
