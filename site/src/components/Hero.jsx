import React from "react";

import QueryConsole from "./QueryConsole.jsx";
import ThemeToggle from "./ThemeToggle.jsx";
import { GITHUB_URL } from "../config.js";
import { compact } from "../format.js";
import { useActiveSection, useCountUp, useScrollProgress } from "../hooks.js";

const SECTIONS = [
  { id: "console", label: "Live query" },
  { id: "architecture", label: "Architecture" },
  { id: "catalogue", label: "Catalogue" },
];

// Module-level so the observer effect doesn't re-subscribe on every render.
const SECTION_IDS = SECTIONS.map((s) => s.id);

function Stat({ label, value, format = (n) => String(Math.round(n)) }) {
  const shown = useCountUp(value ?? null);
  return (
    <div className="stat">
      <span className="stat-value fig">{value ? format(shown) : "—"}</span>
      <span className="stat-label">{label}</span>
    </div>
  );
}

export default function Hero({ catalogue }) {
  const sources = catalogue?.sources ?? [];
  const datasets = catalogue?.datasets ?? [];
  const totalRows = sources.reduce((a, s) => a + (s.rows || 0), 0);
  const redistributable = sources.filter((s) => s.redistributable).length;
  const progress = useScrollProgress();
  const active = useActiveSection(SECTION_IDS);

  return (
    <header className="masthead">
      <a className="skip-link" href="#console">
        Skip to the query console
      </a>
      <nav className="nav">
        <div className="nav-inner">
          <a className="brand" href="#top">
            <span className="brand-rule" />
            quant-data-engine
          </a>
          <div className="nav-right">
            <div className="nav-links">
              {SECTIONS.map((s) => (
                <a
                  key={s.id}
                  href={`#${s.id}`}
                  className={active === s.id ? "active" : undefined}
                  aria-current={active === s.id ? "true" : undefined}
                >
                  {s.label}
                </a>
              ))}
              <a href={GITHUB_URL} target="_blank" rel="noreferrer">
                GitHub
              </a>
            </div>
            <ThemeToggle />
          </div>
        </div>
        <div
          className="nav-progress"
          style={{ width: `${progress * 100}%` }}
          aria-hidden="true"
        />
      </nav>

      <div className="hero-inner" id="top">
        <div className="hero-glow" aria-hidden="true" />

        <div className="hero-head">
          <span className="eyebrow">Serve files, not queries · zero egress</span>
          <h1>
            Open market data you
            <br />
            query yourself
          </h1>
          <p className="lede">
            Crypto OHLCV, tick microstructure, the volatility complex, rates, positioning and a
            bitemporal economic calendar — published as Parquet on Cloudflare R2. No signup, no
            server, no egress fees.
          </p>
          <a className="btn primary pill" href="#console">
            Run a query now
          </a>
        </div>

        <QueryConsole />

        {sources.length > 0 && (
          <div className="trust-row">
            <span className="trust-label">Ingested from</span>
            <div className="trust-names">
              {[...new Set(sources.map((s) => s.name))].map((n) => (
                <span className="trust-name" key={n}>
                  {n}
                </span>
              ))}
            </div>
          </div>
        )}

        <div className="stat-strip">
          <Stat label="Sources" value={sources.length || null} />
          <Stat label="Datasets" value={datasets.length || null} />
          <Stat label="Rows" value={totalRows || null} format={compact} />
          <Stat label="Redistributable" value={redistributable || null} />
        </div>
      </div>
    </header>
  );
}
