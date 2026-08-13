import React from "react";

import { useReveal } from "../hooks.js";

const STAGES = [
  { k: "generation", t: "Sources", d: "20+ heterogeneous APIs: exchanges, FRED/ALFRED, CBOE, CFTC — unified by one registry (the 'little book')." },
  { k: "ingestion", t: "Ingest", d: "One BaseIngestor pattern: retries, pagination, watermarks, idempotent upserts. A new source is one registry row." },
  { k: "bronze", t: "Bronze", d: "Raw-as-received Parquet on R2, partitioned by group / source / date. Never modified — the replay log." },
  { k: "silver", t: "Silver", d: "dbt views: cleaned, typed, deduplicated, schema-enforced. One row per candle / observation / release." },
  { k: "gold", t: "Gold", d: "dbt marts: returns, ATR, realized vol, cross-venue basis, and the bitemporal revision history." },
  { k: "serve", t: "Serve", d: "Files, not queries. You point your own DuckDB at the public lake and pay your own (tiny) compute." },
];

const PRINCIPLES = [
  {
    t: "Group by shape, not asset class",
    d: "bars · series · events · microstructure. VIX isn't a special case — it's one row pointing at the series schema. New instrument types stop being new modules.",
  },
  {
    t: "One definition, many consumers",
    d: "Each source is declared once. The same SourceSpec drives the ingestor config, the data-quality thresholds, and this public catalogue. Nothing can drift.",
  },
  {
    t: "Bitemporality kills lookahead bias",
    d: "The economic calendar stores what was known and when. GDP for a 2020 quarter was revised 13× over five years — backtesting on today's number is silent lookahead.",
  },
  {
    t: "~$0 to run",
    d: "R2 has zero egress fees, so serving files pushes compute to the client. Marginal cost per user approaches zero — the whole thing runs on free tiers.",
  },
];

export default function Architecture() {
  // The pipeline reveals in order, so the sequence reads as a sequence.
  const [pipelineRef, pipelineShown] = useReveal(0.2);
  const [principlesRef, principlesShown] = useReveal(0.15);

  return (
    <section className="section" id="architecture">
      <div className="section-head">
        <h2>How it works</h2>
        <span className="rule" />
        <span className="label">Six stages</span>
      </div>
      <p className="section-lede">
        A full data lifecycle — generation, ingestion, storage, transformation, serving — with
        data quality, orchestration, and observability running throughout.
      </p>

      <div className="pipeline" ref={pipelineRef}>
        {STAGES.map((s, i) => (
          <div
            className={`stage stage-${s.k} reveal${pipelineShown ? " is-visible" : ""}`}
            style={{ transitionDelay: `${i * 70}ms` }}
            key={s.k}
          >
            <div className="stage-seq">{String(i + 1).padStart(2, "0")}</div>
            <div className="stage-title">{s.t}</div>
            <div className="stage-desc">{s.d}</div>
          </div>
        ))}
      </div>

      <div className="serve-callout">
        <div className="stamp">The key decision</div>
        <span className="label">Cost model</span>
        <p>
          <strong>Serve files, not queries.</strong> A query API pays compute per request — one
          runaway scan can generate a real bill, and cost scales with users. Instead the lake is
          published as Parquet on R2's zero-egress storage; DuckDB fetches only the columns and
          row-groups a query touches, and the visitor's machine does the work. Marginal cost per
          user ≈ 0.
        </p>
      </div>

      <div className="principles" ref={principlesRef}>
        {PRINCIPLES.map((p, i) => (
          <div
            className={`principle reveal${principlesShown ? " is-visible" : ""}`}
            style={{ transitionDelay: `${i * 60}ms` }}
            key={p.t}
          >
            <h3>{p.t}</h3>
            <p>{p.d}</p>
          </div>
        ))}
      </div>
    </section>
  );
}
