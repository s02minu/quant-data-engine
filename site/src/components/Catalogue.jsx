import React, { useState } from "react";

import { compact, freshness } from "../format.js";

function CopyButton({ text }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      className="copy"
      onClick={() => {
        navigator.clipboard?.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1200);
      }}
    >
      {copied ? "copied ✓" : "copy query"}
    </button>
  );
}

function DatasetCard({ ds }) {
  return (
    <div className="dataset">
      <div className="dataset-head">
        <span className={`layer layer-${ds.layer}`}>{ds.layer}</span>
        <span className="dataset-id">{ds.id}</span>
        <span className="dataset-rows">{compact(ds.row_count)} rows</span>
      </div>
      <div className="dataset-meta">
        <span>{ds.schema?.length ?? 0} columns</span>
        {ds.freshness && <span>· updated {freshness(ds.freshness)}</span>}
      </div>
      <pre className="dataset-query">
        <code>{ds.sample_query}</code>
      </pre>
      <CopyButton text={ds.sample_query} />
    </div>
  );
}

export default function Catalogue({ catalogue }) {
  if (!catalogue) {
    return (
      <section className="section" id="catalogue">
        <h2>Catalogue</h2>
        <p className="section-lede">Loading the catalogue…</p>
      </section>
    );
  }

  const sources = catalogue.sources ?? [];
  const datasets = catalogue.datasets ?? [];

  return (
    <section className="section" id="catalogue">
      <h2>Catalogue</h2>
      <p className="section-lede">
        Generated from the registry plus live lake stats. Each dataset comes with a schema,
        freshness, licence, and a copyable DuckDB query. Excluded from the public lake:{" "}
        {(catalogue.notes?.excluded_sources ?? []).join(", ") || "none"} (code-only, not
        redistributable).
      </p>

      <div className="datasets">
        {datasets.map((ds) => (
          <DatasetCard key={ds.id} ds={ds} />
        ))}
      </div>

      <h3 className="sources-title">Sources</h3>
      <div className="table-wrap">
        <table className="sources">
          <thead>
            <tr>
              <th>Source</th>
              <th>Group</th>
              <th>Rows</th>
              <th>Freshness</th>
              <th>Redistributable</th>
            </tr>
          </thead>
          <tbody>
            {sources.map((s) => (
              <tr key={s.name}>
                <td className="mono">{s.name}</td>
                <td>{s.group}</td>
                <td>{compact(s.rows)}</td>
                <td>{freshness(s.freshness)}</td>
                <td>
                  <span className={s.redistributable ? "pill yes" : "pill no"}>
                    {s.redistributable ? "public" : "code-only"}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
