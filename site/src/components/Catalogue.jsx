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
      <div>
        <div className="dataset-head">
          <span className={`layer layer-${ds.layer}`}>{ds.layer}</span>
          <span className="dataset-id">{ds.id}</span>
        </div>
        <div className="dataset-meta">
          {compact(ds.row_count)} rows · {ds.schema?.length ?? 0} columns
          {ds.freshness && ` · updated ${freshness(ds.freshness)}`}
        </div>
      </div>
      <div>
        <pre className="dataset-query">
          <code>{ds.sample_query}</code>
        </pre>
        <CopyButton text={ds.sample_query} />
      </div>
    </div>
  );
}

export default function Catalogue({ catalogue }) {
  if (!catalogue) {
    return (
      <section className="section" id="catalogue">
        <div className="section-head">
          <h2>Catalogue</h2>
          <span className="rule" />
        </div>
        <p className="section-lede">Loading the catalogue…</p>
      </section>
    );
  }

  const sources = catalogue.sources ?? [];
  const datasets = catalogue.datasets ?? [];
  // The catalogue snapshot ships with a placeholder origin until the public bucket
  // is live. Say so plainly rather than handing out SQL that can't run.
  const pendingBucket = /REPLACE-ME/.test(catalogue.public_base_url ?? "");

  return (
    <section className="section" id="catalogue">
      <div className="section-head">
        <h2>Catalogue</h2>
        <span className="rule" />
        <span className="label">
          {datasets.length} datasets · {sources.length} sources
        </span>
      </div>
      <p className="section-lede">
        Generated from the registry plus live lake stats. Each dataset comes with a schema,
        freshness, licence, and a copyable DuckDB query. Excluded from the public lake:{" "}
        {(catalogue.notes?.excluded_sources ?? []).join(", ") || "none"} (code-only, not
        redistributable).
      </p>

      {pendingBucket && (
        <p className="notice">
          <strong>The public bucket isn&apos;t live yet.</strong> These queries show the shape
          you&apos;ll use, but their host is a placeholder — swap in the bucket origin to run
          them. The console above works today: it reads a bundled sample.
        </p>
      )}

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
