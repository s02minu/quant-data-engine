import React, { useState } from "react";

import { tokenize } from "./SqlEditor.jsx";
import { compact, freshness } from "../format.js";

// The catalogue ships each query on one very long line. Break the major clauses
// onto their own lines so it reads like SQL you'd actually write.
function formatSql(sql) {
  return String(sql ?? "")
    .replace(/\s+\bFROM\b\s+/gi, "\nFROM ")
    .replace(/\s+\bWHERE\b\s+/gi, "\nWHERE ")
    .replace(/\s+\bORDER\s+BY\b\s+/gi, "\nORDER BY ")
    .replace(/\s+\bGROUP\s+BY\b\s+/gi, "\nGROUP BY ")
    .replace(/\s+\bLIMIT\b\s+/gi, "\nLIMIT ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function HighlightedSql({ sql }) {
  return (
    <pre className="dataset-query">
      <code>
        {tokenize(sql).map((t, i) => (
          <span key={i} className={`tk-${t.t}`}>
            {t.v}
          </span>
        ))}
      </code>
    </pre>
  );
}

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
          {/* The streamed archive reports no counts: it is mirrored bucket-to-bucket
              and never lands on the machine that builds this, so measuring it would
              mean scanning the whole thing over HTTP. "— rows · 0 columns" would read
              as a broken dataset rather than an unmeasured one. */}
          {ds.row_count == null && !ds.schema
            ? "full archive · not measured"
            : `${compact(ds.row_count)} rows · ${ds.schema?.length ?? 0} columns`}
          {ds.freshness && ` · updated ${freshness(ds.freshness)}`}
        </div>
      </div>
      <div>
        {/* Not every dataset can advertise a runnable query. The microstructure
            archive has no consolidated file and plain HTTP cannot expand a glob, so
            it carries access notes instead — an empty code block with a dead copy
            button would be worse than saying why there isn't one. */}
        {ds.sample_query ? (
          <>
            <HighlightedSql sql={formatSql(ds.sample_query)} />
            <CopyButton text={formatSql(ds.sample_query)} />
          </>
        ) : (
          <p className="dataset-notes">{ds.notes}</p>
        )}
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
