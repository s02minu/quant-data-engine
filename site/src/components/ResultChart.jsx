import React, { useMemo, useState } from "react";

// An area chart of whatever the query just returned. Single series, so there is no
// legend — the caption names it. The line draws in left-to-right on each new result,
// which is the query visibly delivering rather than decoration.

const W = 1000; // viewBox units; the SVG scales to its container
const H = 220;
const PAD = { t: 16, r: 16, b: 22, l: 52 };

// Prefer a price-ish column, else the first numeric one that isn't the x axis.
const PREFERRED = ["close", "price", "value", "basis_bps", "n", "count"];

function pickColumns(result) {
  if (!result?.rows?.length) return null;
  const { columns, rows, temporal } = result;
  const xCol = columns.find((c) => temporal?.has(c));
  if (!xCol) return null;

  const numeric = columns.filter(
    (c) => c !== xCol && rows.every((r) => r[c] == null || typeof r[c] === "number"),
  );
  if (!numeric.length) return null;
  const yCol = PREFERRED.find((p) => numeric.includes(p)) ?? numeric[0];
  return { xCol, yCol };
}

function fmt(n) {
  const a = Math.abs(n);
  if (a >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (a >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (a >= 1e3) return (n / 1e3).toFixed(1) + "k";
  if (a >= 1) return n.toFixed(2);
  return n.toPrecision(3);
}

export default function ResultChart({ result }) {
  const [hover, setHover] = useState(null);

  const model = useMemo(() => {
    const cols = pickColumns(result);
    if (!cols) return null;
    const { xCol, yCol } = cols;

    // Queries are usually ORDER BY date DESC; a time axis must read chronologically.
    const pts = result.rows
      .filter((r) => typeof r[yCol] === "number" && r[xCol] != null)
      .map((r) => ({ x: Number(r[xCol]), y: r[yCol] }))
      .sort((a, b) => a.x - b.x);
    if (pts.length < 2) return null;

    const xs = pts.map((p) => p.x);
    const ys = pts.map((p) => p.y);
    const x0 = Math.min(...xs);
    const x1 = Math.max(...xs);
    let y0 = Math.min(...ys);
    let y1 = Math.max(...ys);
    if (y0 === y1) {
      y0 -= 1;
      y1 += 1;
    }
    // Breathing room so the line never touches the frame.
    const padY = (y1 - y0) * 0.12;
    y0 -= padY;
    y1 += padY;

    const sx = (x) => PAD.l + ((x - x0) / (x1 - x0 || 1)) * (W - PAD.l - PAD.r);
    const sy = (y) => PAD.t + (1 - (y - y0) / (y1 - y0)) * (H - PAD.t - PAD.b);

    const scaled = pts.map((p) => ({ ...p, px: sx(p.x), py: sy(p.y) }));
    const line = scaled.map((p, i) => `${i ? "L" : "M"}${p.px.toFixed(1)} ${p.py.toFixed(1)}`).join(" ");
    const area = `${line} L${scaled.at(-1).px.toFixed(1)} ${H - PAD.b} L${scaled[0].px.toFixed(1)} ${H - PAD.b} Z`;

    // Three gridlines is enough context without becoming a table.
    const ticks = [y0 + (y1 - y0) * 0.5, y1 - padY, y0 + padY].map((v) => ({ v, py: sy(v) }));

    return { xCol, yCol, scaled, line, area, ticks };
  }, [result]);

  if (!model) return null;

  const { yCol, scaled, line, area, ticks } = model;
  const first = scaled[0];
  const last = scaled.at(-1);
  const delta = last.y - first.y;
  const pct = first.y !== 0 ? (delta / Math.abs(first.y)) * 100 : 0;

  function onMove(e) {
    const svg = e.currentTarget;
    const box = svg.getBoundingClientRect();
    const px = ((e.clientX - box.left) / box.width) * W;
    let best = scaled[0];
    for (const p of scaled) {
      if (Math.abs(p.px - px) < Math.abs(best.px - px)) best = p;
    }
    setHover(best);
  }

  return (
    <figure className="chart">
      <figcaption className="chart-cap">
        <span className="chart-title">{yCol}</span>
        <span className={`chart-delta ${delta >= 0 ? "up" : "down"}`}>
          {delta >= 0 ? "▲" : "▼"} {fmt(Math.abs(delta))} ({pct.toFixed(2)}%)
        </span>
        <span className="chart-range">{scaled.length} points</span>
      </figcaption>

      <svg
        className="chart-svg"
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={`${yCol} over time, ${scaled.length} points, change ${pct.toFixed(2)} percent`}
        onMouseMove={onMove}
        onMouseLeave={() => setHover(null)}
      >
        <defs>
          <linearGradient id="areaFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--chart)" stopOpacity="0.28" />
            <stop offset="100%" stopColor="var(--chart)" stopOpacity="0" />
          </linearGradient>
          <clipPath id="drawIn">
            <rect className="chart-wipe" x="0" y="0" width={W} height={H} />
          </clipPath>
        </defs>

        {ticks.map((t, i) => (
          <g key={i}>
            <line className="chart-grid" x1={PAD.l} x2={W - PAD.r} y1={t.py} y2={t.py} />
            <text className="chart-tick" x={PAD.l - 8} y={t.py + 3.5} textAnchor="end">
              {fmt(t.v)}
            </text>
          </g>
        ))}

        <g clipPath="url(#drawIn)">
          <path className="chart-area" d={area} fill="url(#areaFill)" />
          <path className="chart-line" d={line} />
        </g>

        {hover && (
          <g className="chart-hover">
            <line className="chart-cross" x1={hover.px} x2={hover.px} y1={PAD.t} y2={H - PAD.b} />
            <circle cx={hover.px} cy={hover.py} r="4.5" />
          </g>
        )}
      </svg>

      {hover && (
        <div
          className="chart-tip"
          style={{ left: `${(hover.px / W) * 100}%` }}
          role="status"
        >
          <span className="chart-tip-v">{fmt(hover.y)}</span>
          <span className="chart-tip-x">{new Date(hover.x).toISOString().slice(0, 10)}</span>
        </div>
      )}
    </figure>
  );
}
