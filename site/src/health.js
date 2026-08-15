// One definition of "late", shared by every surface that reports health.
//
// This lives on its own because it was briefly duplicated: the home page and the
// status page each graded freshness with their own thresholds and disagreed about
// the same source. A status page that contradicts the page linking to it is worse
// than no status page, so the rule has exactly one home.

// Expected cadence per group, in hours. Deliberately generous — an unattended
// health signal must bias to silence, and a source that is genuinely broken keeps
// aging and trips the threshold within a period or two anyway. This mirrors the
// reasoning behind the pipeline's own _STALE_FACTOR.
const CADENCE_HOURS = {
  bars: 24,
  series: 72,
  events: 168,
};

const DEFAULT_CADENCE = 24;

export function cadenceFor(group) {
  return CADENCE_HOURS[group] ?? DEFAULT_CADENCE;
}

/** Milliseconds since a source's newest observation, or null if unknown. */
export function ageMs(source, now = Date.now()) {
  const last = source?.freshness?.last ? Date.parse(source.freshness.last) : null;
  if (!last || Number.isNaN(last)) return null;
  return now - last;
}

/** "ok" | "warn" | "late" | "unknown" — graded against the source's own cadence. */
export function healthOf(source, now = Date.now()) {
  const age = ageMs(source, now);
  if (age == null) return "unknown";
  const hours = age / 3.6e6;
  const cadence = cadenceFor(source.group);
  if (hours <= cadence * 1.5) return "ok";
  if (hours <= cadence * 3) return "warn";
  return "late";
}

/** Counts per state across a list of sources. */
export function healthSummary(sources, now = Date.now()) {
  const counts = { ok: 0, warn: 0, late: 0, unknown: 0 };
  for (const s of sources) counts[healthOf(s, now)] += 1;
  return { ...counts, total: sources.length, degraded: counts.warn + counts.late };
}

export function humanAge(ms) {
  if (!Number.isFinite(ms) || ms < 0) return "—";
  const s = Math.floor(ms / 1000);
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d) return `${d}d ${h}h`;
  if (h) return `${h}h ${m}m`;
  return `${m}m ${s % 60}s`;
}
