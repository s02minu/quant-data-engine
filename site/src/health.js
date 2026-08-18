// One definition of "late", shared by every surface that reports health.
//
// This lives on its own because it was briefly duplicated: the home page and the
// status page each graded freshness with their own thresholds and disagreed about
// the same source. A status page that contradicts the page linking to it is worse
// than no status page, so the rule has exactly one home.

// FALLBACK cadence per group, in hours, used only when the catalogue does not carry
// a source's real threshold. The real one is `expected_within_hours`, computed by
// qde.checks from each series' own observed spacing and published per source — the
// same number the nightly enforces.
//
// These constants used to be the only rule here, and they were a second, competing
// definition of "late": graded at series=72h, CFTC's weekly COT release looked
// overdue every single week, so the status page reported "2 of 14 sources behind
// schedule" on a night the pipeline recorded zero violations. Its real budget is
// 504h. A status page that contradicts the pipeline it reports on is worse than no
// status page — the more so on a site whose own copy promises "one definition, many
// consumers".
const CADENCE_HOURS = {
  bars: 24,
  series: 72,
  events: 168,
};

const DEFAULT_CADENCE = 24;

export function cadenceFor(group) {
  return CADENCE_HOURS[group] ?? DEFAULT_CADENCE;
}

/** The sources a visitor can actually check for themselves.
 *
 * A withheld source (yfinance is code-only, not redistributable) still appears in the
 * catalogue — that is the platform disclosing what it ingests. But its rows and
 * freshness are read from the private lake, so there is no public file to verify them
 * against, and the status page opens by promising the opposite.
 *
 * Exported from here, next to the grading rule, for the reason stated at the top of
 * this file: the home page and the status page must never disagree about the same
 * source. Filtering on one surface alone would recreate exactly that.
 */
export function verifiableSources(sources = []) {
  return sources.filter((s) => s?.redistributable !== false);
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
  // The published threshold already includes the pipeline's tolerance factor, so it
  // is compared directly; only the group fallback needs the 1.5x/3x grading.
  const published = source?.expected_within_hours;
  if (typeof published === "number" && published > 0) {
    if (hours <= published) return "ok";
    return hours <= published * 2 ? "warn" : "late";
  }
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
