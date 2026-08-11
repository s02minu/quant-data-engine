// Small display helpers shared across components.

export function compact(n) {
  if (n == null) return "—";
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}k`;
  return String(n);
}

export function freshness(fresh) {
  if (!fresh || fresh.age_hours == null) return "—";
  const h = fresh.age_hours;
  if (h < 48) return `${Math.round(h)}h ago`;
  return `${Math.round(h / 24)}d ago`;
}
