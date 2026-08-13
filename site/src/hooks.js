// Small motion primitives. Every one of them degrades to "final state, instantly"
// when the visitor asks for reduced motion.

import { useEffect, useRef, useState } from "react";

function prefersReducedMotion() {
  return (
    typeof window !== "undefined" &&
    window.matchMedia?.("(prefers-reduced-motion: reduce)").matches
  );
}

// Tallies a figure up to its target, the way a ledger totals a column.
export function useCountUp(target, duration = 1100) {
  const [value, setValue] = useState(0);

  useEffect(() => {
    if (target == null || !Number.isFinite(target)) return undefined;
    if (prefersReducedMotion()) {
      setValue(target);
      return undefined;
    }

    let raf;
    const start = performance.now();
    const tick = (now) => {
      const p = Math.min(1, (now - start) / duration);
      // easeOutCubic: fast settle, no bounce — this is an accountant, not a toy.
      setValue(target * (1 - Math.pow(1 - p, 3)));
      if (p < 1) raf = requestAnimationFrame(tick);
      else setValue(target);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [target, duration]);

  return value;
}

// Adds a class once the element first enters the viewport, then stops observing.
export function useReveal(threshold = 0.15) {
  const ref = useRef(null);
  const [shown, setShown] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el || shown) return undefined;
    if (prefersReducedMotion() || typeof IntersectionObserver === "undefined") {
      setShown(true);
      return undefined;
    }

    const io = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setShown(true);
          io.disconnect();
        }
      },
      { threshold },
    );
    io.observe(el);
    return () => io.disconnect();
  }, [shown, threshold]);

  return [ref, shown];
}

// Types text out character by character, then reports done. Used once, on load,
// so the page demonstrates the product instead of describing it.
export function useTypewriter(text, { enabled = true, cps = 90 } = {}) {
  const [shown, setShown] = useState(enabled ? "" : text);
  const [done, setDone] = useState(!enabled);

  useEffect(() => {
    if (!enabled) {
      setShown(text);
      setDone(true);
      return undefined;
    }
    if (prefersReducedMotion()) {
      setShown(text);
      setDone(true);
      return undefined;
    }

    let raf;
    const start = performance.now();
    const tick = (now) => {
      const n = Math.floor(((now - start) / 1000) * cps);
      if (n >= text.length) {
        setShown(text);
        setDone(true);
        return;
      }
      setShown(text.slice(0, n));
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [text, enabled, cps]);

  return [shown, done];
}

// Which section the reader is currently in, so the nav can say where you are
// and not just how far you've come.
export function useActiveSection(ids) {
  const [active, setActive] = useState(null);

  useEffect(() => {
    const els = ids.map((id) => document.getElementById(id)).filter(Boolean);
    if (!els.length || typeof IntersectionObserver === "undefined") return undefined;

    const seen = new Map();
    const io = new IntersectionObserver(
      (entries) => {
        entries.forEach((e) => seen.set(e.target.id, e.intersectionRatio));
        // Most-visible section wins; ties fall to document order.
        let best = null;
        let bestRatio = 0;
        els.forEach((el) => {
          const r = seen.get(el.id) ?? 0;
          if (r > bestRatio) {
            bestRatio = r;
            best = el.id;
          }
        });
        setActive(bestRatio > 0 ? best : null);
      },
      { threshold: [0, 0.15, 0.35, 0.6, 1], rootMargin: "-70px 0px -35% 0px" },
    );
    els.forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, [ids]);

  return active;
}

// 0→1 scroll position, for the rule that fills across the masthead.
export function useScrollProgress() {
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    let raf = null;
    const measure = () => {
      const el = document.documentElement;
      const max = el.scrollHeight - el.clientHeight;
      setProgress(max > 0 ? Math.min(1, el.scrollTop / max) : 0);
      raf = null;
    };
    const onScroll = () => {
      if (raf == null) raf = requestAnimationFrame(measure);
    };
    measure();
    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", onScroll, { passive: true });
    return () => {
      if (raf != null) cancelAnimationFrame(raf);
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onScroll);
    };
  }, []);

  return progress;
}
