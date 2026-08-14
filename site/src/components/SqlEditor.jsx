import React, { useRef } from "react";

// A minimal SQL editor: a syntax-highlighted layer sitting exactly under a
// transparent textarea, with a line-number gutter. The textarea keeps every
// native editing behaviour (caret, selection, undo, IME); the layer below only
// paints colour. The two must share font, size, line-height and padding or the
// caret drifts away from the glyphs — that alignment is the whole trick.

const KEYWORDS = new Set(
  `select from where group by order limit offset join inner left right full outer on as and or not in is null
   distinct having union all case when then else end asc desc with over partition between like ilike exists
   create table view insert into values update delete set cast interval using natural cross return`
    .split(/\s+/)
    .filter(Boolean),
);

// One pass, ordered so earlier alternatives win: comments, strings, numbers,
// then bare words (classified after the fact), then everything else.
const TOKEN = /(--[^\n]*)|('(?:[^']|'')*')|(\b\d+(?:\.\d+)?\b)|([A-Za-z_]\w*)/g;

export function tokenize(sql) {
  const out = [];
  let last = 0;
  let m;
  TOKEN.lastIndex = 0;
  while ((m = TOKEN.exec(sql)) !== null) {
    if (m.index > last) out.push({ t: "plain", v: sql.slice(last, m.index) });
    const [raw, comment, str, num, word] = m;
    if (comment) out.push({ t: "comment", v: raw });
    else if (str) out.push({ t: "string", v: raw });
    else if (num) out.push({ t: "number", v: raw });
    else if (word) {
      if (KEYWORDS.has(word.toLowerCase())) out.push({ t: "keyword", v: raw });
      // A word immediately followed by "(" is a call — read_parquet, round, count.
      else if (sql[m.index + raw.length] === "(") out.push({ t: "fn", v: raw });
      else out.push({ t: "plain", v: raw });
    }
    last = m.index + raw.length;
  }
  if (last < sql.length) out.push({ t: "plain", v: sql.slice(last) });
  return out;
}

export default function SqlEditor({ value, onChange, onKeyDown, readOnly, label }) {
  const wrapRef = useRef(null);
  const preRef = useRef(null);
  const gutterRef = useRef(null);

  const lines = value.split("\n").length;

  // Keep the paint layer and the gutter locked to the textarea's scroll.
  function syncScroll(e) {
    const { scrollTop, scrollLeft } = e.target;
    if (preRef.current) {
      preRef.current.scrollTop = scrollTop;
      preRef.current.scrollLeft = scrollLeft;
    }
    if (gutterRef.current) gutterRef.current.scrollTop = scrollTop;
  }

  return (
    <div className="editor" ref={wrapRef}>
      <div className="editor-gutter" ref={gutterRef} aria-hidden="true">
        {Array.from({ length: lines }, (_, i) => (
          <span key={i}>{i + 1}</span>
        ))}
      </div>
      <div className="editor-code">
        <pre className="editor-paint" ref={preRef} aria-hidden="true">
          <code>
            {tokenize(value).map((tok, i) => (
              <span key={i} className={`tk-${tok.t}`}>
                {tok.v}
              </span>
            ))}
            {"\n"}
          </code>
        </pre>
        <textarea
          className="editor-input"
          spellCheck={false}
          autoCapitalize="off"
          autoCorrect="off"
          value={value}
          readOnly={readOnly}
          onChange={onChange}
          onKeyDown={onKeyDown}
          onScroll={syncScroll}
          aria-label={label}
          rows={Math.max(7, lines)}
        />
      </div>
    </div>
  );
}
