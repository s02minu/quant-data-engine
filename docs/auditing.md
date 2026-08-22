# Handing the platform to an external auditor

A second reviewer — Codex, another model, a person — finds a different class of bug
than the author does. The 2026-08-20 pass is the case study: an external read-only
audit found two publishing-gate holes that had been written into the code *by the
same session that designed the gate*, because the author "knew" it was covered. In
the other direction, the defects found by running against live data that week (16
days of partial bars, phantom verification peers, a catalogue overstating rows by
29%) were all invisible to static review.

Both are worth having. This note is how to get the second one safely.

## The actual risk

It is **not** the filesystem. The one irreversible action in this platform is
**publishing to the public R2 bucket** — bytes served to strangers cannot be recalled,
which is why the publishing gate is an allowlist rather than a denylist.

Everything else is recoverable: a wrong edit is a diff, a broken local lake is a
re-backfill, a bad branch is a delete.

So isolation has two layers, and only the second one really matters.

## Layer 1 — the worktree (cheap, mechanical)

```bash
bash scripts/audit-worktree.sh
```

Creates a throwaway branch (`audit/<timestamp>`) in a sibling directory. The auditor
works there; you review with `git -C <path> diff` and delete the directory when done.

Two deliberate details:

- **It refuses to run on a dirty tree.** A worktree branches from a *commit*, so
  uncommitted work is invisible to it — an auditor would review the last committed
  state and silently report on code that no longer matches your disk.
- **The worktree is a sibling, not a subdirectory.** This project globs recursively
  (the lake, dbt, the bronze consolidation). A worktree nested inside the repo gets
  swept into those globs as if it were data.

Tear down with `bash scripts/audit-worktree.sh --remove <path>`.

## Layer 2 — credentials (this is the one that matters)

| Give | Withhold |
|---|---|
| `secrets/r2-read.env` (read-only) | `secrets-infra/r2.env` — **write credentials** |
| `secrets/fred.env` (read-only API key) | VPS SSH key |
| The public HTTPS base URL | `QDE_R2_PUBLIC_BUCKET` |

There is a structural safeguard worth knowing about: the read credentials are named
`QDE_R2_READ_KEY_ID` / `QDE_R2_READ_SECRET`, while `qde.publish_public` authenticates
with `QDE_R2_ACCESS_KEY_ID` / `QDE_R2_SECRET_ACCESS_KEY`. **Different variable names**,
so an auditor holding only the read file cannot publish even by accident — the publish
path finds no credentials and fails closed rather than silently doing nothing.

`scripts/audit-worktree.sh --with-secrets` copies only the read-only pair. Write
credentials live in `secrets-infra/`, which the script never reads and which is not
mounted into any container.

## Auditing the site

Entirely safe with no special access: `npm ci && npm run build` is local, and the
deployed site is a public URL. The one thing to remember is that **the site
auto-deploys from `git push`** — so an auditor must not push, which the worktree
branch already discourages (it is a throwaway ref that should never be merged).

## What an auditor cannot reach, and why that is the point

The public lake is designed so a stranger can verify it with **no credentials at all** —
that is the whole "serve files, not queries" model. So an auditor needs nothing more
than a stranger does:

```sql
SELECT * FROM read_parquet('<public-base-url>/gold/group=bars/mart=fct_bars_daily/data.parquet') LIMIT 100;
```

If a finding requires private access to demonstrate, that is itself worth knowing — it
means the public surface cannot be independently checked, which is a product problem
before it is a security one.

## Running a generated draft safely

`qde.draft` proves a candidate ingestor by **running** it, and importing a Python
module executes its top-level code with the calling process's filesystem, network and
environment. Quarantining a draft in `drafts/` keeps it out of the *pipeline*; it does
nothing to contain a hostile one.

That matters most for the case the tool exists for: a draft written from API
documentation fetched off the internet is exactly the shape of thing that carries a
prompt injection. In this repository an unchecked draft could read every
`secrets/*.env` file and post them somewhere.

Three defences, and it is worth being clear that only the third is containment:

1. **Containment is the default, not a flag.** `run_gauntlet` runs the candidate in a
   throwaway container unless the caller explicitly asks for `isolation="in-process"`.
   An earlier version gated execution behind a `--trust-this-draft` flag instead; that
   was the wrong shape, because it put a human judgement call back in the loop the
   gauntlet exists to remove, and a tired yes is not a security control. If docker is
   unavailable the gauntlet **fails** rather than quietly falling back — degrading to
   in-process at the moment containment is missing is the worst possible time for it.
2. **Scoped credentials.** Candidate code runs holding only the one credential its
   source needs — a Tiingo draft cannot read FRED's key or the R2 read keys.
3. **The AST screen**, on the host, before a container is even started.

```bash
python -m qde.draft verify drafts/x.py --name src --symbol SYM --from 2024-01-01
```

## Drafting one with the agent

`qde.author` is the other half: it reads a source's API documentation, fills in the
three methods `scaffold` leaves unimplemented, runs the result through the gauntlet,
and feeds the failures back for another attempt. Bounded at three rounds.

```bash
pip install -e ".[agent]"
python -m qde.author --doc https://example.com/api-docs --name acme --group bars     --symbol SPY=SPY --from 2024-01-01
```

It needs `ANTHROPIC_API_KEY` (or `secrets/anthropic.env`), and it is local/dev only —
`anthropic` is an optional dependency and is deliberately absent from the VPS image and
from the container that executes candidates.

Two things the agent is structurally not allowed to decide. `redistributable` is absent
from its output schema, so no draft can set it — whether a licence permits republishing
is a legal judgement, and a confident paragraph in a doc page does not settle it. The
measured thresholds (`null_tolerance`, `expected_daily_rows`, freshness SLA) are not
asked for either: documentation states a rate limit, it does not know what healthy looks
like in this lake.

Nothing is registered and nothing is committed. A passing draft sits in `drafts/` for a
person to read — the gauntlet has passed ingestors carrying real defects before (raw
prices where adjusted were needed; a silently dropped row), so "it went green" is a
reason to review it, not a reason to skip reviewing it.

There is also an AST screen that refuses drafts importing `socket`/`subprocess` or
calling `eval`/`os.system`. It runs *before* execution and costs nothing, but it is
**not a security boundary** — a string concatenation walks past it. It exists so the
careless draft fails loudly instead of running.

## Reading the report

Verify each finding against live data before acting on it. Of the four findings in the
2026-08-20 external audit: two were real and fixed, one was a deliberate documented
trade-off (the nightly exits 0 so a DQ violation cannot block compaction and sync —
the durable record is `dq_runs`, which is what a monitor should read), and one was
correct but under-ranked (unpinned dependencies on a 24/7 collector).

An audit is evidence, not a verdict — the same standard this platform applies to its
own data.
