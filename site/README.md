# quant-data-engine — showcase site

A React (Vite) landing page for the platform, with a **live DuckDB-WASM console**: a
full SQL engine compiled to WebAssembly that queries the Parquet lake straight from R2,
in the visitor's browser, with no backend. It reads the static `catalogue.json` the
pipeline publishes.

It works **before the public bucket exists** — the console and catalogue fall back to a
bundled sample (`public/sample/fct_bars_daily.parquet`, real BTC/ETH/SOL data) and
`public/catalogue.json`.

## Run locally

```bash
cd site
npm install
npm run dev
```

Open the printed URL (default <http://localhost:5173>). Click a preset, hit **Run
query** — it executes in your browser.

## Build

```bash
npm run build      # -> dist/
npm run preview    # serve the production build locally
```

## Configuration (build-time env)

| Var | What | Default |
|---|---|---|
| `VITE_PUBLIC_BASE_URL` | HTTPS origin of the public R2 bucket (e.g. `https://data.yourdomain` or an `r2.dev` URL). When set, the console + catalogue query the **live** lake instead of the bundled sample. | unset → bundled sample |
| `VITE_STREAMLIT_URL` | URL of the Streamlit dashboard (adds a footer link). | unset |

Set them in `site/.env.local` (gitignored) or in the Cloudflare Pages build settings.

## Deploy to Cloudflare Pages (free)

1. Push the repo (already done).
2. Cloudflare dashboard → **Workers & Pages → Create → Pages → Connect to Git** → pick
   this repo.
3. Build settings:
   - **Root directory:** `site`
   - **Build command:** `npm run build`
   - **Output directory:** `dist`
4. (Optional) add the env vars above under **Settings → Environment variables**.
5. Deploy. You get a free `*.pages.dev` URL; add a custom domain later if you want.

## Going live against the real lake

Once the public R2 bucket is provisioned and `qde.publish_public` has run:

1. Set `VITE_PUBLIC_BASE_URL` to the bucket's public origin and redeploy.
2. **Enable CORS on the bucket** so the browser can fetch it — in the R2 bucket's
   settings, add a CORS policy allowing `GET` from your Pages origin (and `*` for a
   public demo). Without CORS, in-browser queries against the live bucket are blocked
   (the bundled sample still works, since it is same-origin).
