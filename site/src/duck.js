// DuckDB-WASM helper — a full SQL engine running in the visitor's browser.
//
// This is the literal embodiment of "serve files, not queries": the WASM engine
// loads from the jsDelivr CDN, fetches Parquet over HTTP (range requests, so only the
// bytes a query touches move), and does the compute on the client. No backend.

import * as duckdb from "@duckdb/duckdb-wasm";

let dbPromise = null;

async function initDb() {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles); // picks the right wasm for this browser
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })
  );
  const worker = new Worker(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);
  return db;
}

// Lazily initialise once; every query reuses the same instance.
export function getDb() {
  if (!dbPromise) dbPromise = initDb();
  return dbPromise;
}

export async function runQuery(sql) {
  const db = await getDb();
  const conn = await db.connect();
  try {
    const table = await conn.query(sql);
    const columns = table.schema.fields.map((f) => f.name);
    const rows = table.toArray().map((row) => {
      const obj = row.toJSON();
      for (const key of Object.keys(obj)) {
        const v = obj[key];
        if (typeof v === "bigint") obj[key] = Number(v); // int64 -> number for rendering
      }
      return obj;
    });
    return { columns, rows };
  } finally {
    await conn.close();
  }
}
