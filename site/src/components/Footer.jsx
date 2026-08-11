import React from "react";

import { GITHUB_URL, STREAMLIT_URL } from "../config.js";

export default function Footer() {
  return (
    <footer className="footer">
      <div className="footer-inner">
        <div>
          <div className="brand">
            <span className="brand-dot" />
            quant-data-engine
          </div>
          <p className="footer-note">
            An open financial data lakehouse. Exchange-native + U.S.-government data,
            redistributable and free. Licensed sources ship as open ingestors, not data.
          </p>
        </div>
        <div className="footer-links">
          <a href={GITHUB_URL} target="_blank" rel="noreferrer">
            GitHub ↗
          </a>
          {STREAMLIT_URL && (
            <a href={STREAMLIT_URL} target="_blank" rel="noreferrer">
              Dashboard ↗
            </a>
          )}
          <a href={`${GITHUB_URL}/blob/main/docs/licensing.md`} target="_blank" rel="noreferrer">
            Licensing ↗
          </a>
          <a href={`${GITHUB_URL}/blob/main/docs/ROADMAP.md`} target="_blank" rel="noreferrer">
            Architecture ↗
          </a>
        </div>
      </div>
      <div className="footer-bottom">
        Built with DuckDB, dbt, Dagster, and Cloudflare R2 · serve files, not queries
      </div>
    </footer>
  );
}
