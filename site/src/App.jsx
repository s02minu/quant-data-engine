import React, { useEffect, useState } from "react";

import Architecture from "./components/Architecture.jsx";
import Catalogue from "./components/Catalogue.jsx";
import Footer from "./components/Footer.jsx";
import Hero from "./components/Hero.jsx";
import Status from "./components/Status.jsx";
import { CATALOGUE_URL } from "./config.js";

export default function App() {
  const [catalogue, setCatalogue] = useState(null);
  // No router library: there are two pages. The Worker serves index.html for any
  // path (assets `not_found_handling: single-page-application`), so reading the
  // pathname is enough.
  const isStatus = typeof window !== "undefined" && window.location.pathname.startsWith("/status");

  useEffect(() => {
    // Try the live catalogue first; fall back to the bundled snapshot so the site
    // always renders even before the public bucket exists.
    fetch(CATALOGUE_URL)
      .then((r) => (r.ok ? r.json() : Promise.reject()))
      .catch(() => fetch("/catalogue.json").then((r) => r.json()))
      .then(setCatalogue)
      .catch(() => setCatalogue(null));
  }, []);

  if (isStatus) return <Status catalogue={catalogue} />;

  return (
    <div className="app">
      <Hero catalogue={catalogue} />
      <main>
        <Architecture />
        <Catalogue catalogue={catalogue} />
      </main>
      <Footer />
    </div>
  );
}
