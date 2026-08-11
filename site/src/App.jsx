import React, { useEffect, useState } from "react";

import Architecture from "./components/Architecture.jsx";
import Catalogue from "./components/Catalogue.jsx";
import Footer from "./components/Footer.jsx";
import Hero from "./components/Hero.jsx";
import QueryConsole from "./components/QueryConsole.jsx";
import { CATALOGUE_URL } from "./config.js";

export default function App() {
  const [catalogue, setCatalogue] = useState(null);

  useEffect(() => {
    // Try the live catalogue first; fall back to the bundled snapshot so the site
    // always renders even before the public bucket exists.
    fetch(CATALOGUE_URL)
      .then((r) => (r.ok ? r.json() : Promise.reject()))
      .catch(() => fetch("/catalogue.json").then((r) => r.json()))
      .then(setCatalogue)
      .catch(() => setCatalogue(null));
  }, []);

  return (
    <div className="app">
      <Hero catalogue={catalogue} />
      <main>
        <Architecture />
        <QueryConsole />
        <Catalogue catalogue={catalogue} />
      </main>
      <Footer />
    </div>
  );
}
