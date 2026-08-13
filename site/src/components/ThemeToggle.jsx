import React, { useEffect, useState } from "react";

// The pre-paint script in index.html has already resolved and applied the theme;
// read it back rather than guessing, so the button never disagrees with the page.
function currentTheme() {
  if (typeof document === "undefined") return "dark";
  return document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
}

export default function ThemeToggle() {
  const [theme, setTheme] = useState(currentTheme);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    try {
      localStorage.setItem("qde-theme", theme);
    } catch {
      // Private mode / storage disabled: the toggle still works for this session.
    }
  }, [theme]);

  const next = theme === "dark" ? "light" : "dark";

  return (
    <button
      className="theme-toggle"
      onClick={() => setTheme(next)}
      aria-label={`Switch to ${next} mode`}
      title={`Switch to ${next} mode`}
    >
      <span className="theme-dot" aria-hidden="true" />
      {theme === "dark" ? "Phosphor" : "Greenbar"}
    </button>
  );
}
