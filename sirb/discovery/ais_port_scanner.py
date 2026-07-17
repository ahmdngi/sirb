"""Port scanner — discovers vessels in a port via AIS aggregator pages.

Uses Playwright (or CloakBrowser) to scrape VesselFinder port pages,
extracting vessel names, MMSIs, and positions from the vessel table.

Falls back to static port configs if browser is unavailable.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Optional

from .port_config import PortDefinition


class PortScanner:
    """Scan a port's vessel list from VesselFinder."""

    # Regex patterns for VesselFinder port page
    _MMSI_PATTERN = re.compile(r"/vessels/details/(\d{7,9})")
    _VESSEL_ROW_PATTERN = re.compile(
        r"vessel-name[^>]*>([^<]+)"
    )

    def __init__(self, port: PortDefinition):
        self._port = port
        self._browser = None
        self._page = None

    async def scan(self) -> list[dict]:
        """Scan vessels currently in this port.

        Returns a list of dicts::
            [{"mmsi": "273342890", "name": "YAZ", "type": "...", "destination": "..."}]
        """
        # Try browser-based scan first
        vessels = await self._scan_vesselfinder()
        if vessels:
            return vessels

        # Fallback: return empty and log
        print(f"[discovery] No vessels found for {self._port.name} "
              f"(browser may be unavailable)")
        return []

    async def _scan_vesselfinder(self) -> list[dict]:
        """Scrape VesselFinder port page for vessel list."""
        url = self._port.vessel_finder_url
        if not url:
            return []

        browser = None
        page = None
        use_cloak = False
        p = None  # playwright instance

        try:
            import cloakbrowser
            use_cloak = True
        except ImportError:
            try:
                from playwright.sync_api import sync_playwright
                use_cloak = False
            except ImportError:
                return []

        try:
            if use_cloak:
                from cloakbrowser import launch
                browser = launch(headless=True, args=["--no-sandbox"])
                page = browser.new_page()
            else:
                from playwright.sync_api import sync_playwright
                p = sync_playwright().start()
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox"],
                )
                page = browser.new_page()

            page.goto(url, wait_until="networkidle", timeout=30000)
            time.sleep(3)

            # Extract vessel table data
            raw = page.evaluate("""(() => {
                const vessels = [];
                const rows = document.querySelectorAll('tr');

                for (const row of rows) {
                    const cells = row.querySelectorAll('td');
                    if (cells.length < 3) continue;

                    const vessel = {};

                    // MMSI from links
                    const links = row.querySelectorAll('a');
                    for (const link of links) {
                        const m = link.href.match(/\\/vessels\\/details\\/(\\d{7,9})/);
                        if (m) vessel.mmsi = m[1];
                        if (link.classList.contains('vessel-name')) {
                            vessel.name = link.innerText.trim();
                        }
                    }

                    // Type, destination, etc from cells
                    cells.forEach((cell, i) => {
                        const text = cell.innerText.trim();
                        if (i === 1 && !vessel.name) vessel.name = text;
                        if (i === 2) vessel.type = text;
                        if (i === cells.length - 1) vessel.destination = text;
                    });

                    if (vessel.mmsi || vessel.name) {
                        vessels.push(vessel);
                    }
                }
                return vessels;
            })()""")

            browser.close()
            if p:
                p.stop()

            # Dedup by MMSI
            seen: set[str] = set()
            unique = []
            for v in raw:
                mmsi = v.get("mmsi", "")
                if mmsi and mmsi not in seen:
                    seen.add(mmsi)
                    unique.append(v)

            return unique

        except Exception as e:
            print(f"[discovery] VesselFinder scan failed for "
                  f"{self._port.name}: {e}")
            if browser:
                try:
                    browser.close()
                except Exception:
                    pass
            return []
