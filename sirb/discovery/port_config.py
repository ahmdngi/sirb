"""Port configuration — loaded from sirb.yml."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PortDefinition:
    """A port that Sirb can scan for vessels."""

    name: str                     # "Port of Tallinn"
    slug: str                     # VesselFinder path: "EE-TLL"
    lat_min: float = 0.0
    lat_max: float = 0.0
    lon_min: float = 0.0
    lon_max: float = 0.0
    vessel_finder_url: str = ""   # full URL override


class PortConfig:
    """Loads port definitions from sirb.yml."""

    DEFAULT_PORTS: dict[str, PortDefinition] = {
        "tallinn": PortDefinition(
            name="Port of Tallinn",
            slug="EE-TLL",
            lat_min=59.4, lat_max=59.7,
            lon_min=24.5, lon_max=24.9,
            vessel_finder_url="https://www.vesselfinder.com/ports/EE-TLL-port-of-tallinn",
        ),
        "helsinki": PortDefinition(
            name="Port of Helsinki",
            slug="FI-HEL",
            lat_min=60.1, lat_max=60.2,
            lon_min=24.9, lon_max=25.0,
            vessel_finder_url="https://www.vesselfinder.com/ports/FI-HEL-port-of-helsinki",
        ),
    }

    def __init__(self, config: Optional[dict] = None):
        self._ports: dict[str, PortDefinition] = dict(self.DEFAULT_PORTS)

        if config:
            for key, val in config.items():
                if isinstance(val, dict):
                    self._ports[key] = PortDefinition(
                        name=val.get("name", key),
                        slug=val.get("slug", ""),
                        lat_min=val.get("lat_min", 0.0),
                        lat_max=val.get("lat_max", 0.0),
                        lon_min=val.get("lon_min", 0.0),
                        lon_max=val.get("lon_max", 0.0),
                        vessel_finder_url=val.get("vessel_finder_url", ""),
                    )

    def get(self, key: str) -> Optional[PortDefinition]:
        return self._ports.get(key)

    def all(self) -> dict[str, PortDefinition]:
        return dict(self._ports)

    def keys(self) -> list[str]:
        return list(self._ports.keys())
