"""The NTA GTFS-Realtime feeds we collect, and the call schedule the usage policy allows.

The NTA developer usage policy (read 15 November 2026, recorded in ``docs/02-nta-usage-policy.md``)
restricts each token to calling the GTFS Real Time API once every 60 seconds. We read that as one
call per minute across *all* endpoints, not per endpoint, because it is the reading that cannot
break the policy. So the feeds share one call per minute (ADR-0005):

* TripUpdates and Vehicles alternate, so each is refreshed every two minutes.
* Alerts take one slot in fifteen.

The schedule is a pure function of the UTC minute, so a stateless Lambda fired every minute and
the long-running local poller pick the same feed for the same minute.
"""

from __future__ import annotations

from enum import StrEnum

from ghostbus_core.topics import ALERTS, TRIP_UPDATES, VEHICLE_POSITIONS, Topic

API_BASE = "https://api.nationaltransport.ie/gtfsr/v2/"
STATIC_GTFS_URL = "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"

#: Minimum spacing between two calls with one token, from the usage policy.
MIN_CALL_SPACING_S = 60.0
#: Every ``ALERTS_EVERY``-th minute slot goes to the alerts feed.
ALERTS_EVERY = 15


class Feed(StrEnum):
    TRIP_UPDATES = "trip_updates"
    VEHICLES = "vehicles"
    ALERTS = "alerts"

    @property
    def path(self) -> str:
        return _PATHS[self]

    @property
    def url(self) -> str:
        return API_BASE + self.path

    @property
    def topic(self) -> Topic:
        return _TOPICS[self]


_PATHS = {Feed.TRIP_UPDATES: "TripUpdates", Feed.VEHICLES: "Vehicles", Feed.ALERTS: "alerts"}
_TOPICS = {Feed.TRIP_UPDATES: TRIP_UPDATES, Feed.VEHICLES: VEHICLE_POSITIONS, Feed.ALERTS: ALERTS}


def minute_slot(epoch_s: float) -> int:
    """The UTC minute number since the epoch. One call is allowed per slot."""
    return int(epoch_s // 60)


def feed_for_slot(slot: int) -> Feed:
    """Which feed to call in a given minute.

    Slots ``14, 29, 44, ...`` (one in fifteen) are alerts. The other slots alternate strictly
    between TripUpdates and Vehicles, counting only non-alert slots, so an alert slot delays both
    by one minute instead of costing one of them a whole turn. Each is called at least once in
    any three consecutive minutes.
    """
    if slot % ALERTS_EVERY == ALERTS_EVERY - 1:
        return Feed.ALERTS
    non_alert_index = slot - (slot + 1) // ALERTS_EVERY
    return Feed.TRIP_UPDATES if non_alert_index % 2 == 0 else Feed.VEHICLES
