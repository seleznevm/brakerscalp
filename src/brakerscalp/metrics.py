from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram


ALERTS_TOTAL = Counter(
    "brakerscalp_alerts_total",
    "Signals emitted by class and setup.",
    ["signal_class", "setup"],
)
ALERT_LATENCY_SECONDS = Histogram(
    "brakerscalp_alert_latency_seconds",
    "Latency between signal detection and bot delivery.",
)
STALE_DATA_RATIO = Gauge(
    "brakerscalp_stale_data_ratio",
    "Ratio of stale market data checks over the latest engine cycle.",
)
WS_RECONNECTS_TOTAL = Counter(
    "brakerscalp_ws_reconnects_total",
    "Reconnect counter placeholder per venue.",
    ["venue"],
)
VENUE_HEALTH = Gauge(
    "brakerscalp_venue_health",
    "Venue health flag per venue and symbol.",
    ["venue", "symbol"],
)
SIGNALS_IN_DB = Gauge(
    "brakerscalp_signals_in_db",
    "Latest total persisted signals.",
)

