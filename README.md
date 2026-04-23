# BrakerScalp

`BrakerScalp` is a private Telegram alert bot for crypto breakout and bounce setups on USDT perpetual markets. The repository is structured as a small VPS-friendly multi-service application with:

- `collector` for exchange ingestion and hot state updates
- `engine` for level detection, scoring, alert deduplication and outbox
- `bot` for Telegram polling and command handling
- `api` for health, metrics and debug endpoints
- `redis` and `postgres` as state backends

## Quick start

1. Create a virtual environment and install the package:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
```

2. Copy the environment file and fill in `BOT_TOKEN`, `ALLOWED_CHAT_IDS` and infrastructure credentials:

```powershell
Copy-Item .env.example .env
```

3. Start infrastructure:

```powershell
docker compose up -d postgres redis
```

4. Run services in separate terminals:

```powershell
python -m brakerscalp.app.collector
python -m brakerscalp.app.engine
python -m brakerscalp.app.bot
python -m brakerscalp.app.api
```

5. Or run the full stack:

```powershell
docker compose up --build
```

## Configuration

- Universe defaults are stored in `config/universe.json`.
- The bot is private by default and enforces `ALLOWED_CHAT_IDS`.
- `ALERT_CHAT_IDS` can be narrower or broader than `ALLOWED_CHAT_IDS`; if omitted it falls back to `ALLOWED_CHAT_IDS`.
- `ALERT_MESSAGE_THREAD_ID` can be set for Telegram forum groups/topics; for example `475` sends alerts into topic/thread `475` inside the configured group chat.
- Use `/chatinfo` inside the target group/topic to verify the actual `chat_id` and `message_thread_id` seen by the bot.
- The first release is polling-based and does not expose a Telegram webhook.
- Postgres credentials in `.env` are reused by `docker-compose.yml`, so the VPS deploy only needs one source of truth.
- Exchange integrations are public-market-data only in v1, so no exchange API keys are required yet.

## Main services

- `collector` polls public market data from Binance, Bybit and OKX and normalizes it.
- `collector` gathers full candles/book/trades/derivatives from each symbol's `primary_venue`, while secondary venues stay in lightweight health-check mode so the bot can cover the whole universe instead of stalling on one market.
- `engine` computes levels from 4h/1h candles, validates 15m/5m triggers and enqueues alerts.
- `bot` consumes the persistent outbox and serves `/start`, `/status`, `/last`, `/mute`, `/unmute`, `/health`.
- `bot` also supports `/config`, `/help`, `/pending`, `/chatinfo` and `/testalert` for VPS verification and delivery recovery checks.
- `api` exposes `/health/live`, `/health/ready`, `/metrics`, `/debug/candidates`, `/debug/alerts/latest`, `/debug/deliveries/latest`, `/debug/deliveries/counts`, `/debug/venues/health`, `/debug/runtime-config`.

## Recommended VPS .env

At minimum, set these before deploy:

```env
BOT_TOKEN=...
ALLOWED_CHAT_IDS=123456789
ALERT_CHAT_IDS=123456789
POSTGRES_DB=brakerscalp
POSTGRES_USER=braker
POSTGRES_PASSWORD=strong-password
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
DATABASE_URL=postgresql+asyncpg://braker:strong-password@postgres:5432/brakerscalp
REDIS_URL=redis://redis:6379/0
ENABLE_BINANCE=true
ENABLE_BYBIT=true
ENABLE_OKX=true
```

`DATABASE_URL` and `REDIS_URL` can be left empty if you want them assembled automatically from the `POSTGRES_*` and `REDIS_*` variables.

## Testing

```powershell
pytest
```

## Notes

- Live adapters are implemented against public exchange market-data endpoints and a common contract.
- The storage schema is bootstrapped automatically via SQLAlchemy metadata on startup.
- Alert deliveries are persisted in the database and re-queued on bot restart if they were still `queued`, `requeued` or `failed`.
- Repeated alerts for the same setup are throttled by a duplicate window so the bot does not spam one breakout every engine cycle.
- The codebase leaves a clean seam for a later calibrated ranker without blocking the rule-based v1.
