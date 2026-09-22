# Temporal Agent

The API and Temporal worker run as separate processes. The API creates session workflows and streams turn chunks from the shared persistence layer. The worker executes the LLM activity.

## Run locally

Start Temporal Server and Ollama first, then run these in separate terminals:

```bash
uv run temporal-agent-worker
uv run temporal-agent
```

The API listens on `http://localhost:8000`.

## Configuration

`TEMPORAL_ADDRESS` defaults to `192.168.1.85:7233`, the Temporal SDK/worker gRPC endpoint.

The Temporal Web UI is available at `http://192.168.1.85:8233/namespaces/default/workflows`; do not use that URL as `TEMPORAL_ADDRESS`.

`TEMPORAL_TASK_QUEUE` defaults to `live-agent-tasks`.

`CORS_ORIGINS` defaults to `http://localhost:8000` and accepts a comma-separated list.

`OLLAMA_BASE_URL` defaults to `http://192.168.1.85:11434` and can be set when the worker uses a different Ollama server.

The Web UI creates and remembers a unique UUID session in browser local storage. The console client creates a unique session per launch; set `SESSION_ID` to reconnect to a previous console session.

Local development uses one SQLite file per session under `~/.temporal-agent/sessions/`. Set `TEMPORAL_DATA_DIR` to override this location. This isolates sessions but is still host-local; for multiple API or worker instances, replace the persistence adapter with a shared database or stream store such as Postgres or Redis.
