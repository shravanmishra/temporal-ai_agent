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

`TEMPORAL_ADDRESS` defaults to `localhost:7233`.

`TEMPORAL_TASK_QUEUE` defaults to `live-agent-tasks`.

`CORS_ORIGINS` defaults to `http://localhost:8000` and accepts a comma-separated list.

SQLite is used for local development. For multiple API or worker instances, replace the persistence adapter with a shared database or stream store such as Postgres or Redis.
