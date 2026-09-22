import asyncio
import os
from uuid import uuid4
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from starlette.responses import JSONResponse
from pydantic import BaseModel
from temporalio.client import Client, WorkflowUpdateStage
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporal_agent.workflows import LiveAgentWorkflow
from temporal_agent import database

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.temporal_client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "192.168.1.85:7233"))
    yield

# Inject lifespan into FastAPI initialization
app = FastAPI(title="Temporal AI Agent Backend API", version="1.0.0", lifespan=lifespan)

@app.exception_handler(Exception)
async def handle_unexpected_error(request: Request, exc: Exception):
    response = JSONResponse(status_code=500, content={"detail": str(exc) or "Internal server error"})
    origin = request.headers.get("origin")
    if origin and (origin == "null" or origin.startswith(("http://localhost:", "http://127.0.0.1:"))):
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Vary"] = "Origin"
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip()
        for origin in os.getenv(
            "CORS_ORIGINS",
            "http://127.0.0.1:5500,http://localhost:5500,http://127.0.0.1:8000,http://localhost:8000,null",
        ).split(",")
        if origin.strip()
    ],
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    session_id: str
    prompt: str
    turn_id: str | None = None

@app.post("/api/chat/stream")
async def stream_chat_turn(request: ChatRequest):
    client: Client = app.state.temporal_client
    turn_id = request.turn_id or str(uuid4())
    try:
        turn = database.create_turn(request.session_id, turn_id, request.prompt)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    handle = client.get_workflow_handle(request.session_id)
    update_result_task = None
    if turn["status"] == "pending":
        try:
            await client.start_workflow(
                LiveAgentWorkflow.run,
                args=[request.session_id],
                id=request.session_id,
                task_queue="live-agent-tasks",
            )
        except WorkflowAlreadyStartedError:
            pass

        try:
            update_handle = await handle.start_update(
                LiveAgentWorkflow.handle_agent_turn,
                args=[request.session_id, request.prompt, turn_id],
                wait_for_stage=WorkflowUpdateStage.ACCEPTED,
            )
            update_result_task = asyncio.create_task(update_handle.result())
        except Exception as exc:
            database.fail_turn(request.session_id, turn_id, str(exc))
            detail = str(exc)
            if "already has an active turn" in detail:
                raise HTTPException(status_code=409, detail="This session already has an active turn") from exc
            raise HTTPException(status_code=503, detail=f"Failed to submit agent update step: {detail}") from exc

    # === FIXED CHUNK FORMATTING FOR SSE CLIENTS ===
    async def token_generator():
        next_sequence = 0
        while True:
            await asyncio.sleep(0.03)
            chunks = database.load_stream_chunks(request.session_id, turn_id, next_sequence)
            for sequence, chunk in chunks:
                next_sequence = sequence + 1
                for line in chunk.splitlines() or [""]:
                    yield f"data: {line}\n"
                yield "\n"

            current_turn = database.get_turn(request.session_id, turn_id)
            if current_turn and current_turn["status"] in {"completed", "failed"}:
                if current_turn["status"] == "failed":
                    yield f"event: error\ndata: {current_turn['error'] or 'Agent failed'}\n\n"
                break

            # SQLite is local to each process. If the worker runs on another
            # host, use the durable Temporal update result instead of waiting
            # forever for chunks that were written to the worker's database.
            if update_result_task and update_result_task.done():
                try:
                    response = update_result_task.result()
                    if next_sequence == 0 and response:
                        for line in response.splitlines() or [""]:
                            yield f"data: {line}\n"
                        yield "\n"
                    break
                except Exception as exc:
                    failed_turn = database.get_turn(request.session_id, turn_id)
                    detail = (failed_turn or {}).get("error") or str(exc)
                    yield f"event: error\ndata: {detail}\n\n"
                    break

        yield "data: [DONE]\n\n"

    return StreamingResponse(
        token_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
