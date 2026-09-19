import asyncio
import os
from uuid import uuid4
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from temporalio.client import Client, WorkflowUpdateStage
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporal_agent.workflows import LiveAgentWorkflow
from temporal_agent import database

@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db()
    app.state.temporal_client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))
    yield
    await app.state.temporal_client.close()

# Inject lifespan into FastAPI initialization
app = FastAPI(title="Temporal AI Agent Backend API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in os.getenv("CORS_ORIGINS", "null").split(",") if origin.strip()],
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
        turn = database.create_turn(turn_id, request.session_id, request.prompt)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    handle = client.get_workflow_handle(request.session_id)
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
            await handle.start_update(
                LiveAgentWorkflow.handle_agent_turn,
                args=[request.session_id, request.prompt, turn_id],
                wait_for_stage=WorkflowUpdateStage.ACCEPTED,
            )
        except Exception as exc:
            database.fail_turn(turn_id, str(exc))
            detail = str(exc)
            if "already has an active turn" in detail:
                raise HTTPException(status_code=409, detail="This session already has an active turn") from exc
            raise HTTPException(status_code=503, detail=f"Failed to submit agent update step: {detail}") from exc

    # === FIXED CHUNK FORMATTING FOR SSE CLIENTS ===
    async def token_generator():
        next_sequence = 0
        while True:
            await asyncio.sleep(0.03)
            chunks = database.load_stream_chunks(turn_id, next_sequence)
            for sequence, chunk in chunks:
                next_sequence = sequence + 1
                for line in chunk.splitlines() or [""]:
                    yield f"data: {line}\n"
                yield "\n"

            current_turn = database.get_turn(turn_id)
            if current_turn and current_turn["status"] in {"completed", "failed"}:
                if current_turn["status"] == "failed":
                    yield f"event: error\ndata: {current_turn['error'] or 'Agent failed'}\n\n"
                break

        yield "data: [DONE]\n\n"

    return StreamingResponse(
        token_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
