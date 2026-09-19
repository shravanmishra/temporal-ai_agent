import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from temporalio.client import Client, WorkflowUpdateStage
from temporalio.worker import Worker

from temporal_agent import activities
from temporal_agent.workflows import LiveAgentWorkflow

# Lifespan manager to cleanly boot up and close the background worker
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Connect to Temporal Server
    try:
        client = await Client.connect("localhost:7233")
    except Exception as e:
        print(f"CRITICAL: Failed connecting worker instance to Temporal Server: {e}")
        yield
        return

    # 2. Build the Worker engine
    worker = Worker(
        client,
        task_queue="live-agent-tasks",
        workflows=[LiveAgentWorkflow],
        activities=[activities.execute_agent_brain],
    )

    # 3. Start worker as a background task
    worker_task = asyncio.create_task(worker.run())
    print("🤖 Background Temporal Worker Engine Active and Listening.")

    yield  # FastAPI runs here until application shutdown

    # 4. Cleanup on stop
    print("Shutting down worker process...")
    await worker.shutdown()
    await worker_task

# Inject lifespan into FastAPI initialization
app = FastAPI(title="Temporal AI Agent Backend API", version="1.0.0", lifespan=lifespan)

# Enable Cross-Origin Resource Sharing (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    session_id: str
    prompt: str

async def get_temporal_client() -> Client:
    try:
        return await Client.connect("localhost:7233")
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Could not connect to the Temporal Server cluster on port 7233: {str(e)}"
        )

@app.post("/api/chat/stream")
async def stream_chat_turn(request: ChatRequest):
    client = await get_temporal_client()

    try:
        handle = await client.start_workflow(
            LiveAgentWorkflow.run,
            args=[request.session_id],
            id=request.session_id,
            task_queue="live-agent-tasks"
        )
    except Exception:
        handle = client.get_workflow_handle(request.session_id)

    try:
        await handle.start_update(
            LiveAgentWorkflow.handle_agent_turn,
            args=[request.session_id, request.prompt],
            wait_for_stage=WorkflowUpdateStage.ACCEPTED
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to submit agent update step: {str(e)}")

    # === FIXED CHUNK FORMATTING FOR SSE CLIENTS ===
    async def token_generator():
        while True:
            await asyncio.sleep(0.03)
            chunks = await handle.query(LiveAgentWorkflow.fetch_stream_buffer)
            for chunk in chunks:
                if chunk:
                    # Strip raw lines and wrap securely so it doesn't break the HTML text stream parser
                    yield f"data: {chunk}\n\n"

            is_thinking = await handle.query(LiveAgentWorkflow.is_thinking)
            if not is_thinking:
                final_chunks = await handle.query(LiveAgentWorkflow.fetch_stream_buffer)
                for chunk in final_chunks:
                    if chunk:
                        yield f"data: {chunk}\n\n"
                break

        yield "data: [DONE]\n\n"

    return StreamingResponse(token_generator(), media_type="text/event-stream")
