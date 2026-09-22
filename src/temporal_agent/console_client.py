import asyncio
import os
import sys
from uuid import uuid4
from temporalio.client import Client, WorkflowUpdateStage
from temporal_agent import database
from temporal_agent.workflows import LiveAgentWorkflow

async def main():
    try:
        client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "192.168.1.85:7233"))
    except Exception as e:
        print(f"CRITICAL: Failed connecting to Temporal Server: {e}")
        sys.exit(1)

    session_id = os.getenv("SESSION_ID", f"console_{uuid4()}")
    SESSION_ID = session_id
    database.init_db(SESSION_ID)

    try:
        handle = client.get_workflow_handle(SESSION_ID)
        # Verify it exists
        await handle.describe()
        print(f"🔗 Connected to active agent session: '{SESSION_ID}'")
    except Exception:
        # If it doesn't exist yet, we start it
        handle = await client.start_workflow(
            LiveAgentWorkflow.run, args=[SESSION_ID], id=SESSION_ID, task_queue="live-agent-tasks"
        )
        print(f"✨ Initialized brand new agent session: '{SESSION_ID}'")

    print("Console Client Active. Type your message below. Enter 'q' to quit.\n")

    while True:
        prompt = await asyncio.get_event_loop().run_in_executor(None, input, "You: ")
        prompt = prompt.strip()

        if prompt.lower() == "q":
            break
        if not prompt:
            continue

        # Send prompt update to Temporal
        turn_id = str(uuid4())
        database.create_turn(SESSION_ID, turn_id, prompt)
        await handle.start_update(
            LiveAgentWorkflow.handle_agent_turn,
            args=[SESSION_ID, prompt, turn_id],
            wait_for_stage=WorkflowUpdateStage.ACCEPTED
        )
        print("Assistant: ", end="", flush=True)

        # Read the streaming token buffer in real-time
        next_sequence = 0
        while True:
            await asyncio.sleep(0.03)
            chunks = database.load_stream_chunks(SESSION_ID, turn_id, next_sequence)
            for sequence, chunk in chunks:
                next_sequence = sequence + 1
                print(str(chunk), end="", flush=True)

            turn = database.get_turn(SESSION_ID, turn_id)
            if turn and turn["status"] in {"completed", "failed"}:
                break

        print("\n")

if __name__ == "__main__":
    asyncio.run(main())
