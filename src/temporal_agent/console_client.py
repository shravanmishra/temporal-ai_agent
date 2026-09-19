import asyncio
import sys
from temporalio.client import Client, WorkflowUpdateStage
from temporal_agent.workflows import LiveAgentWorkflow

async def main():
    try:
        client = await Client.connect("localhost:7233")
    except Exception as e:
        print(f"CRITICAL: Failed connecting to Temporal Server: {e}")
        sys.exit(1)

    # Use the EXACT same session ID as your web UI to share memory history!
    SESSION_ID = "web_ui_session_002"

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
        await handle.start_update(
            LiveAgentWorkflow.handle_agent_turn,
            args=[SESSION_ID, prompt],
            wait_for_stage=WorkflowUpdateStage.ACCEPTED
        )
        print("Assistant: ", end="", flush=True)

        # Read the streaming token buffer in real-time
        while True:
            await asyncio.sleep(0.03)
            chunks = await handle.query(LiveAgentWorkflow.fetch_stream_buffer)
            for chunk in chunks:
                print(str(chunk), end="", flush=True)

            is_thinking = await handle.query(LiveAgentWorkflow.is_thinking)
            if not is_thinking:
                final_chunks = await handle.query(LiveAgentWorkflow.fetch_stream_buffer)
                for chunk in final_chunks:
                    print(str(chunk), end="", flush=True)
                break

        print("\n")

if __name__ == "__main__":
    asyncio.run(main())
