import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_agent import activities
from temporal_agent.workflows import LiveAgentWorkflow


async def run_worker() -> None:
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))
    worker = Worker(
        client,
        task_queue=os.getenv("TEMPORAL_TASK_QUEUE", "live-agent-tasks"),
        workflows=[LiveAgentWorkflow],
        activities=[activities.execute_agent_brain],
    )
    await worker.run()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()