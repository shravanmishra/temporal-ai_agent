from datetime import timedelta
from temporalio.common import RetryPolicy
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent import activities

@workflow.defn
class LiveAgentWorkflow:
    def __init__(self) -> None:
        self._exit_requested = False
        self._is_processing = False

    @workflow.run
    async def run(self, session_id: str) -> None:
        while not self._exit_requested:
            await workflow.wait_condition(lambda: self._exit_requested)

    @workflow.update
    async def handle_agent_turn(self, session_id: str, prompt: str, turn_id: str | None = None) -> str:
        self._is_processing = True
        try:
            if turn_id is None:
                # Keep histories created before turn IDs were introduced replayable.
                return await workflow.execute_activity(
                    activities.execute_agent_brain,
                    args=[session_id, prompt, workflow.info().workflow_id],
                    start_to_close_timeout=timedelta(seconds=90),
                )
            return await workflow.execute_activity(
                activities.execute_agent_brain,
                args=[session_id, prompt, turn_id],
                start_to_close_timeout=timedelta(seconds=90),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        finally:
            self._is_processing = False

    @handle_agent_turn.validator
    def validate_agent_turn(self, session_id: str, prompt: str, turn_id: str | None = None) -> None:
        if self._is_processing:
            raise RuntimeError("This session already has an active turn")

    @workflow.signal
    def stream_chunk(self, chunk: str) -> None:
        """Accept legacy token signals while replaying older session histories."""

    @workflow.query
    def is_thinking(self) -> bool:
        return self._is_processing

    @workflow.signal
    def shutdown_agent(self) -> None:
        self._exit_requested = True
