from datetime import timedelta
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent import activities

@workflow.defn
class LiveAgentWorkflow:
    def __init__(self) -> None:
        self._exit_requested = False
        self._token_queue = []
        self._is_processing = False

    @workflow.run
    async def run(self, session_id: str) -> None:
        while not self._exit_requested:
            await workflow.wait_condition(lambda: self._exit_requested)

    # An Update handler handles receiving an action and returning live state metrics
    @workflow.update
    async def handle_agent_turn(self, session_id: str, prompt: str) -> str:
        self._is_processing = True
        self._token_queue.clear()
        
        # Trigger the LLM activity execution task
        result = await workflow.execute_activity(
            activities.execute_agent_brain,
            args=[session_id, prompt, workflow.info().workflow_id],
            start_to_close_timeout=timedelta(seconds=90),
        )
        
        self._is_processing = False
        return result

    @workflow.signal
    def stream_chunk(self, chunk: str) -> None:
        """The activity directly pipes tokens here while thinking"""
        self._token_queue.append(chunk)

    @workflow.query
    def fetch_stream_buffer(self) -> list:
        tokens = list(self._token_queue)
        self._token_queue.clear()
        return tokens

    @workflow.query
    def is_thinking(self) -> bool:
        return self._is_processing

    @workflow.signal
    def shutdown_agent(self) -> None:
        self._exit_requested = True
