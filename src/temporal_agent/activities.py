import asyncio
import re
from datetime import datetime
import psutil
from temporalio import activity
from openai import OpenAI
from temporal_agent import database

def _get_mac_metrics() -> str:
    cpu = psutil.cpu_percent(interval=0.1)
    ram = psutil.virtual_memory().percent
    return f"Current Mac Hardware Status -> CPU Usage: {cpu}%, RAM Usage: {ram}%"

def _sync_agent_execution(session_id: str, prompt: str, turn_id: str) -> str:
    database.init_db()
    turn = database.get_turn(turn_id)
    if turn is None:
        database.create_turn(turn_id, session_id, prompt)
        turn = database.get_turn(turn_id)
    if turn and turn["status"] == "completed":
        return turn["response"] or ""

    database.mark_turn_running(turn_id)
    history = database.load_messages(session_id)
    known_facts = database.load_facts(session_id)
    
    database.append_message(session_id, "user", prompt, turn_id)

    facts_str = ", ".join([f"{k}: {v}" for k, v in known_facts.items()]) if known_facts else "None yet"
    
    # Clean system prompt with zero tracking bracket layout noise
    system_instructions = (
        "You are a helpful, extremely concise personal assistant running locally on a Mac M1.\n"
        f"The current local system time is: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Information about the user: {facts_str}\n\n"
        "Answer the user's questions clearly and directly based on this context. Do not mention memory tags."
    )

    messages = [{"role": "system", "content": system_instructions}]
    for msg in history:
        messages.append({"role": msg["role"], "content": msg["content"]})
        
    if "mac stat" in prompt.lower() or "cpu usage" in prompt.lower():
        tool_output = _get_mac_metrics()
        messages.append({"role": "user", "content": prompt})
        messages.append({"role": "assistant", "content": f"Let me check that. {tool_output}"})
        messages.append({"role": "user", "content": "Summarize that hardware check for me."})
    else:
        messages.append({"role": "user", "content": prompt})

    client = OpenAI(base_url="http://localhost:11434/v1", api_key="ollama")
    
    full_response = ""
    sequence = 0
    try:
        stream = client.chat.completions.create(
            model="llama3.2:1b",
            messages=messages,
            max_tokens=300,
            stream=True
        )
        
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                token = chunk.choices[0].delta.content
                full_response += token
                database.append_stream_chunk(turn_id, sequence, token)
                sequence += 1
                
    except Exception as exc:
        database.fail_turn(turn_id, str(exc))
        raise

    # === Deterministic Python Memory Extraction ===
    # If the user says "my name is X", catch it right here using regex patterns
    name_match = re.search(r"my name is\s+([a-zA-Z0-9_-]+)", prompt.lower())
    if name_match:
        extracted_name = name_match.group(1).capitalize()
        database.upsert_fact(session_id, "name", extracted_name)

    database.append_message(session_id, "assistant", full_response, turn_id)
    database.complete_turn(turn_id, full_response)
    return full_response

@activity.defn
async def execute_agent_brain(session_id: str, prompt: str, turn_id: str) -> str:
    return await asyncio.to_thread(_sync_agent_execution, session_id, prompt, turn_id)
