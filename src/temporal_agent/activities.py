import asyncio
import os
import re
import time
from datetime import datetime
import psutil
from temporalio import activity
from openai import OpenAI
from temporal_agent import database

def _get_mac_metrics() -> str:
    cpu = psutil.cpu_percent(interval=0.1)
    ram = psutil.virtual_memory().percent
    return f"Current Mac Hardware Status -> CPU Usage: {cpu}%, RAM Usage: {ram}%"

def _get_current_time() -> str:
    return datetime.now().astimezone().strftime("Current time: %Y-%m-%d %H:%M:%S %Z")

def _get_current_date() -> str:
    return datetime.now().astimezone().strftime("Today's date: %Y-%m-%d")

def _sync_agent_execution(session_id: str, prompt: str, turn_id: str) -> str:
    database.init_db(session_id)
    turn = database.get_turn(session_id, turn_id)
    if turn is None:
        database.create_turn(session_id, turn_id, prompt)
        turn = database.get_turn(session_id, turn_id)
    if turn and turn["status"] == "completed":
        return turn["response"] or ""

    database.mark_turn_running(session_id, turn_id)
    history = database.load_messages(session_id)
    known_facts = database.load_facts(session_id)
    
    database.append_message(session_id, "user", prompt, turn_id)

    facts_str = ", ".join([f"{k}: {v}" for k, v in known_facts.items()]) if known_facts else "None yet"
    
    system_instructions = (
        "You are a helpful, extremely concise personal assistant running locally on a Mac M1.\n"
        f"Information about the user: {facts_str}\n\n"
        "Answer the user's questions clearly and directly based on this context. "
        "Do not invent current time or system metrics; the application supplies those values. "
        "Do not mention memory tags."
    )

    messages = [{"role": "system", "content": system_instructions}]
    for msg in history:
        messages.append({"role": msg["role"], "content": msg["content"]})
        
    normalized_prompt = prompt.lower()
    if re.search(r"\b(current time|what time is it|what(?:'s| is) the time|time now|time)\b", normalized_prompt):
        response = _get_current_time()
        database.append_stream_chunk(session_id, turn_id, 0, response)
        database.append_message(session_id, "assistant", response, turn_id)
        database.complete_turn(session_id, turn_id, response)
        return response

    if re.search(r"\b(today(?:'s)? date|current date|what date is it|what(?:'s| is) today's date|date today|date)\b", normalized_prompt):
        response = _get_current_date()
        database.append_stream_chunk(session_id, turn_id, 0, response)
        database.append_message(session_id, "assistant", response, turn_id)
        database.complete_turn(session_id, turn_id, response)
        return response

    if "mac stat" in normalized_prompt or "cpu usage" in normalized_prompt:
        response = _get_mac_metrics()
        database.append_stream_chunk(session_id, turn_id, 0, response)
        database.append_message(session_id, "assistant", response, turn_id)
        database.complete_turn(session_id, turn_id, response)
        return response
    else:
        messages.append({"role": "user", "content": prompt})

    client = OpenAI(
        base_url=f"{os.getenv('OLLAMA_BASE_URL', 'http://192.168.1.85:11434').rstrip('/')}/v1",
        api_key="ollama",
    )
    
    full_response = ""
    sequence = 0
    pending_chunks = []
    last_flush = time.monotonic()
    try:
        stream = client.chat.completions.create(
            model="phi4-mini",
            messages=messages,
            max_tokens=300,
            stream=True
        )
        
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                token = chunk.choices[0].delta.content
                full_response += token
                pending_chunks.append((sequence, token))
                should_flush = (
                    sequence == 0
                    or len(pending_chunks) >= 16
                    or time.monotonic() - last_flush >= 0.1
                )
                if should_flush:
                    database.append_stream_chunks(session_id, turn_id, pending_chunks)
                    pending_chunks.clear()
                    last_flush = time.monotonic()
                sequence += 1

        database.append_stream_chunks(session_id, turn_id, pending_chunks)
    except Exception as exc:
        database.fail_turn(session_id, turn_id, str(exc))
        raise

    # === Deterministic Python Memory Extraction ===
    # If the user says "my name is X", catch it right here using regex patterns
    name_match = re.search(r"my name is\s+([a-zA-Z0-9_-]+)", prompt.lower())
    if name_match:
        extracted_name = name_match.group(1).capitalize()
        database.upsert_fact(session_id, "name", extracted_name)

    database.append_message(session_id, "assistant", full_response, turn_id)
    database.complete_turn(session_id, turn_id, full_response)
    return full_response

@activity.defn
async def execute_agent_brain(session_id: str, prompt: str, turn_id: str) -> str:
    return await asyncio.to_thread(_sync_agent_execution, session_id, prompt, turn_id)
