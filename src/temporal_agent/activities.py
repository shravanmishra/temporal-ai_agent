import asyncio
import json
import re
from datetime import datetime
import psutil
from temporalio import activity
from temporalio.client import Client
from openai import OpenAI
from temporal_agent import database

def _get_mac_metrics() -> str:
    cpu = psutil.cpu_percent(interval=0.1)
    ram = psutil.virtual_memory().percent
    return f"Current Mac Hardware Status -> CPU Usage: {cpu}%, RAM Usage: {ram}%"

def _sync_agent_execution(session_id: str, prompt: str, workflow_id: str) -> str:
    database.init_db()
    history = database.load_messages(session_id)
    known_facts = database.load_facts(session_id)
    
    database.append_message(session_id, "user", prompt)

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
    
    async def emit_token_signal(token_fragment: str):
        c = await Client.connect("localhost:7233")
        h = c.get_workflow_handle(workflow_id)
        await h.signal("stream_chunk", token_fragment)

    full_response = ""
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
                asyncio.run(emit_token_signal(token))
                
    except Exception as e:
        full_response = f"[Agent Error: Could not generate response due to {str(e)}]"

    # === Deterministic Python Memory Extraction ===
    # If the user says "my name is X", catch it right here using regex patterns
    name_match = re.search(r"my name is\s+([a-zA-Z0-9_-]+)", prompt.lower())
    if name_match:
        extracted_name = name_match.group(1).capitalize()
        database.upsert_fact(session_id, "name", extracted_name)

    database.append_message(session_id, "assistant", full_response)
    return full_response

@activity.defn
async def execute_agent_brain(session_id: str, prompt: str, workflow_id: str) -> str:
    return await asyncio.to_thread(_sync_agent_execution, session_id, prompt, workflow_id)
