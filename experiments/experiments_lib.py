#!/usr/bin/env python3
import os
import asyncio

# Agents SDK
from agents import Agent, Runner
from agents.mcp import MCPServerStreamableHttp
from pathlib import Path

from collections import deque

context_window = None
base_instructions = "You are assisting an actuary performing data analysis on life insurance data."

def set_context_window_size(size=10):
    global context_window
    context_window = deque(maxlen=size)

def get_context_window():
    return context_window

def set_base_instructions(instructions):
    global base_instructions
    base_instructions = instructions

async def prompt_ilec_data_async(prompt, model="gpt-5", max_turns=500):

    if context_window is None:
        raise Exception("Must call set_context_window_size()")

    MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:9090/mcp/")  # note the trailing slash

    key = os.getenv("OPENAI_API_KEY") or Path("/home/mike/workspace/soa-ilec/soa-ilec/.openai_key").read_text().strip()
    os.environ["OPENAI_API_KEY"] = key  # Agents SDK reads this
    
    final_prompt = base_instructions + prompt
    if len(context_window) > 0:
        final_prompt += "\n\n here are your previous prompts and responses for context:\n\n" + "\n".join(context_window)        

    async with MCPServerStreamableHttp(
        name="ilec",
        client_session_timeout_seconds = 1800,
        params={"url": MCP_URL, "timeout": 1800},
        cache_tools_list=True,
    ) as mcp_server:
        agent = Agent(
            name="Minimal MCP Agent",
            instructions="You may call MCP tools like `schema` and `sql`. Prefer SELECT with LIMIT. ",
            mcp_servers=[mcp_server],
            model=model            
        )

        print("Running request...")
        # --- simplest: one-shot, non-streaming
        result = await Runner.run(agent, final_prompt, max_turns=max_turns)
        context_window.append(prompt + "\n" + result.final_output)

        return result.final_output
    
# --- robust sync wrapper (works with or without a running loop) ---
def prompt_ilec_data(prompt: str, max_turns: int = 50) -> str:
    """
    Synchronously run prompt_ilec_data(prompt) and return the model's final output.
    Handles environments that already have an event loop by offloading to a worker thread.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop: safe to use asyncio.run directly
        return asyncio.run(prompt_ilec_data(prompt))
    else:
        # A loop is already running (e.g., Jupyter). Run in a separate thread.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(lambda: asyncio.run(prompt_ilec_data_async(prompt, max_turns=max_turns)))
            return fut.result()
