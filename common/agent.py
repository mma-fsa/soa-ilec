#!/usr/bin/env python3
import os
import asyncio

# Agents SDK
from agents import Agent, Runner, ModelSettings
from agents.mcp import MCPServerStreamableHttp

from env_vars import DEFAULT_MCP_URL, OPENAI_API_KEY
from prompt import BASE_AGENT_INSTRUCTIONS

class AssumptionsAgent:

    def __init__(self, mcp_url = DEFAULT_MCP_URL, instructions=BASE_AGENT_INSTRUCTIONS,\
                 openai_key=OPENAI_API_KEY, timeout = 1800, max_turns=500, model="gpt-5"):

        self.mcp_url = mcp_url
        self.instructions = instructions
        self.timeout = timeout
        self.max_turns = max_turns
        self.model = model
        os.environ["OPENAI_API_KEY"] = openai_key

    async def prompt_async(self, prompt):
        
        async with MCPServerStreamableHttp(
            name="ilec",
            client_session_timeout_seconds = self.timeout,
            params={
                "url": self.mcp_url, 
                "timeout": self.timeout
            },
            cache_tools_list=True,
        ) as mcp_server:
            agent = Agent(
                name=str(self.__class__),
                instructions=self.instructions,
                mcp_servers=[mcp_server],
                model=self.model,
                model_settings=ModelSettings(                
                    parallel_tool_calls=True,
                    tool_choice="required"
                )
            )
                        
            result = await Runner.run(agent, str(prompt), max_turns=self.max_turns)

            return result.final_output
            
    def prompt(self, prompt: str) -> str:

        try:
            _ = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop: safe to use asyncio.run directly
            return asyncio.run(self.prompt_asyc(prompt))
        else:
            # A loop is already running (e.g., Jupyter). Run in a separate thread.
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(lambda: asyncio.run(self.prompt_async(prompt)))
                return fut.result()
