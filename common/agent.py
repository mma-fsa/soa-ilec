#!/usr/bin/env python3
import os, re, asyncio

# Agents SDK
from agents import Agent, Runner, ModelSettings
from agents.mcp import MCPServerStreamableHttp

from env_vars import DEFAULT_MCP_URL, OPENAI_API_KEY, DEFAULT_AGENT_WORK_DIR
from prompt import BASE_AGENT_INSTRUCTIONS

from app_shared import AppSession, Database
from pathlib import Path

class AssumptionsAgent:

    def __init__(self, mcp_url = DEFAULT_MCP_URL, instructions=BASE_AGENT_INSTRUCTIONS,\
                 openai_key=OPENAI_API_KEY, timeout = 3600, max_turns=500, model="gpt-5"):
        self.mcp_url = mcp_url
        self.instructions = instructions
        self.timeout = timeout
        self.max_turns = max_turns
        self.model = model
        os.environ["OPENAI_API_KEY"] = openai_key
    
    @staticmethod
    def is_valid_agent_name(agent_name: str):
        VALID_NAME = re.compile(r'^(?!\.\.?$)[^/\x00]{1,255}$')
        return False if not VALID_NAME.match(agent_name) else True        

    def _set_active_agent_name(self, agent_name : str):
        # check the agent name
        if not AssumptionsAgent.is_valid_agent_name(agent_name):
            raise Exception(f"Invalid agent name '{agent_name}'")

        with Database.get_session_conn() as conn:
            app_session = AppSession(conn)
            app_session["AGENT_NAME"] = agent_name
            work_dir_path = Path(DEFAULT_AGENT_WORK_DIR) / Path(agent_name + "/")
            if not work_dir_path.exists():
                work_dir_path.mkdir(parents=True, exist_ok=True)
            app_session["MCP_WORK_DIR"] = str(work_dir_path)
                
    def _set_active_agent_status(self, status : str):        
        status = status.upper().strip()
        valid_status = set(["PENDING", "RUNNING", "COMPLETE"])
        if not status in valid_status:
            raise ValueError(f"Invalid agent_status: {status}")
        
        with Database.get_session_conn() as conn:
            app_session = AppSession(conn)
            app_session["AGENT_STATUS"] = status
        
    async def prompt_async(self, agent_name : str, prompt):
        
        self._set_active_agent_name(agent_name)
        self._set_active_agent_status("PENDING")

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
            self._set_active_agent_status("RUNNING")
            result = await Runner.run(agent, str(prompt), max_turns=self.max_turns)
            self._set_active_agent_status("COMPLETE")

            return result.final_output
            
    def prompt(self, agent_name: str, prompt: str) -> str:
        try:
            _ = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop: safe to use asyncio.run directly
            return asyncio.run(self.prompt_asyc(agent_name, prompt))
        else:
            # A loop is already running (e.g., Jupyter). Run in a separate thread.
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(lambda: asyncio.run(self.prompt_async(agent_name, prompt)))
                return fut.result()
