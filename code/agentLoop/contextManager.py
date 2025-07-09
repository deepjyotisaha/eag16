# contextManager.py – 100% NetworkX Graph-First (SIMPLIFIED)

import networkx as nx
import json
import time
from datetime import datetime
from pathlib import Path
import asyncio
from action.executor import run_user_code, run_python_code_legacy
from rich.console import Console
from rich.prompt import Prompt
from rich.panel import Panel
from rich.text import Text
from config.log_config import get_logger, logger_step, logger_json_block, logger_prompt, logger_code_block

logger = get_logger(__name__)

class ExecutionContextManager:
    def __init__(self, plan_graph: dict, session_id: str = None, original_query: str = None, file_manifest: list = None, debug_mode: bool = False):
        # 🎯 Build NetworkX graph with ALL data
        self.plan_graph = nx.DiGraph()
        
        # Store session metadata in graph attributes
        self.plan_graph.graph['session_id'] = session_id or str(int(time.time()))[-8:]
        self.plan_graph.graph['original_query'] = original_query
        self.plan_graph.graph['file_manifest'] = file_manifest or []
        self.plan_graph.graph['created_at'] = datetime.utcnow().isoformat()
        self.plan_graph.graph['status'] = 'running'
        self.plan_graph.graph['globals_schema'] = {}
        
        # Add ROOT node
        self.plan_graph.add_node("ROOT",
            description="Initial Query",
            agent="System", 
            status='completed',
            output=None,
            error=None,
            cost=0.0,
            start_time=None,
            end_time=None,
            execution_time=0.0
        )

        # Build plan DAG
        for node in plan_graph.get("nodes", []):
            self.plan_graph.add_node(node["id"], 
                **node,
                status='pending',
                output=None,
                error=None,
                cost=0.0,
                start_time=None,
                end_time=None,
                execution_time=0.0
            )
            
        for edge in plan_graph.get("edges", []):
            self.plan_graph.add_edge(edge["source"], edge["target"])

        self.debug_mode = debug_mode
        self._live_display = None

    def get_ready_steps(self):
        """Return all steps whose dependencies are complete and not yet run."""
        ready = []
        
        for node_id in self.plan_graph.nodes:
            node_data = self.plan_graph.nodes[node_id]
            
            if node_id == "ROOT":
                continue
                
            status = node_data.get('status', 'pending')
            if status in ['completed', 'failed', 'running']:
                continue
                
            # Check if all dependencies are complete
            predecessors = list(self.plan_graph.predecessors(node_id))
            all_deps_complete = all(
                self.plan_graph.nodes[p].get('status', 'pending') == 'completed'
                for p in predecessors
            )
                
            if all_deps_complete:
                ready.append(node_id)
        
        return ready

    def mark_running(self, step_id):
        """Mark step as running"""
        self.plan_graph.nodes[step_id]['status'] = 'running'
        self.plan_graph.nodes[step_id]['start_time'] = datetime.utcnow().isoformat()
        self._auto_save()

    def _has_executable_code(self, output):
        """Universal detection of executable code patterns"""
        if not isinstance(output, dict):
            return False
        
        return (
            "code_variants" in output or
            any(k.startswith("CODE_") for k in output.keys()) or
            any(key in output for key in ["tool_calls", "schedule_tool", "browser_commands", "python_code"])
        )
    
    def _extract_executable_code(self, output):
        """Extract executable code"""
        code_to_execute = {}

        #logger.info(f"🔄 Executable code found in output: {output}")
        
        if "code_variants" in output:
            for key, code in output["code_variants"].items():
                if isinstance(code, str):
                    code_to_execute[key] = code.strip()

        #logger.info(f"🔄 Returning code to execute: {code_to_execute}")

        return code_to_execute
    
    async def _auto_execute_code(self, step_id, output, self_iteration=0):
        """Execute code with COMPLETE variable injection"""
        logger_step(logger, f"🔄 Executing code for step: {step_id} - Self iteration: {self_iteration}")

        code_to_execute = self._extract_executable_code(output)

        logger_code_block(logger, f"Code to execute:", code_to_execute, "None")

        logger_json_block(logger, f"Code to execute for step {step_id} - Self iteration: {self_iteration}", code_to_execute)
        
        if not code_to_execute:
            return {"status": "error", "error": "No executable code found"}
        
        # Get node data for context
        node_data = self.plan_graph.nodes[step_id]
        reads = node_data.get("reads", [])
        
        # Get globals_schema for injection
        globals_schema = self.plan_graph.graph['globals_schema']

        logger_json_block(logger, f"Step: {step_id} - Globals schema:", globals_schema)
        logger_json_block(logger, f"Step: {step_id} - Reads:", reads)
        logger_json_block(logger, f"Step: {step_id} - Node data:", node_data)
        
        for code_key, code in code_to_execute.items():
            logger.info(f"🔄 Executing code for step: {step_id} - Code key: {code_key}")
            try:
                # INJECT ALL AVAILABLE VARIABLES
                globals_injection = ""
                
                # 1. Inject ALL globals_schema variables
                for var_name, var_value in globals_schema.items():
                    globals_injection += f'{var_name} = {repr(var_value)}\n'
                
                # 2. Inject agent's own output variables
                for var_name, var_value in output.items():
                    if var_name in globals_schema:
                        logger.warning(f"⚠️ Variable name conflict detected: '{var_name}' already exists in globals_schema. Using agent output value.")
                        logger.info("We will not overvwritte the value of globals_schema with agent output value")
                    else:
                        if var_name not in ['code_variants', 'call_self', 'cost', 'input_tokens', 'output_tokens', 'execution_result', 'execution_status', 'execution_error', 'execution_time', 'executed_variant']:
                            globals_injection += f'{var_name} = {repr(var_value)}\n'
                
                # 3. Create convenience variables for reads
                reads_data = {}
                for read_key in reads:
                    if read_key in globals_schema:
                        reads_data[read_key] = globals_schema[read_key]
                
                globals_injection += f'reads_data = {repr(reads_data)}\n'
                
                enhanced_code = globals_injection + code
                
                # Code shared by Rohan - has bug
                #result = await run_user_code(
                #    enhanced_code,
                #    self.multi_mcp if hasattr(self, 'multi_mcp') else None,
                #    self.plan_graph.graph['session_id']
                #)

                #logger.info(f"🔄 Calling run_user_code for step: {step_id} - Code key: {code_key}")
                # Fix 1 - Use the new run_user_code, it uses correct code variants
                result = await run_user_code(
                    {"code_variants": {code_key: enhanced_code}},
                    self.multi_mcp if hasattr(self, 'multi_mcp') else None,
                    self.plan_graph.graph['session_id'],
                    globals_schema,
                    None,
                    step_id,
                    self_iteration
                )

                # Fix 2 - Use the old run_python_code_legacy, it uses hard coded code variant CODE_O1A
                #result = await run_python_code_legacy(
                #    enhanced_code,
                #    self.multi_mcp if hasattr(self, 'multi_mcp') else None,
                #    self.plan_graph.graph['session_id'],
                #    globals_schema
                #)

                logger_code_block(logger, f"Step: {step_id} - Code executed - Results:", enhanced_code, result)
                
                if result.get("status") == "success":
                    result["executed_variant"] = code_key
                    return result
                
            except Exception as e:
                logger.error(f"❌ Error executing code: {e}")
                continue
        
        return {"status": "error", "error": "All code variants failed"}
    
    def _merge_execution_results(self, original_output, execution_result):
        """Merge execution results into agent output"""
        
        logger.info(f"🔄 Merging execution results into agent output")
        #logger_json_block(logger, f"🔄 Original output", original_output)
        #logger_json_block(logger, f"🔄 Execution result", execution_result)
        
        if not isinstance(original_output, dict):
            logger.info(f"🔄 Inside Merge: Original output is not a dict")
            return original_output
        
        enhanced_output = original_output.copy()
        agent_output = enhanced_output.get("output", {})
        logger_json_block(logger, f"🔄 Inside Merge: Copied Enhanced output", enhanced_output)
        logger_json_block(logger, f"🔄 Inside Merge: Copied Agent output", agent_output)

        #enhanced_output["execution_result"] = execution_result.get("code_results")
        #enhanced_output["execution_status"] = execution_result.get("status")
        #enhanced_output["execution_error"] = execution_result.get("error") 
        #enhanced_output["execution_time"] = execution_result.get("total_time")
        #enhanced_output["executed_variant"] = execution_result.get("executed_variant")

        agent_output["execution_result"] = execution_result.get("code_results")
        agent_output["execution_status"] = execution_result.get("status")
        agent_output["execution_error"] = execution_result.get("error") 
        agent_output["execution_time"] = execution_result.get("total_time")
        agent_output["executed_variant"] = execution_result.get("executed_variant")
        logger_json_block(logger, f"🔄 Inside Merge: Partial Agent output", agent_output)
        
        # Merge execution results directly
        #if agent_output.get("status") == "success":
        #    logger.info(f"🔄 Inside Merge: Enhanced output is success")
        #    result_data = agent_output.get("result", {})
        #    if isinstance(result_data, dict):
        #        for key, value in result_data.items():
        #            if key not in enhanced_output:
        #                enhanced_output[key] = value

        logger.info(f"✅ Merged execution results into agent output")
        #logger_json_block(logger, f"✅ Enhanced output", enhanced_output)
        
        return enhanced_output
    
    def _is_clarification_request(self, agent_type, output):
        """Check if agent output requires user interaction"""
        return (
            agent_type == "ClarificationAgent" and 
            isinstance(output, dict) and
            "clarificationMessage" in output
        )
    
    def set_live_display(self, live_display):
        """Set reference to Live display for pausing during user interaction"""
        self._live_display = live_display
    
    def _handle_user_interaction_rich(self, clarification_output):
        """Handle user interaction with Rich prompts"""
        message = clarification_output.get("clarificationMessage", "")
        options = clarification_output.get("options", [])
        
        # Pause Live display during user interaction
        live_was_running = False
        if self._live_display and self._live_display._live_render.is_started:
            self._live_display.stop()
            live_was_running = True
        
        try:
            console = Console()
            console.clear()
            console.print(Panel(
                Text(message, style="bold white"),
                title="🤔 User Input Required",
                border_style="yellow",
                padding=(1, 2)
            ))
            
            if options:
                console.print("\n[bold cyan]Available Options:[/bold cyan]")
                for i, option in enumerate(options, 1):
                    console.print(f"  [bold white]{i}.[/bold white] {option}")
                
                choices = [str(i) for i in range(1, len(options) + 1)]
                choice = Prompt.ask(
                    "\n[bold green]Select option[/bold green]",
                    choices=choices,
                    default="1",
                    show_choices=False
                )
                
                selected_option = options[int(choice) - 1]
                console.print(f"[dim]✓ Selected: {selected_option}[/dim]")
                return selected_option
            else:
                response = Prompt.ask("\n[bold green]Your response[/bold green]")
                console.print(f"[dim]✓ Response: {response}[/dim]")
                return response
                
        finally:
            if live_was_running and self._live_display:
                self._live_display.start()
    
    async def mark_done(self, step_id, output=None, cost=None, input_tokens=None, output_tokens=None):
        """Mark step as completed with COMPLETE extraction logic"""
        node_data = self.plan_graph.nodes[step_id]
        agent_type = node_data.get('agent', '')
        writes = node_data.get("writes", [])
        
        # Extract cost data
        if output and isinstance(output, dict):
            cost = cost or output.get('cost', 0.0)
            input_tokens = input_tokens or output.get('input_tokens', 0)
            output_tokens = output_tokens or output.get('output_tokens', 0)
        
        # USER INTERACTION CHECK
        if self._is_clarification_request(agent_type, output):
            try:
                user_response = self._handle_user_interaction_rich(output)
                writes_to = output.get("writes_to", "user_response")
                self.plan_graph.graph['globals_schema'][writes_to] = user_response
                
                output = output.copy()
                output["user_response"] = user_response
                output["interaction_completed"] = True
                print(f"✅ User input captured: {writes_to} = '{user_response}'")
                
            except Exception as e:
                print(f"❌ User interaction failed: {e}")
        
        # CODE EXECUTION CHECK
        execution_result = None
        #if self._has_executable_code(output):
        #    try:
        #        #execution_result = await self._auto_execute_code(step_id, output)
        #        execution_result = output.get("code_results", {})
        #        logger_json_block(logger, f"🔄 Context Manager - Execution result for step {step_id}", execution_result)
        #        #output = self._merge_execution_results(output, execution_result)
        #        #logger_json_block(logger, f"🔄 Context Manager - Merged execution result with output for step {step_id}", output)
        #    except Exception as e:
        #        print(f"❌ Code execution failed: {e}")
        
        # EXTRACTION LOGIC - Handle both code execution results AND direct agent outputs
        globals_schema = self.plan_graph.graph['globals_schema']
        execution_result = output.get("execution_result", {})

        #logger_json_block(logger, f"🔄 Mark done: Output", output)
        #logger_json_block(logger, f"🔄 Mark done: Execution Result", execution_result)
        
        if writes:
            for write_key in writes:
                extracted = False
                logger.info(f"🔄 Marked done: Extracting {write_key}")
                
                # Strategy 1: Extract from code execution results (RetrieverAgent, CoderAgent)
                if execution_result and execution_result.get("status") == "success":
                    result_data = execution_result.get("result", {})
                    
                    if write_key in result_data:
                        #logger.info(f"🔄 Inside Marked done: Extracting {write_key} = {result_data[write_key]}")
                        globals_schema[write_key] = result_data[write_key]
                        print(f"✅ Extracted {write_key} from execution result")
                        logger.info(f"✅ Extracted {write_key} from execution result")
                        #logger_json_block(logger, f"🔄 Marked done: Updated globals_schema with {write_key} from execution result, globals_schema:", globals_schema)
                        extracted = True
                    elif len(result_data) == 1 and len(writes) == 1:
                        key, value = next(iter(result_data.items()))
                        globals_schema[write_key] = value
                        print(f"✅ Extracted {write_key} from key {key} in execution result")
                        logger.info(f"✅ Extracted {write_key} from key {key} in execution result")
                        #logger_json_block(logger, f"🔄 Marked done: Updated globals_schema with {write_key} from key {key} in execution result, globals_schema:", globals_schema)
                        extracted = True
                
                # Strategy 2: Extract from direct agent output (ThinkerAgent, DistillerAgent, FormatterAgent)
                if not extracted and output and isinstance(output, dict):
                    if write_key in output:
                        logger.info(f"🔄 Marked done: Extracting {write_key} = {output[write_key]} (direct)")
                        globals_schema[write_key] = output[write_key]
                        print(f"✅ Extracted {write_key} from direct output")
                        logger.info(f"✅ Extracted {write_key} from direct output")
                        #logger_json_block(logger, f"🔄 Marked done: Updated globals_schema with {write_key} from direct output, globals_schema:", globals_schema)
                        extracted = True
                
                # Strategy 3: Emergency fallback - try to find any matching data
                if not extracted:
                    # Set empty placeholder to prevent downstream errors
                    globals_schema[write_key] = []
                    print(f"⚠️  Could not extract {write_key} from writes")
                    logger.info(f"⚠️  Could not extract {write_key} from writes")
                    #logger_json_block(logger, f"🔄 Marked done: Could not extract {write_key}, set empty placeholder for {write_key} in globals_schema:", globals_schema)
        
        # Look for code outputs and update global schema
        #if execution_result and execution_result.get("status") == "success":
        #    result_data = execution_result.get("result", {})
        #    # Iterate through all execution result data and store in globals_schema
        #    for key, value in result_data.items():
        #        # Skip if this key is already handled by the writes list
        #        if key not in writes and key not in globals_schema:
        #            globals_schema[key] = value
        #            print(f"✅ Stored temporary code output: {key}")
        #            logger.info(f"✅ Stored temporary code output: {key}")
        

        
        # Store results
        node_data['status'] = 'completed'
        node_data['end_time'] = datetime.utcnow().isoformat()
        node_data['output'] = output
        node_data['cost'] = cost or 0.0
        node_data['input_tokens'] = input_tokens or 0
        node_data['output_tokens'] = output_tokens or 0
        node_data['total_tokens'] = (input_tokens or 0) + (output_tokens or 0)
        
        # Calculate execution time
        if 'start_time' in node_data and node_data['start_time']:
            start = datetime.fromisoformat(node_data['start_time'])
            end = datetime.fromisoformat(node_data['end_time'])
            node_data['execution_time'] = (end - start).total_seconds()
        
        print(f"✅ {step_id} completed successfully")
        logger_step(logger, f"✅ {step_id} completed successfully")
        #logger_json_block(logger, f"🔄 Marked done for step {step_id} - Node data", node_data)
        #logger_json_block(logger, f"🔄 Marked done for step {step_id} - Globals schema", globals_schema)

        self._auto_save()



    async def update_globals_schema(self, step_id, output=None, self_iteration=0):
        """Update globals_schema with COMPLETE extraction logic"""
        node_data = self.plan_graph.nodes[step_id]
        agent_type = node_data.get('agent', '')
        writes = node_data.get("writes", [])
        
        execution_result = None

        # EXTRACTION LOGIC - Handle both code execution results AND direct agent outputs
        globals_schema = self.plan_graph.graph['globals_schema']
        execution_result = output.get("execution_result", {})

        logger_json_block(logger, f"🔄 Updating globals_schema: Output", output)
        logger_json_block(logger, f"🔄 Updating globals_schema: Execution Result", execution_result)
        
        if writes:
            for write_key in writes:
                extracted = False
                logger.info(f"🔄 Updating globals_schema: Extracting {write_key}")
                
                # Strategy 1: Extract from code execution results (RetrieverAgent, CoderAgent)
                if execution_result and execution_result.get("status") == "success":
                    result_data = execution_result.get("result", {})
                    
                    if write_key in result_data:
                        logger.info(f"🔄 Inside Updating globals_schema: Extracting {write_key} = {result_data[write_key]}")
                        globals_schema[write_key] = result_data[write_key]
                        print(f"✅ Extracted {write_key} from execution result")
                        logger.info(f"✅ Extracted {write_key} from execution result")
                        logger_json_block(logger, f"🔄 Updating globals_schema: Updated globals_schema with {write_key} from execution result, globals_schema:", globals_schema)
                        extracted = True
                    #elif len(result_data) == 1 and len(writes) == 1:
                    #    key, value = next(iter(result_data.items()))
                    #    globals_schema[write_key] = value
                    #    print(f"✅ Extracted {write_key} from key {key} in execution result")
                    #    logger.info(f"✅ Extracted {write_key} from key {key} in execution result")
                    #    logger_json_block(logger, f"🔄 Updating globals_schema: Updated globals_schema with {write_key} from key {key} in execution result, globals_schema:", globals_schema)
                    #    extracted = True
                
                # Strategy 2: Extract from direct agent output (ThinkerAgent, DistillerAgent, FormatterAgent)
                if not extracted and output and isinstance(output, dict):
                    if write_key in output:
                        logger.info(f"🔄 Updating globals_schema: Extracting {write_key} = {output[write_key]} (direct)")
                        globals_schema[write_key] = output[write_key]
                        print(f"✅ Extracted {write_key} from direct output")
                        logger.info(f"✅ Extracted {write_key} from direct output")
                        logger_json_block(logger, f"🔄 Updating globals_schema: Updated globals_schema with {write_key} from direct output, globals_schema:", globals_schema)
                        extracted = True
                
                # Strategy 3: Emergency fallback - try to find any matching data
                if not extracted:
                    # Set empty placeholder to prevent downstream errors
                    globals_schema[write_key] = []
                    print(f"⚠️  Could not extract {write_key}")
                    logger.info(f"⚠️  Could not extract {write_key}")
                    #logger_json_block(logger, f"🔄 Marked done: Could not extract {write_key}, set empty placeholder for {write_key} in globals_schema:", globals_schema)
        
        # Look for code outputs and update global schema
        if execution_result and execution_result.get("status") == "success":
            result_data = execution_result.get("result", {})
            # Iterate through all execution result data and store in globals_schema
            for key, value in result_data.items():
                # Skip if this key is already handled by the writes list
                if key not in writes and key not in globals_schema:
                    globals_schema[key] = value
                    print(f"✅ Stored temporary code output: {key}")
                    logger.info(f"✅ Stored temporary code output: {key}")
        
        #print(f"✅ {step_id} completed successfully")
        logger_step(logger, f"✅ {step_id} updated globals_schema")
        #logger_json_block(logger, f"🔄 Marked done for step {step_id} - Node data", node_data)
        #logger_json_block(logger, f"🔄 Marked done for step {step_id} - Globals schema", globals_schema)

        self._auto_save()    

    def mark_failed(self, step_id, error=None):
        """Mark step as failed"""
        node_data = self.plan_graph.nodes[step_id]
        node_data['status'] = 'failed'
        node_data['end_time'] = datetime.utcnow().isoformat()
        node_data['error'] = str(error) if error else None
        
        if node_data['start_time']:
            start = datetime.fromisoformat(node_data['start_time'])
            end = datetime.fromisoformat(node_data['end_time'])
            node_data['execution_time'] = (end - start).total_seconds()
            
        self._auto_save()

    def get_step_data(self, step_id):
        """Get all step data from graph"""
        return self.plan_graph.nodes[step_id]

    def get_inputs(self, reads):
        """Get input data from graph globals_schema"""
        inputs = {}
        globals_schema = self.plan_graph.graph['globals_schema']
        
        for read_key in reads:
            if read_key in globals_schema:
                inputs[read_key] = globals_schema[read_key]
            else:
                print(f"⚠️  Missing dependency: '{read_key}' not found in globals_schema")
                print(f"📋 Available keys: {list(globals_schema.keys())}")
                
        return inputs

    def all_done(self):
        """Check if all steps are completed or failed"""
        return all(
            self.plan_graph.nodes[node_id]['status'] in ['completed', 'failed']
            for node_id in self.plan_graph.nodes
        )

    def get_execution_summary(self):
        """Get execution summary with cost and token breakdown"""
        completed = sum(1 for node_id in self.plan_graph.nodes 
                       if node_id != "ROOT" and 
                       self.plan_graph.nodes[node_id].get('status') == 'completed')
        failed = sum(1 for node_id in self.plan_graph.nodes 
                    if node_id != "ROOT" and 
                    self.plan_graph.nodes[node_id].get('status') == 'failed')
        total = len(self.plan_graph.nodes) - 1
        
        # Calculate costs
        total_cost = 0.0
        total_input_tokens = 0
        total_output_tokens = 0
        cost_breakdown = {}
        
        for node_id in self.plan_graph.nodes:
            if node_id != "ROOT":
                node_data = self.plan_graph.nodes[node_id]
                node_cost = node_data.get('cost', 0.0)
                node_input_tokens = node_data.get('input_tokens', 0)
                node_output_tokens = node_data.get('output_tokens', 0)
                
                if node_cost > 0:
                    agent = node_data.get('agent', 'Unknown')
                    cost_breakdown[f"{node_id} ({agent})"] = {
                        "cost": node_cost,
                        "input_tokens": node_input_tokens,
                        "output_tokens": node_output_tokens
                    }
                
                total_cost += node_cost
                total_input_tokens += node_input_tokens
                total_output_tokens += node_output_tokens
        
        # Get final outputs
        final_outputs = {}
        all_reads = set()
        all_writes = set()
        
        for node_id in self.plan_graph.nodes:
            node_data = self.plan_graph.nodes[node_id]
            all_reads.update(node_data.get("reads", []))
            all_writes.update(node_data.get("writes", []))
        
        final_write_keys = all_writes - all_reads
        globals_schema = self.plan_graph.graph['globals_schema']
        for key in final_write_keys:
            if key in globals_schema:
                final_outputs[key] = globals_schema[key]

        return {
            "session_id": self.plan_graph.graph['session_id'],
            "original_query": self.plan_graph.graph['original_query'],
            "completed_steps": completed,
            "failed_steps": failed,
            "total_steps": total,
            "total_cost": total_cost,
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_input_tokens + total_output_tokens,
            "cost_breakdown": cost_breakdown,
            "final_outputs": final_outputs,
            "globals_schema": globals_schema
        }

    def set_file_profiles(self, file_profiles):
        """Store file profiles in graph attributes"""
        self.plan_graph.graph['file_profiles'] = file_profiles

    def set_multi_mcp(self, multi_mcp):
        """Set multi_mcp reference for code execution"""
        self.multi_mcp = multi_mcp

    def _auto_save(self):
        """Auto-save graph to disk"""
        if self.debug_mode:
            return
        try:
            self._save_session()
        except Exception as e:
            print(f"⚠️  Auto-save failed: {e}")

    def _save_session(self):
        """Save the NetworkX graph as session"""
        base_dir = Path("memory/session_summaries_index")
        today = datetime.now()
        date_dir = base_dir / str(today.year) / f"{today.month:02d}" / f"{today.day:02d}"
        date_dir.mkdir(parents=True, exist_ok=True)
        
        session_id = self.plan_graph.graph['session_id']
        session_file = date_dir / f"session_{session_id}.json"
        
        graph_data = nx.node_link_data(self.plan_graph)
        
        with open(session_file, 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, indent=2, default=str, ensure_ascii=False)

    @classmethod
    def load_session(cls, session_file: Path, debug_mode: bool = False):
        """Load a NetworkX graph session from disk"""
        with open(session_file, 'r', encoding='utf-8') as f:
            graph_data = json.load(f)
        
        plan_graph = nx.node_link_graph(graph_data, edges="links")
        
        context = cls.__new__(cls)
        context.plan_graph = plan_graph
        context.debug_mode = debug_mode
        return context
