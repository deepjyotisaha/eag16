# flow.py – 100% NetworkX Graph-First (No agentSession)

import networkx as nx
import asyncio
from agentLoop.contextManager import ExecutionContextManager
from agentLoop.agents import AgentRunner
from utils.utils import log_step, log_error
from agentLoop.model_manager import ModelManager
from agentLoop.visualizer import ExecutionVisualizer
from rich.live import Live
from rich.console import Console
from datetime import datetime
from config.log_config import get_logger, logger_step, logger_json_block, logger_prompt, logger_code_block

logger = get_logger(__name__)    

class AgentLoop4:
    def __init__(self, multi_mcp, strategy="conservative"):
        self.multi_mcp = multi_mcp
        self.strategy = strategy
        self.agent_runner = AgentRunner(multi_mcp)

    async def run(self, query, file_manifest, globals_schema, uploaded_files):
        # Phase 1: File Profiling (if files exist)
        file_profiles = {}
        if uploaded_files:
            logger_step(logger, "Phase 1: File Profiling - Running DistillerAgent")
            file_result = await self.agent_runner.run_agent(
                "DistillerAgent",
                {
                    "task": "profile_files",
                    "files": uploaded_files,
                    "instruction": "Profile and summarize each file's structure, columns, content type",
                    "writes": ["file_profiles"]
                }
            )
            if file_result["success"]:
                file_profiles = file_result["output"]
        else:
            logger_step(logger, "Phase 1: File Profiling - No files uploaded, skipping DistillerAgent")

        # Phase 2: Planning with AgentRunner
        logger_step(logger, "Phase 2: Planning - Running PlannerAgent")
        plan_result = await self.agent_runner.run_agent(
            "PlannerAgent",
            {
                "original_query": query,
                "planning_strategy": self.strategy,
                "globals_schema": globals_schema,
                "file_manifest": file_manifest,
                "file_profiles": file_profiles
            }
        )

        if not plan_result["success"]:
            raise RuntimeError(f"Planning failed: {plan_result['error']}")

        # Check if plan_graph exists
        if 'plan_graph' not in plan_result['output']:
            raise RuntimeError(f"PlannerAgent output missing 'plan_graph' key. Got: {list(plan_result['output'].keys())}")
        
        plan_graph = plan_result["output"]["plan_graph"]

        logger_json_block(logger, "Plan Graph", plan_graph)

        try:
            # Phase 3: 100% NetworkX Graph-First Execution
            logger_step(logger, "Phase 3: 100% NetworkX Graph-First Execution - Calling ExecutionContextManager")
            context = ExecutionContextManager(
                plan_graph,
                session_id=None,
                original_query=query,
                file_manifest=file_manifest
            )
            
            # Add multi_mcp reference
            context.multi_mcp = self.multi_mcp
            
            # Initialize graph with file profiles and globals
            context.set_file_profiles(file_profiles)
            logger_json_block(logger, "Globals Schema", globals_schema)
            context.plan_graph.graph['globals_schema'].update(globals_schema)

            logger_step(logger, "🔄 Calling execution context manager to execute the plan graph")

            # Phase 4: Execute DAG with visualization
            logger_step(logger, "Phase 4: Execute DAG with visualization")
            await self._execute_dag(context)

            # Phase 5: Return the CONTEXT OBJECT, not summary
            return context

        except Exception as e:
            print(f"❌ ERROR creating ExecutionContextManager: {e}")
            import traceback
            traceback.print_exc()
            raise

    async def _execute_dag(self, context):
        """Execute DAG with visualization - DEBUGGING MODE"""
        
        # Get plan_graph structure for visualization
        plan_graph = {
            "nodes": [
                {"id": node_id, **node_data} 
                for node_id, node_data in context.plan_graph.nodes(data=True)
            ],
            "links": [
                {"source": source, "target": target}
                for source, target in context.plan_graph.edges()
            ]
        }

        #logger.info("🔄 Calling execution context manager to execute the plan graph")
        #logger_json_block(logger, "Plan Graph", plan_graph)
        
        # Create visualizer
        visualizer = ExecutionVisualizer(plan_graph)
        console = Console()
        
        # 🔧 DEBUGGING MODE: No Live display, just regular prints
        max_iterations = 20
        iteration = 0

        logger_step(logger, f"Starting execution of plan graph with {len(context.plan_graph.nodes())} nodes and {len(context.plan_graph.edges())} edges")
        logger_json_block(logger, "Plan Graph", plan_graph)
        logger_step(logger, f"Max iterations: {max_iterations}")

        while not context.all_done() and iteration < max_iterations:

            #logger.info(f"🔄 Iteration: {iteration} for max iterations: {max_iterations}")
            logger_step(logger, f"🔄 Iteration: {iteration} for max iterations: {max_iterations}")
            iteration += 1
            
            # Show current state
            console.print(visualizer.get_layout())
            
            # Get ready nodes
            ready_steps = context.get_ready_steps()
            
            if not ready_steps:
                # Check for failures
                has_failures = any(
                    context.plan_graph.nodes[n]['status'] == 'failed' 
                    for n in context.plan_graph.nodes
                )
                if has_failures:
                    break
                await asyncio.sleep(0.3)
                continue

            # Mark running
            for step_id in ready_steps:
                visualizer.mark_running(step_id)
                context.mark_running(step_id)
            
            # ✅ EXECUTE AGENTS FOR REAL
            #logger.info(f"🔄 Executing agents for real")
            logger_step(logger, f"🔄 Executing agents steps which are ready for execution: {ready_steps}")
            #tasks = [self._execute_step(step_id, context) for step_id in ready_steps]
            #tasks = [self._execute_step_self(step_id, context) for step_id in ready_steps]
            tasks = [self._execute_step_try(step_id, context) for step_id in ready_steps]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            logger_step(logger, f"🔄 Executing agents steps which are ready for execution: {ready_steps} - Completed")
            logger_json_block(logger, f"🔄 Results of executing agents steps which are ready for execution: {ready_steps}", results)

            # Process results
            for step_id, result in zip(ready_steps, results):
                #logger.info(f"🔄 Result for step {step_id}: {result}")
                logger_json_block(logger, f"🔄 Processing Result for step {step_id}", result)
                if isinstance(result, Exception):
                    visualizer.mark_failed(step_id, result)
                    logger.info(f"❌ Marking step {step_id} as failed")
                    context.mark_failed(step_id, str(result))
                elif result["success"]:
                    logger.info(f"✅ Marking step {step_id} as completed")
                    visualizer.mark_completed(step_id)
                    await context.mark_done(step_id, result["output"])
                else:
                    logger.info(f"❌ Marking step {step_id} as failed")
                    visualizer.mark_failed(step_id, result["error"])
                    context.mark_failed(step_id, result["error"])

        # Final state
        console.print(visualizer.get_layout())
        
        if context.all_done():
            console.print("🎉 All tasks completed!")

    async def _execute_step(self, step_id, context):
        """Execute a single step with call_self support"""

        step_data = context.get_step_data(step_id)
        agent_type = step_data["agent"]

        logger_step(logger, f"🔄 Executing step: {step_id} by calling agent {step_data['agent']}")
        logger.info(f"🔄 Executing step: {step_id} by calling agent {step_data['agent']}")
        
        # Get inputs from NetworkX graph
        inputs = context.get_inputs(step_data.get("reads", []))
        #logger_json_block(logger, "Inputs", inputs)
        
        # 🔧 HELPER FUNCTION: Build agent input (consistent for both iterations)
        def build_agent_input(instruction=None, previous_output=None, iteration_context=None):
            if agent_type == "FormatterAgent":
                all_globals = context.plan_graph.graph['globals_schema'].copy()
                return {
                    "step_id": step_id,
                    "agent_prompt": instruction or step_data.get("agent_prompt", step_data["description"]),
                    "reads": step_data.get("reads", []),
                    "writes": step_data.get("writes", []),
                    "inputs": inputs,
                    "all_globals_schema": all_globals,  # ✅ ALWAYS included for FormatterAgent
                    "original_query": context.plan_graph.graph['original_query'],
                    "session_context": {
                        "session_id": context.plan_graph.graph['session_id'],
                        "created_at": context.plan_graph.graph['created_at'],
                        "file_manifest": context.plan_graph.graph['file_manifest']
                    },
                    **({"previous_output": previous_output} if previous_output else {}),
                    **({"iteration_context": iteration_context} if iteration_context else {})
                }
            else:
                return {
                    "step_id": step_id,
                    "agent_prompt": instruction or step_data.get("agent_prompt", step_data["description"]),
                    "reads": step_data.get("reads", []),
                    "writes": step_data.get("writes", []),
                    "inputs": inputs,
                    **({"previous_output": previous_output} if previous_output else {}),
                    **({"iteration_context": iteration_context} if iteration_context else {})
                }

        # Execute first iteration
        agent_input = build_agent_input()
        logger.info(f"🔄 Running agent {agent_type} with input: {agent_input}")
        logger_json_block(logger, f"Agent Input for step {step_id}", agent_input)
        result = await self.agent_runner.run_agent(agent_type, agent_input)
        logger_json_block(logger, f"Agent Result for step {step_id}", result)
        if result["success"]:
            output = result["output"]
            
            # Check for call_self
            if output.get("call_self"):
                logger_step(logger, f"🔄 Call self detected for step: {step_id}")
                # Handle code execution if needed
                if context._has_executable_code(output):
                    logger_step(logger, f"🔄 Need to execute code for step: {step_id}")
                    execution_result = await context._auto_execute_code(step_id, output)
                    if execution_result.get("status") == "success":
                        execution_data = execution_result.get("result", {})
                        logger_json_block(logger, f"Execution data for step {step_id}", execution_data)
                        inputs = {**inputs, **execution_data}  # Update inputs for iteration 2
                        logger_json_block(logger, f"Merged Inputs for step {step_id}", inputs)
                
                # Execute second iteration with consistent input structure
                second_agent_input = build_agent_input(
                    instruction=output.get("next_instruction", "Continue the task"),
                    previous_output=output,
                    iteration_context=output.get("iteration_context", {})
                )

                logger.info(f"🔄 Running agent {agent_type} for step {step_id} with input (second iteration): {second_agent_input}")
                logger_json_block(logger, f"Agent Input for step {step_id} - Second iteration", second_agent_input)
                
                second_result = await self.agent_runner.run_agent(agent_type, second_agent_input)
                
                # 💾 CRITICAL: Store iteration data in session
                iterations_data = [
                    {"iteration": 1, "output": output}
                ]
                
                if second_result["success"]:
                    iterations_data.append({"iteration": 2, "output": second_result["output"]})
                    final_result = second_result
                else:
                    iterations_data.append(None)
                    final_result = result
                
                # Store iterations in the node data for session persistence
                step_data = context.get_step_data(step_id)
                step_data['iterations'] = iterations_data
                step_data['call_self_used'] = True
                step_data['final_iteration_output'] = final_result["output"]
                
                return final_result
            else:
                return result
        else:
            return result
        
    async def _execute_step_try(self, step_id, context):
        """Execute a single step with call_self support"""

        step_data = context.get_step_data(step_id)
        agent_type = step_data["agent"]

        logger_step(logger, f"🔄 Executing step: {step_id} by calling agent {step_data['agent']}")
        #logger.info(f"🔄 Executing step: {step_id} by calling agent {step_data['agent']}")
        
        # Get inputs from NetworkX graph
        inputs = context.get_inputs(step_data.get("reads", []))
        #logger_json_block(logger, "Inputs", inputs)
        
        # 🔧 HELPER FUNCTION: Build agent input (consistent for both iterations)
        def build_agent_input(instruction=None, previous_output=None, iteration_context=None):
            if agent_type == "FormatterAgent":
                all_globals = context.plan_graph.graph['globals_schema'].copy()
                return {
                    "step_id": step_id,
                    "agent_prompt": instruction or step_data.get("agent_prompt", step_data["description"]),
                    "reads": step_data.get("reads", []),
                    "writes": step_data.get("writes", []),
                    "inputs": inputs,
                    "all_globals_schema": all_globals,  # ✅ ALWAYS included for FormatterAgent
                    "original_query": context.plan_graph.graph['original_query'],
                    "session_context": {
                        "session_id": context.plan_graph.graph['session_id'],
                        "created_at": context.plan_graph.graph['created_at'],
                        "file_manifest": context.plan_graph.graph['file_manifest']
                    },
                    **({"previous_output": previous_output} if previous_output else {}),
                    **({"iteration_context": iteration_context} if iteration_context else {})
                }
            else:
                return {
                    "step_id": step_id,
                    "agent_prompt": instruction or step_data.get("agent_prompt", step_data["description"]),
                    "reads": step_data.get("reads", []),
                    "writes": step_data.get("writes", []),
                    "inputs": inputs,
                    **({"previous_output": previous_output} if previous_output else {}),
                    **({"iteration_context": iteration_context} if iteration_context else {})
                }

        # Execute first iteration
        agent_input = build_agent_input()
        logger.info(f"🔄 Running agent {agent_type} with input: {agent_input}")
        logger_json_block(logger, f"🔄 Agent Input for step {step_id}", agent_input)
        result = await self.agent_runner.run_agent(agent_type, agent_input)
        logger_json_block(logger, f"🔄 Agent Output for step {step_id}", result)
        if result["success"]:
            output = result["output"]

            if context._has_executable_code(output):
                logger_step(logger, f"🔄 Need to execute code for step: {step_id}")
                execution_result = await context._auto_execute_code(step_id, output)
                logger_json_block(logger, f"🔄 Execution result for step {step_id}", execution_result)
                if execution_result.get("status") == "success":
                    logger_step(logger, f"🔄 Code execution successful for step {step_id}, merging execution result with output")
                    #logger_json_block(logger, f"🔄 Output for step {step_id}", output)
                    #logger_json_block(logger, f"🔄 Execution result for step {step_id}", execution_result)
 
                    output = context._merge_execution_results(result, execution_result)
                    logger.info(f"✅ Merged execution result with output for step {step_id}")
                    #logger_json_block(logger, f"✅ Merged execution result with output for step {step_id}, output:", output)

                else:
                    logger.info(f"❌ Execution failed for step {step_id}: {execution_result}")
            else:
                logger_step(logger, f"🔄 No code execution for step: {step_id}, assigning result to output")
                output = result        
            
            # 💾 CRITICAL: Store iteration data in session
            iterations_data = [
                    {"iteration": 1, "output": output}
            ]

            if output.get("call_self"):
                logger_step(logger, f"🔄 Call self detected for step: {step_id}")

                second_agent_input = build_agent_input(
                    instruction=output.get("next_instruction", "Continue the task"),
                    previous_output=output,
                    iteration_context=output.get("iteration_context", {})
                )

                logger.info(f"🔄 Running agent {agent_type} for step {step_id} with input (second iteration): {second_agent_input}")

                logger_json_block(logger, f"Agent Input for step {step_id} - Second iteration", second_agent_input)
                
                second_result = await self.agent_runner.run_agent(agent_type, second_agent_input)

                if second_result["success"]:
                    iterations_data.append({"iteration": 2, "output": second_result["output"]})
                    final_result = second_result
                else:
                    iterations_data.append(None)
                    

            # Store iterations in the node data for session persistence
            step_data = context.get_step_data(step_id)
            step_data['iterations'] = iterations_data
            #NOTE: This needs to be fixed, we need to check if call_self was used in the final iteration
            step_data['call_self_used'] = True
            step_data['final_iteration_output'] = output

            logger_step(logger, f"✅ Step {step_id} completed successfully")
            #logger_json_block(logger, f"✅ Step {step_id} completed successfully, final result:", final_result)
                
            return output
        else:
            return result


    async def _execute_step_self(self, step_id, context, iteration=0, max_iterations=2):
        """Execute a single step with call_self support"""

        step_data = context.get_step_data(step_id)
        agent_type = step_data["agent"]

        logger_step(logger, f"🔄 *** Executing step: {step_id} by calling agent {step_data['agent']} (iteration {iteration})")
        #logger.info(f"🔄 *** Executing step: {step_id} by calling agent {step_data['agent']} (iteration {iteration})")
        
        # Get inputs from NetworkX graph - ONLY for first iteration
        if iteration == 0:
            inputs = context.get_inputs(step_data.get("reads", []))
        else:
            # For subsequent iterations, use the inputs that were updated in previous iterations
            # The inputs should already be available from the previous iteration's execution
            inputs = step_data.get('current_inputs', {})

        logger_json_block(logger, f"Just got the inputs for step {step_id} - Iteration {iteration}", inputs)
        
        # 🔧 HELPER FUNCTION: Build agent input (consistent for all iterations)
        def build_agent_input(instruction=None, previous_output=None, iteration_context=None):
            if agent_type == "FormatterAgent":
                all_globals = context.plan_graph.graph['globals_schema'].copy()
                return {
                    "step_id": step_id,
                    "agent_prompt": instruction or step_data.get("agent_prompt", step_data["description"]),
                    "reads": step_data.get("reads", []),
                    "writes": step_data.get("writes", []),
                    "inputs": inputs,
                    "all_globals_schema": all_globals,  # ✅ ALWAYS included for FormatterAgent
                    "original_query": context.plan_graph.graph['original_query'],
                    "session_context": {
                        "session_id": context.plan_graph.graph['session_id'],
                        "created_at": context.plan_graph.graph['created_at'],
                        "file_manifest": context.plan_graph.graph['file_manifest']
                    },
                    **({"previous_output": previous_output} if previous_output else {}),
                    **({"iteration_context": iteration_context} if iteration_context else {})
                }
            else:
                return {
                    "step_id": step_id,
                    "agent_prompt": instruction or step_data.get("agent_prompt", step_data["description"]),
                    "reads": step_data.get("reads", []),
                    "writes": step_data.get("writes", []),
                    "inputs": inputs,
                    **({"previous_output": previous_output} if previous_output else {}),
                    **({"iteration_context": iteration_context} if iteration_context else {})
                }

        # Build agent input based on iteration
        if iteration == 0:
            # First iteration
            agent_input = build_agent_input()
        else:
            # Subsequent iterations - use previous output
            previous_output = step_data['iterations'][-1]['output']
            logger_step(logger, f"🔄 Buidling agent input from previous output for step {step_id} - Iteration {iteration}")
            logger_json_block(logger, f"Previous output for step {step_id} - Iteration {iteration}", previous_output)
            agent_input = build_agent_input(
                instruction=previous_output.get("next_instruction", "Continue the task"),
                previous_output=previous_output,
                iteration_context=previous_output.get("iteration_context", {})
            )

        #logger.info(f"🔄 Running agent {agent_type} with input (iteration {iteration}): {agent_input}")
        logger_json_block(logger, f"Agent Input for step {step_id} - Iteration {iteration}", agent_input)
        # Execute agent
        result = await self.agent_runner.run_agent(agent_type, agent_input)
        logger_json_block(logger, f"Agent Result for step {step_id} - Iteration {iteration}", result)
        
        if not result["success"]:
            logger_step(logger, f"❌ Agent failed for step {step_id} at iteration {iteration}")
            return result

        output = result["output"]
        
        # Handle code execution if needed
        if context._has_executable_code(output):
            logger_step(logger, f"🔄 Need to execute code for step: {step_id} (iteration {iteration})")
            execution_result = await context._auto_execute_code(step_id, output, iteration)
            if execution_result.get("status") == "success":
                # 🔧 FIX: Extract the actual result data correctly
                code_results = execution_result.get("code_results", {})
                execution_data = code_results.get("result", {})
                logger_json_block(logger, f"Raw execution result for step {step_id} - Iteration {iteration}", execution_result)
                logger_json_block(logger, f"Code results for step {step_id} - Iteration {iteration}", code_results)
                logger_json_block(logger, f"Execution data for step {step_id} - Iteration {iteration}", execution_data)
                
                # Update globals_schema with execution data - variables returned by the code
                # This ensures ALL code outputs are stored in globals_schema for the next iteration
                #if execution_data and isinstance(execution_data, dict):
                #    for key, value in execution_data.items():
                #        if key not in ['call_self', 'code_variants', 'cost', 'input_tokens', 'output_tokens', 'execution_result', 'execution_status', 'execution_error', 'execution_time', 'executed_variant']:
                #            context.plan_graph.graph['globals_schema'][key] = value
                
                
                #logger_json_block(logger, f"Execution data for step {step_id} - Iteration {iteration}", execution_data)

                logger_json_block(logger, f"Printing the inputs for step {step_id} - Iteration {iteration}", inputs)
                
                # Update inputs with execution results for next iteration
                inputs = {**inputs, **execution_data}
                
                # Store updated inputs in step_data for next iteration
                step_data['current_inputs'] = inputs
                
                # Also merge execution results into the output
                output = {**output, **execution_data}
                
                logger_json_block(logger, f"Updated inputs for step {step_id} - Iteration {iteration}", inputs)
                logger_json_block(logger, f"Updated output for step {step_id} - Iteration {iteration}", output)
        else:
            logger_step(logger, f"🔄 No code execution for step {step_id} - Iteration {iteration}")
             # 🔧 CRITICAL:  Update globals_schema with agent output 
            #There is no code execution, so we need to update the globals_schema with the agent output
            #if output and isinstance(output, dict):
            #    for key, value in output.items():
            #        if key not in ['call_self', 'code_variants', 'cost', 'input_tokens', 'output_tokens', 'execution_result', 'execution_status', 'execution_error', 'execution_time', 'executed_variant']:
            #            context.plan_graph.graph['globals_schema'][key] = value
            #            #logger_step(logger, f"✅ Agent output: Stored {key} = {value} in globals_schema")

        
        # 🔧 CRITICAL: Update globals_schema with agent output (regardless of code execution)
        # This ensures ALL agent outputs are stored in globals_schema


        #logger_json_block(logger, f"Updated globals_schema", context.plan_graph.graph['globals_schema'])

        # Check for call_self
        if output.get("call_self") and iteration < max_iterations:
            logger_step(logger, f"🔄 Call self detected for step: {step_id} (iteration {iteration})")
            
            # Store current iteration before recursive call
            if iteration == 0:
                step_data['iterations'] = [{"iteration": iteration, "output": output}]
            else:
                step_data['iterations'].append({"iteration": iteration, "output": output})
            
            # Recursive call for next iteration
            logger_step(logger, f"🔄 Recursively calling next iteration for step {step_id}")
            return await self._execute_step_self(step_id, context, iteration + 1, max_iterations)
        
        else:
            # No more call_self or max iterations reached - we're done
            logger_step(logger, f"🔄 *** No more call_self or max iterations reached for step {step_id}")
            #logger.info(f"🔄 *** No more call_self or max iterations reached for step {step_id}")
            if output.get("call_self") and iteration >= max_iterations:
                logger_step(logger, f"⚠️ *** Max iterations ({max_iterations}) reached for step {step_id}")
                #logger.info(logger, f"⚠️ Max iterations ({max_iterations}) reached for step {step_id}")
                step_data['max_iterations_reached'] = True
            
            # Store final iteration
            if iteration == 0:
                step_data['iterations'] = [{"iteration": iteration, "output": output}]
            else:
                step_data['iterations'].append({"iteration": iteration, "output": output})
            
            # Store final metadata exactly like original
            step_data['call_self_used'] = len(step_data['iterations']) > 1
            # NOTE: If we want the code variable to be here, then use current_output instead of output
            step_data['final_iteration_output'] = output
            
            # NOTE: If we want the code variable to be here, then use current_output instead of output
            # 🔧 CRITICAL: Ensure globals_schema is updated with final results
            #if current_output and isinstance(current_output, dict):
            #    for key, value in current_output.items():
            #        if key not in ['call_self', 'code_variants', 'cost', 'input_tokens', 'output_tokens']:
            #            context.plan_graph.graph['globals_schema'][key] = value
            #            logger_step(logger, f"✅ Final: Stored {key} = {value} in globals_schema")
            
            # 🔧 CRITICAL: Save session to persist globals_schema
            #context._auto_save()
            
            logger_step(logger, f"✅ Step {step_id} completed after {len(step_data['iterations'])} iteration(s)")
            logger_json_block(logger, f"Final output for step {step_id}", output)
            #logger_json_block(logger, f"Final output for step {step_id}", current_output)
            logger_json_block(logger, f"Final output stored for step {step_id}", step_data['final_iteration_output'])
            logger_json_block(logger, f"Globals schema for step {step_id}", context.plan_graph.graph['globals_schema'])
            logger_json_block(logger, f"Result for step {step_id}", result)
            return result




    async def _handle_failures(self, context):
        """Handle failures via mid-session replanning"""
        # TODO: Implement mid-session replanning with PlannerAgent
        log_error("Mid-session replanning not yet implemented")
