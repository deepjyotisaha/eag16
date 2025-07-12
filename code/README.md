# EAG16: Multi-Agent System with Self-Iterating Agents

A sophisticated multi-agent system that combines planning, execution, and self-iteration capabilities to handle complex tasks through coordinated agent workflows.

## 🎯 Overview

This system implements a **NetworkX Graph-First Architecture** with multiple specialized agents that can:
- **Plan complex workflows** using a dedicated PlannerAgent
- **Self-iterate** to improve their outputs through multiple passes
- **Execute code** and generate static assets (HTML, CSS, JS)
- **Format comprehensive reports** with consulting-grade quality
- **Retrieve external data** using web search and document tools

## 🏗️ Architecture

### Core Components

1. **AgentLoop4** - Main orchestrator that manages the execution flow
2. **ExecutionContextManager** - NetworkX-based graph execution engine
3. **AgentRunner** - Handles individual agent execution and LLM interactions
4. **MultiMCP** - Manages multiple Model Context Protocol servers for tool access

### Agent Types

| Agent | Purpose | Self-Iteration | Tools |
|-------|---------|----------------|-------|
| **PlannerAgent** | Creates execution plans | ❌ | None |
| **RetrieverAgent** | Data acquisition | ✅ | Web search, documents |
| **CoderAgent** | Code generation | ✅ | Web application, Python |
| **FormatterAgent** | Report formatting | ✅ | None |
| **DistillerAgent** | File profiling | ❌ | None |
| **ThinkerAgent** | Analysis & reasoning | ❌ | None |
| **QAAgent** | Question answering | ❌ | Web search |
| **ExecutorAgent** | Code execution | ❌ | None |

## 🔄 Self-Iterating Agents

### How Self-Iteration Works

Self-iterating agents can call themselves multiple times to improve their outputs. This is controlled by the `call_self` flag in their JSON responses.

#### Self-Iteration Flow

```python
# Agent decides if it needs more iterations
{
  "call_self": true,
  "next_instruction": "Continue with next phase",
  "iteration_context": {
    "current_step": "search_phase",
    "next_step": "extraction_phase"
  }
}
```

#### Example: RetrieverAgent Self-Iteration

**First Iteration (Search Phase):**
```json
{
  "flight_urls_1A": [],
  "call_self": true,
  "next_instruction": "Extract detailed flight information from major airline URLs only",
  "iteration_context": {
    "current_step": "search_urls",
    "next_step": "extract_details",
    "filter_criteria": "major_airlines_only"
  },
  "code_variants": {
    "CODE_1A": "urls = fetch_search_urls('Bangalore to NYC flights Emirates Air India British Airways', 10)\nreturn {'flight_urls_1A': urls}"
  }
}
```

**Second Iteration (Extraction Phase):**
```json
{
  "blr_to_nyc_flight_options_T001": [],
  "call_self": false,
  "code_variants": {
    "CODE_2A": "results = []\nfor url in flight_urls_1A[:5]:\n    content = webpage_url_to_raw_text(url)\n    results.append({'url': url, 'content': content})\nreturn {'blr_to_nyc_flight_options_T001': results}"
  }
}
```

#### Example: CoderAgent Self-Iteration

**First Iteration (HTML Generation):**
```json
{
  "layout_html_1A": [],
  "call_self": true,
  "next_instruction": "Generate CSS styling for the HTML layout",
  "code_variants": {
    "CODE_1A": "html = \"\"\"<html><head><title>Timer</title></head><body><div id='timer'>5:00</div></body></html>\"\"\"\nwith open('index.html', 'w') as f:\n    f.write(html)\nreturn {'layout_html_1A': {'path': 'index.html', 'content': html}}"
  }
}
```

**Second Iteration (CSS Generation):**
```json
{
  "layout_css_2A": [],
  "call_self": true,
  "next_instruction": "Generate JavaScript for countdown functionality",
  "code_variants": {
    "CODE_2A": "css = \"\"\"body { font-family: Arial; text-align: center; }\n#timer { font-size: 48px; margin-top: 100px; }\"\"\"\nwith open('style.css', 'w') as f:\n    f.write(css)\nreturn {'layout_css_2A': {'path': 'style.css', 'content': css}}"
  }
}
```

**Third Iteration (JavaScript Generation):**
```json
{
  "countdown_timer_T001": [],
  "call_self": false,
  "code_variants": {
    "CODE_3A": "js = \"\"\"const timerDisplay = document.getElementById('timer');\nlet timeLeft = 300;\nfunction updateTimer() {\n    let minutes = Math.floor(timeLeft / 60);\n    let seconds = timeLeft % 60;\n    timerDisplay.textContent = `${minutes}:${seconds}`;\n    timeLeft--;\n    if (timeLeft < 0) {\n        timerDisplay.textContent = \"Time's up!\";\n        clearInterval(timerInterval);\n    }\n}\nconst timerInterval = setInterval(updateTimer, 1000);\"\"\"\nwith open('script.js', 'w') as f:\n    f.write(js)\nreturn {'countdown_timer_T001': {'path': 'script.js', 'content': js}}"
  }
}
```

#### Example: FormatterAgent Self-Iteration

**First Iteration (Basic Structure):**
```json
{
  "formatted_report_T003": "<div class='report'><h1>Analysis Report</h1><h2>Executive Summary</h2><p>Basic overview...</p></div>",
  "call_self": true,
  "next_instruction": "Add detailed analysis sections, competitive positioning, and risk factors",
  "iteration_context": {
    "current_step": "basic_structure",
    "next_step": "enrich_content"
  }
}
```

**Second Iteration (Enhanced Content):**
```json
{
  "formatted_report_T003": "<div class='report'><h1>Analysis Report</h1>...<h2>Risk Factors</h2><p>Detailed risk analysis...</p><h2>Competitive Analysis</h2><p>Market positioning...</p></div>",
  "call_self": false
}
```

## 🌐 Global Schema Updates

The system maintains a global schema that gets updated after each agent execution, enabling data flow between agents.

### Global Schema Update Process

```python
async def update_globals_schema(self, step_id, output=None, self_iteration=0):
    """Update globals_schema with COMPLETE extraction logic"""
    globals_schema = self.plan_graph.graph['globals_schema']
    execution_result = output.get("execution_result", {})
    
    # Extract from code execution results (RetrieverAgent, CoderAgent)
    if execution_result and execution_result.get("status") == "success":
        result_data = execution_result.get("result", {})
        for write_key in writes:
            if write_key in result_data:
                globals_schema[write_key] = result_data[write_key]
    
    # Extract from direct agent output (ThinkerAgent, FormatterAgent)
    if write_key in output:
        globals_schema[write_key] = output[write_key]
```

### Data Flow Example

1. **RetrieverAgent** extracts data → stored in `globals_schema`
2. **CoderAgent** reads from `globals_schema` → generates code
3. **FormatterAgent** reads from `globals_schema` → creates report

## 📊 Sample Outputs

The system generates both code applications and comprehensive reports. Here are actual examples from the `sample_output/` directory:

### Generated Code Example: Countdown Timer Application

**Files generated by CoderAgent with self-iteration:**

📁 **HTML File** ([`sample_output/code/index.html`](sample_output/code/index.html)):
```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Countdown Timer</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <div class="container">
        <h1 id="timerDisplay">5:00</h1>
    </div>
    <script src="script.js"></script>
</body>
</html>
```

📁 **CSS File** ([`sample_output/code/style.css`](sample_output/code/style.css)):
```css
body {
    background-color: #222;
    color: #eee;
    font-family: sans-serif;
    display: flex;
    justify-content: center;
    align-items: center;
    min-height: 100vh;
    margin: 0;
}

.container {
    text-align: center;
}

#timerDisplay {
    font-size: 5em;
    font-weight: bold;
}
```

📁 **JavaScript File** ([`sample_output/code/script.js`](sample_output/code/script.js)):
```javascript
const timerDisplay = document.getElementById('timerDisplay');
let timeLeft = 300; // 5 minutes in seconds

function updateTimer() {
    let minutes = Math.floor(timeLeft / 60);
    let seconds = timeLeft % 60;
    seconds = seconds < 10 ? '0' + seconds : seconds;
    timerDisplay.textContent = `${minutes}:${seconds}`;
    timeLeft--;

    if (timeLeft < 0) {
        timerDisplay.textContent = "Time's up!";
        clearInterval(timerInterval);
    }
}

const timerInterval = setInterval(updateTimer, 1000);
```

### Generated Report Example: Mahindra XUV 3XO Analysis

**Files generated by FormatterAgent with self-iteration:**

📁 **Main Report** ([`sample_output/report/index.html`](sample_output/report/index.html)) - 16KB comprehensive analysis
📁 **Report Styling** ([`sample_output/report/styles.css`](sample_output/report/styles.css)) - Professional styling
📁 **Interactive Features** ([`sample_output/report/script.js`](sample_output/report/script.js)) - Sortable tables
📁 **Compressed Archive** ([`sample_output/report/report.zip`](sample_output/report/report.zip)) - Complete report package

**Report Features:**
- **Executive Summary** with market positioning
- **Pricing Analysis** with 9 trim variants and detailed pricing
- **Engine Specifications** with performance data
- **Feature Comparisons** (safety, digital, luxury features)
- **Pros and Cons** analysis
- **Interactive Tables** with sorting capabilities
- **Professional Styling** with responsive design

**Sample Report Content:**
```html
<div class="comprehensive-report">
<h1>📊 COMPREHENSIVE Mahindra XUV 3XO ANALYSIS REPORT</h1>

<div class="executive-summary">
<h2>🎯 Executive Summary</h2>
<p>The Mahindra XUV 3XO is a subcompact SUV known for its bold design, spacious cabin, and advanced features. It offers a sorted ride, engaging drive, and attractive pricing, making it a competitive option in its segment.</p>
</div>

<h2>💰 Pricing and Variants Analysis</h2>
<table>
  <thead>
    <tr>
      <th>Variant</th>
      <th>Features</th>
      <th>Price (Ex-Showroom)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>MX1</td>
      <td>Rear seat centre armrest, parking sensors, air conditioner...</td>
      <td>Rs. 7.99 Lakh</td>
    </tr>
    <!-- 8 more variants with detailed features -->
  </tbody>
</table>

<h2>Engine and Performance</h2>
<table>
  <thead>
    <tr>
      <th>Engine Type</th>
      <th>Power</th>
      <th>Torque</th>
      <th>Transmission</th>
      <th>Mileage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1.2-litre turbo-petrol (mStallion TCMPFi)</td>
      <td>110 & 129 bhp</td>
      <td>200 & 230 Nm</td>
      <td>6-speed manual & Automatic (TC)</td>
      <td>17.96 to 20.1 kmpl</td>
    </tr>
    <!-- 2 more engine variants -->
  </tbody>
</table>

<!-- Additional sections: Safety Features, Digital Features, Pros and Cons, etc. -->
</div>
```

## 🚀 Usage

### Running the System

```bash
# Install dependencies
pip install -r requirements.txt

# Run the main application
python main.py
```

### Example Query

```
📁 File Input (optional):
Enter file paths (one per line), or press Enter to skip:
Press Enter twice when done.

📝 Your Question:
Create a countdown timer web application with HTML, CSS, and JavaScript
```

## 📝 Sample Prompts

The system can handle a wide variety of complex queries. Here are some examples:

### Mathematical and Computational Tasks
- **Find the ASCII values of characters in INDIA and then return sum of exponentials of those values.**
- **What is the log value of the amount that Anmol singh paid for his DLF apartment via Capbridge? Hint: use local**

### Research and Information Retrieval
- **How much Anmol singh paid for his DLF apartment via Capbridge?**
- **What do you know about Don Tapscott and Anthony Williams?**
- **What is the relationship between Gensol and Go-Auto?**
- **Who is the current chairman of DLF? Search local documents.**
- **Summarize this page: https://theschoolof.ai/**

### Document Analysis
- **Which course are we teaching on Canvas LMS?** (with uploaded PDF)
- **Analyze the code files shared and prepare a detailed report on the functionality, design, features, bugs and also include new feature suggestions.**

### Comparative Analysis
- **Find the main difference between latest BMW 7 and 5 series online. Reply back in detail and in markdown.**
- **What are the main differences between Mercedes S Class and E Class. Reply back in markdown list.**
- **I want to know the features of Mercedes S Class. Reply back in markdown list**
- **I want to know the features of Mercedes S Class. I want details on safety related features and luxury features. Reply back in markdown list**

### Web Application Development
- **Write an amazingly beautiful html/css/js code to show a timer that COUNTS DOWN minutes to 0. Use dark theme and separate steps of Coder Agent**
- **Write a simple web application to show "Hello world" to the end user. Use dark theme**
- **Write an amazingly beautiful html/css/js code to show a timer that COUNTS DOWN minutes to 0. Use dark theme and single step, but have different files for HTML, CSS and JS**
- **Write an amazingly beautiful html/css/js code to show a timer that COUNTS DOWN minutes from 5 to 0. Use dark theme and use a single coder agent step to generate the HTML, JS and CSS files**

### Comprehensive Research and Reporting
- **Mahindra has launched a new vehicle called xuv 3xo, I want to know feature details, digital features, safety features, price etc. Fetch information from multiple URLs. Compile a detailed report into a report which structured and formatted; then translate it into a beautiful well formatted webpage to view the report. Use single step for RetrieverAgent and CoderAgent unless you need another Agent in between.**

### System Response

The system will:
1. **Plan** the execution using PlannerAgent
2. **Generate code** using CoderAgent (with self-iteration for multiple files)
3. **Execute** the code to create the files
4. **Format** a comprehensive report using FormatterAgent

## 🔧 Configuration

### Agent Configuration (`config/agent_config.yaml`)

```yaml
agents:
  PlannerAgent:
    prompt_file: "prompts/planner_prompt.txt"
    model: "gemini"
    mcp_servers: []
    
  RetrieverAgent:
    prompt_file: "prompts/retriever_prompt_new_v1.txt" 
    model: "gemini"
    mcp_servers: ["documents", "websearch"]
    
  CoderAgent:
    prompt_file: "prompts/coder_prompt_new_v1.txt"
    model: "gemini"
    mcp_servers: ["documents", "websearch"]
    
  FormatterAgent:
    prompt_file: "prompts/formatter_prompt.txt"
    model: "gemini"
    mcp_servers: []
```

### MCP Server Configuration (`config/mcp_server_config.yaml`)

```yaml
mcp_servers:
  - name: "documents"
    command: "python"
    args: ["-m", "mcp.server.filesystem", "--root", "./documents"]
    
  - name: "websearch"
    command: "python"
    args: ["-m", "mcp.server.websearch"]
```

## 🎯 Key Features

### 1. Multiple Agents with Planner
- **PlannerAgent** creates execution plans as NetworkX graphs
- **Specialized agents** handle specific tasks (retrieval, coding, formatting)
- **Dependency management** ensures proper execution order
- **Parallel execution** of independent tasks

### 2. Self-Iterating Agents
- **RetrieverAgent**: Multi-step data acquisition (search → extract)
- **CoderAgent**: Multi-file generation (HTML → CSS → JS)
- **FormatterAgent**: Multi-pass report enhancement
- **Iteration control** via `call_self` flag and `max_iterations`

### 3. Global Schema Updates
- **Automatic extraction** from agent outputs
- **Data persistence** across agent executions
- **Variable injection** for code execution
- **Session management** for complex workflows

### 4. Code Execution
- **Automatic detection** of executable code
- **Variable injection** from global schema
- **Multiple code variants** for robustness
- **Execution result merging** with agent outputs

## 📁 Project Structure

```
eag16/code/
├── main.py                 # Main application entry point
├── agentLoop/             # Core agent execution logic
│   ├── flow.py           # AgentLoop4 orchestrator
│   ├── agents.py         # AgentRunner for LLM interactions
│   ├── contextManager.py # NetworkX graph execution engine
│   └── visualizer.py     # Execution visualization
├── config/               # Configuration files
│   ├── agent_config.yaml # Agent definitions
│   └── mcp_server_config.yaml # MCP server configs
├── prompts/              # Agent prompt templates
├── action/               # Code execution utilities
├── mcp_servers/          # MCP server implementations
├── sample_output/        # Example outputs
│   ├── code/            # Generated code examples
│   └── report/          # Generated report examples
└── utils/               # Utility functions
```

## 🔍 Technical Details

### NetworkX Graph Structure

```python
# Graph nodes represent execution steps
{
  "id": "T001",
  "agent": "RetrieverAgent",
  "description": "Retrieve data from web",
  "reads": ["input_data"],
  "writes": ["retrieved_data_T001"],
  "status": "completed",
  "output": {...},
  "iterations": [...]
}

# Graph edges represent dependencies
[{"source": "T001", "target": "T002"}]
```

### Self-Iteration Control

```python
# Agent decides iteration need
if agent_output.get("call_self") and iteration_count < max_iterations:
    # Continue iteration with previous output as input
    next_input = {**inputs, **agent_output.get("execution_result", {}).get("result", {})}
    # Execute next iteration
```

### Global Schema Management

```python
# Automatic extraction from execution results
if execution_result.get("status") == "success":
    result_data = execution_result.get("result", {})
    for write_key in writes:
        if write_key in result_data:
            globals_schema[write_key] = result_data[write_key]
```

## 🎉 Benefits

1. **Modular Design**: Each agent has a specific role and can be optimized independently
2. **Self-Improvement**: Agents can iterate to improve their outputs
3. **Scalable**: Easy to add new agents or modify existing ones
4. **Robust**: Multiple code variants and error handling
5. **Comprehensive**: Generates both code and detailed reports
6. **Flexible**: Supports various input types and output formats



This system demonstrates advanced multi-agent coordination with self-iteration capabilities, making it suitable for complex task automation and content generation workflows.


