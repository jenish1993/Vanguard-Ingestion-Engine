from langgraph.graph import StateGraph, END
# pyrefly: ignore [missing-import]
from learn_ai.agent.state import PipelineState
# pyrefly: ignore [missing-import]
from learn_ai.agent.nodes import extract_node, transform_node, load_node, healer_node

# Conditional routing edge after Extract Node
def route_after_extract(state: PipelineState) -> str:
    if state["error"] is not None:
        return "healer" if state["heal_attempts"] < state["max_attempts"] else "fail"
    return "transform"

# Conditional routing edge after Transform Node
def route_after_transform(state: PipelineState) -> str:
    if state["error"] is not None:
        return "healer" if state["heal_attempts"] < state["max_attempts"] else "fail"
    return "load"

# Conditional routing edge after Load Node
def route_after_load(state: PipelineState) -> str:
    if state["error"] is not None:
        return "healer" if state["heal_attempts"] < state["max_attempts"] else "fail"
    return "end"

# Healer node routing: routes back to the specific step that failed to retry it
def route_back_to_failed(state: PipelineState) -> str:
    return state["last_failed_step"]

# Compile graph
workflow = StateGraph(PipelineState)

# Add nodes
workflow.add_node("extract", extract_node)
workflow.add_node("transform", transform_node)
workflow.add_node("load", load_node)
workflow.add_node("healer", healer_node)

# Set entry point
workflow.set_entry_point("extract")

# Add conditional edges to handle success vs error branches
workflow.add_conditional_edges(
    "extract",
    route_after_extract,
    {
        "transform": "transform",
        "healer": "healer",
        "fail": END
    }
)

workflow.add_conditional_edges(
    "transform",
    route_after_transform,
    {
        "load": "load",
        "healer": "healer",
        "fail": END
    }
)

workflow.add_conditional_edges(
    "load",
    route_after_load,
    {
        "end": END,
        "healer": "healer",
        "fail": END
    }
)

workflow.add_conditional_edges(
    "healer",
    route_back_to_failed,
    {
        "extract": "extract",
        "transform": "transform",
        "load": "load"
    }
)

app = workflow.compile()