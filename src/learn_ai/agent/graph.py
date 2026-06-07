# pyrefly: ignore [missing-import]
from learn_ai.agent.edges import app

initial_state = {
    "file_path": "Data/batches/world_energy_consumption_batch_1.csv",
    "df": None,
    "error": None,
    "last_failed_step": None,
    "heal_attempts": 0,
    "max_attempts": 3,
    "heal_history": [],
    "status": "pending"
}

# Run the graph
final_state = app.invoke(initial_state)

print("\n--- Pipeline Execution Summary ---")
print(f"Final Status: {final_state['status']}")
print(f"Total Healing Attempts: {final_state['heal_attempts']}")
print(f"Applied fixes: {final_state['heal_history']}")