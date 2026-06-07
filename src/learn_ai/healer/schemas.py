from pydantic import BaseModel, Field
from typing import List, Dict, Any, Literal

class HealingAction(BaseModel):
    action_type: Literal[
        "fill_null",       # Fill null/missing values in a column
        "cast_column",     # Safely cast column to another type (with cleaning)
        "replace_value",   # Replace specific bad strings (e.g. 'N/A' or '$12')
        "truncate_string", # Truncate string to match VARCHAR lengths
        "drop_row"         # Drop rows that are corrupted
    ] = Field(description="The category of fix to apply to the DataFrame.")
    
    column: str = Field(description="The name of the target column to modify.")
    
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Key-value parameters for the action. E.g. {'fill_value': 0.0} or {'target_type': 'double', 'remove_chars': '$'}"
    )

class HealingRecipe(BaseModel):
    error_analysis: str = Field(description="Analysis of what caused the exception.")
    explanation: str = Field(description="Step-by-step description of how the actions fix it.")
    actions: List[HealingAction] = Field(description="Ordered list of actions to perform on the DataFrame.")