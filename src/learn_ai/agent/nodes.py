import traceback
from typing import List
from pyspark.sql.utils import AnalysisException
# pyrefly: ignore [missing-import]
from learn_ai.healer.schemas import HealingAction
from learn_ai.healer.llm_client import structured_llm
from learn_ai.agent.state import PipelineState
from src.pipeline.ingestion import PySparkIngestionPipeline

from pyspark.sql import DataFrame, functions as F

def extract_node(state: PipelineState) -> dict:
    print("\n--- Executing Extract Node ---")
    file_path = state["file_path"]
    
    try:
        # Assuming you inject your SparkSession or import your pipeline class
        # Here we mock the pipeline ingestion extract
        raw_df = pipeline.extract(file_path)
        
        # If successfully extracted:
        return {"df": raw_df, "error": None}
        
    except Exception as e:
        error_msg = f"Extraction failed: {str(e)}\n{traceback.format_exc()}"
        print(f"Error captured during extract: {e}")
        return {
            "error": error_msg,
            "last_failed_step": "extract"
        }

def transform_node(state: PipelineState) -> dict:
    print("\n--- Executing Transform Node ---")
    df = state["df"]
    
    if df is None:
        return {"error": "DataFrame is empty, cannot transform.", "last_failed_step": "transform"}
        
    try:
        # Assuming you inject your pipeline class or custom business transformations
        # Here we mock the pipeline ingestion transform (Identity block placeholder)
        # E.g., transformed_df = pipeline.transform(df)
        
        # For this illustration, we pass the df forward unmodified:
        return {"df": df, "error": None}
    except Exception as e:
        error_msg = f"Transformation failed: {str(e)}\n{traceback.format_exc()}"
        print(f"Error captured during transform: {e}")
        return {
            "error": error_msg,
            "last_failed_step": "transform"
        }

def load_node(state: PipelineState) -> dict:
    print("\n--- Executing Load Node ---")
    df = state["df"]
    
    if df is None:
        return {"error": "DataFrame is empty, cannot load.", "last_failed_step": "load"}
        
    try:
        # Perform Spark JDBC load operation
        # df.write.jdbc(...)
        
        # If load succeeds:
        return {"status": "success", "error": None}
    except Exception as e:
        error_msg = f"Load failed: {str(e)}\n{traceback.format_exc()}"
        print(f"Error captured during load: {e}")
        return {
            "error": error_msg,
            "last_failed_step": "load"
        }

def apply_actions_to_df(df: DataFrame, actions: List[HealingAction]) -> DataFrame:
    """Dynamically applies PySpark DataFrame modifications based on structured AI actions."""
    for action in actions:
        col_name = action.column
        act_type = action.action_type
        params = action.parameters
        
        print(f"Applying fix: {act_type} on column '{col_name}' with parameters {params}")
        
        if act_type == "fill_null":
            fill_val = params.get("fill_value")
            df = df.fillna({col_name: fill_val})
            
        elif act_type == "replace_value":
            old_val = params.get("old_value")
            new_val = params.get("new_value")
            df = df.withColumn(col_name, F.when(df[col_name] == old_val, new_val).otherwise(df[col_name]))
            
        elif act_type == "cast_column":
            target_type = params.get("target_type", "double")
            clean_chars = params.get("clean_chars", []) # e.g., ["$", "%", ","]
            
            col_expr = df[col_name]
            # Strip unwanted characters before casting
            for char in clean_chars:
                col_expr = F.regexp_replace(col_expr, f"\\{char}", "")
            
            df = df.withColumn(col_name, col_expr.cast(target_type))
            
        elif act_type == "truncate_string":
            max_len = params.get("max_length", 255)
            df = df.withColumn(col_name, F.substring(df[col_name], 1, max_len))
            
        elif act_type == "drop_row":
            # Filter out rows matching criteria
            # E.g., dropping rows where this column is null
            df = df.filter(df[col_name].isNotNull())
            
    return df

def healer_node(state: PipelineState) -> dict:
    print("\n--- Executing AI Healer Node ---")
    error = state["error"]
    df = state["df"]
    
    # Extract data sample to help LLM understand the formatting issue
    sample_data = ""
    if df is not None:
        # Get schema fields and top 5 rows
        schema_fields = df.schema.simpleString()
        rows = df.limit(5).collect()
        sample_data = f"Schema: {schema_fields}\nSample Rows:\n" + "\n".join([str(r.asDict()) for r in rows])
        
    prompt = f"""
    You are an AI Data Healer Agent. An ingestion pipeline has failed.
    
    FAILED STEP: {state['last_failed_step']}
    ERROR DETAILS:
    {error}
    
    DATAFRAME CONTEXT:
    {sample_data}
    
    Identify the issue and provide a HealingRecipe consisting of a sequence of HealingActions.
    Only output actions that correspond to the supported operations: fill_null, cast_column, replace_value, truncate_string, drop_row.
    """
    
    # Request Structured Output from LangChain
    recipe: HealingRecipe = structured_llm.invoke(prompt)
    
    print(f"AI Analysis: {recipe.error_analysis}")
    print(f"Proposed Healing Strategy: {recipe.explanation}")
    
    # Apply actions to Spark DataFrame
    healed_df = apply_actions_to_df(df, recipe.actions)
    
    return {
        "df": healed_df,
        "error": None, # Reset error to trigger retry
        "heal_attempts": state["heal_attempts"] + 1,
        "heal_history": state["heal_history"] + [recipe.model_dump()]
    }