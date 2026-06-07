from langchain_google_genai import ChatGoogleGenerativeAI
# pyrefly: ignore [missing-import]
from src.healer.prompt import HealingRecipe
import os

# Initialize the Gemini model
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
    google_api_key= os.getenv("GEMINI_API_KEY")
)

# Bind the structured output schema
structured_llm = llm.with_structured_output(HealingRecipe)