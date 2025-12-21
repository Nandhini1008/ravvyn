"""
API endpoints for the Database Reasoning Agent
Integrates with existing FastAPI system
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import sys
import os

# Add parent directory to path to import the agent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database_reasoning_agent import DatabaseReasoningAgent

router = APIRouter(prefix="/api/reasoning", tags=["reasoning"])

class QuestionRequest(BaseModel):
    question: str
    db_path: Optional[str] = "ravvyn.db"

class QuestionResponse(BaseModel):
    question: str
    answer: str
    success: bool
    error_message: Optional[str] = None

@router.post("/ask", response_model=QuestionResponse)
async def ask_question(request: QuestionRequest):
    """
    Ask a question to the Database Reasoning Agent
    
    The agent follows strict 7-step process with async optimization:
    1. Classify question type
    2. Resolve time to exact date range
    3. Validate time resolution
    4. Generate optimized SQL queries (parallel execution)
    5. Validate query results
    6. Construct answer using only DB data
    7. Handle failure conditions
    """
    try:
        # Initialize the reasoning agent
        agent = DatabaseReasoningAgent(request.db_path)
        
        # Get the answer following the strict 7-step process (async)
        answer = await agent.answer_question(request.question)
        
        # Check if it's a failure response
        failure_messages = [
            "Unable to answer accurately due to ambiguous or unavailable time data.",
            "No data available for the resolved time range."
        ]
        
        success = answer not in failure_messages
        
        return QuestionResponse(
            question=request.question,
            answer=answer,
            success=success,
            error_message=None if success else answer
        )
        
    except Exception as e:
        return QuestionResponse(
            question=request.question,
            answer="Unable to answer accurately due to ambiguous or unavailable time data.",
            success=False,
            error_message=str(e)
        )

@router.get("/health")
async def health_check():
    """Health check for the reasoning system"""
    try:
        agent = DatabaseReasoningAgent()
        return {"status": "healthy", "agent_initialized": True}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@router.post("/debug")
async def debug_question(request: QuestionRequest):
    """
    Debug endpoint that shows the internal reasoning process
    """
    try:
        agent = DatabaseReasoningAgent(request.db_path)
        
        # Step 1: Classify question
        question_type = agent.classify_question_type(request.question)
        
        # Step 2: Resolve time
        time_resolution = agent.resolve_time_to_date_range(request.question, question_type)
        
        # Step 4: Generate SQL
        sql = agent.generate_strict_sql(request.question, time_resolution)
        
        # Execute and get results
        if sql:
            success, results = agent.execute_query(sql)
        else:
            success, results = False, []
        
        # Final answer (async)
        final_answer = await agent.answer_question(request.question)
        
        return {
            "question": request.question,
            "step1_question_type": question_type,
            "step2_time_resolution": time_resolution,
            "step4_generated_sql": sql,
            "step5_query_success": success,
            "step5_results": results,
            "final_answer": final_answer
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))