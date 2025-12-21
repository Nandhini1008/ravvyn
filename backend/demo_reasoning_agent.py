"""
Comprehensive Demo of Database Reasoning Agent
Shows the 7-step strict process in action
"""

import sys
import os
from datetime import date

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database_reasoning_agent import DatabaseReasoningAgent

def demo_step_by_step(question: str, agent: DatabaseReasoningAgent):
    """Demonstrate the 7-step process for a single question"""
    
    print(f"🔍 QUESTION: {question}")
    print("=" * 60)
    
    # STEP 1: Classify Question Type
    question_type = agent.classify_question_type(question)
    print(f"STEP 1 - Question Classification: {question_type}")
    
    # STEP 2: Resolve Time into Exact Date Range
    time_resolution = agent.resolve_time_to_date_range(question, question_type)
    print(f"STEP 2 - Time Resolution:")
    print(f"  • Type: {time_resolution['time_type']}")
    print(f"  • Date Range: {time_resolution['start_date']} to {time_resolution['end_date']}")
    print(f"  • Confidence: {time_resolution['confidence']}")
    
    # STEP 3: Validate Time Resolution (internal)
    if time_resolution["confidence"] == "low":
        print("STEP 3 - ❌ FAILED: Low confidence in time resolution")
        return "Unable to answer accurately due to ambiguous or unavailable time data."
    else:
        print("STEP 3 - ✅ PASSED: Time resolution validated")
    
    # STEP 4: Generate STRICT SQL
    sql = agent.generate_strict_sql(question, time_resolution)
    print(f"STEP 4 - Generated SQL:")
    if sql:
        print(f"  {sql.strip()}")
    else:
        print("  ❌ No SQL generated - question not supported")
        return "Unable to answer accurately due to ambiguous or unavailable time data."
    
    # Execute Query
    success, results = agent.execute_query(sql)
    print(f"STEP 4b - Query Execution: {'✅ SUCCESS' if success else '❌ FAILED'}")
    if success:
        print(f"  Results: {results}")
    
    # STEP 5: Validate Query Results
    validation_passed = agent.validate_query_results(results, time_resolution)
    print(f"STEP 5 - Result Validation: {'✅ PASSED' if validation_passed else '❌ FAILED'}")
    
    if not validation_passed:
        return "No data available for the resolved time range."
    
    # STEP 6: Construct Answer
    answer = agent.construct_answer(results, question, time_resolution)
    print(f"STEP 6 - Final Answer: {answer}")
    
    print("=" * 60)
    return answer

def main():
    """Run comprehensive demo"""
    
    print("🤖 DATABASE REASONING AGENT DEMO")
    print("Following the STRICT 7-Step Process")
    print("=" * 80)
    print()
    
    # Initialize agent
    agent = DatabaseReasoningAgent("ravvyn.db")
    agent.current_date = date(2025, 12, 17)  # Set consistent date for demo
    
    print(f"📅 Current Date: {agent.current_date}")
    print(f"🌍 Timezone: {agent.timezone}")
    print()
    
    # Demo questions that work with the actual database
    demo_questions = [
        # Relative time queries
        "How many sheet records last 7 days?",
        "Data activity last week",
        
        # Month queries  
        "Count of chat messages in December?",
        
        # Non-time queries
        "What is the sync status?",
        "Total sheet records",
        
        # Failure cases
        "What was the revenue sometime?",  # Ambiguous
        "Show me everything",  # Too vague
    ]
    
    for i, question in enumerate(demo_questions, 1):
        print(f"\n📋 DEMO {i}/{len(demo_questions)}")
        answer = demo_step_by_step(question, agent)
        print(f"🎯 FINAL RESULT: {answer}")
        print("\n" + "🔹" * 80 + "\n")
    
    print("✅ DEMO COMPLETED!")
    print("\n🔑 KEY FEATURES DEMONSTRATED:")
    print("  • Strict 7-step validation process")
    print("  • Exact date range resolution")
    print("  • No fuzzy matching or guessing")
    print("  • Controlled failure handling")
    print("  • Works with real database schema")
    print("  • Timezone-aware processing")

if __name__ == "__main__":
    main()