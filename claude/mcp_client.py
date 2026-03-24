# mcp_client.py
# Main file - connects Claude to your AWS data
# Run this file to ask Claude questions

import anthropic
import json
import os
from db_tools import TOOLS
from tool_exec import execute_tool

# ─────────────────────────────────────
# SETUP
# ─────────────────────────────────────
# Set your API key:
# Windows: set ANTHROPIC_API_KEY=your-key
# Mac/Linux: export ANTHROPIC_API_KEY=your-key

client = anthropic.Anthropic(
    api_key=os.environ.get("ANTHROPIC_API_KEY")
)

# ─────────────────────────────────────
# SYSTEM PROMPT
# Tells Claude who it is and what it knows
# ─────────────────────────────────────
SYSTEM_PROMPT = """
You are a Fitbit Wellness Data Analyst AI assistant.

You have access to real Fitbit user data through tools:
- query_athena: Query Gold layer data (steps, calories, anomalies)
- get_user_rewards: Get specific user points and badges
- get_all_rewards: Get all users leaderboard

Your data contains:
- 30 Fitbit users
- Daily activity summaries (steps, calories, sleep, intensity)
- Anomaly scores (0.8+ = suspicious/fraud)
- Activity levels: HIGH (10K+ steps), MEDIUM (5K-10K), LOW (1K-5K), SEDENTARY (0)
- User reward points and badges from workout completion

When answering:
1. Always use tools to get real data
2. Explain anomalies clearly with specific numbers
3. Give business insights, not just raw data
4. Be concise but informative
"""

# ─────────────────────────────────────
# MAIN FUNCTION
# ─────────────────────────────────────
def ask_claude(user_question):
    """
    Send a question to Claude
    Claude will use tools to get data
    Returns Claude's final answer
    """
    print("\n" + "="*60)
    print(f"QUESTION: {user_question}")
    print("="*60)

    # Start conversation
    messages = [
        {"role": "user", "content": user_question}
    ]

    # ─────────────────────────────────
    # ROUND 1: Send to Claude
    # ─────────────────────────────────
    response = client.messages.create(
        model      = "claude-opus-4-5",
        max_tokens = 1024,
        system     = SYSTEM_PROMPT,
        tools      = TOOLS,
        messages   = messages
    )

    print(f"\nClaude stop reason: {response.stop_reason}")

    # ─────────────────────────────────
    # LOOP: Keep going until Claude
    # is done using tools
    # ─────────────────────────────────
    while response.stop_reason == "tool_use":

        # Find the tool Claude wants to use
        tool_use_block = next(
            block for block in response.content
            if block.type == "tool_use"
        )

        tool_name  = tool_use_block.name
        tool_input = tool_use_block.input
        tool_id    = tool_use_block.id

        print(f"\n🤖 Claude wants to use: {tool_name}")

        # Execute the tool
        tool_result = execute_tool(tool_name, tool_input)

        print(f"✅ Tool result preview: {str(tool_result)[:200]}...")

        # Add Claude's response + tool result to conversation
        messages = [
            # Original question
            {"role": "user", "content": user_question},

            # Claude's tool call
            {"role": "assistant", "content": response.content},

            # Tool result
            {
                "role": "user",
                "content": [
                    {
                        "type":        "tool_result",
                        "tool_use_id": tool_id,
                        "content":     json.dumps(
                                           tool_result,
                                           default=str
                                       )
                    }
                ]
            }
        ]

        # Send back to Claude with tool result
        response = client.messages.create(
            model      = "claude-opus-4-5",
            max_tokens = 1024,
            system     = SYSTEM_PROMPT,
            tools      = TOOLS,
            messages   = messages
        )

        print(f"Claude stop reason: {response.stop_reason}")

    # ─────────────────────────────────
    # Get Claude's final text answer
    # ─────────────────────────────────
    final_answer = next(
        block.text
        for block in response.content
        if hasattr(block, 'text')
    )

    print(f"\n{'='*60}")
    print("CLAUDE'S ANSWER:")
    print('='*60)
    print(final_answer)
    print('='*60)

    return final_answer


# ─────────────────────────────────────
# RUN TESTS
# ─────────────────────────────────────
if __name__ == "__main__":

    print("🚀 Fitbit Claude AI Starting...")
    print("Connecting to your AWS data...\n")

    # Test 1 — Fraud explanation
    ask_claude(
        "Why was USR_30c4ba1f flagged as fraud? "
        "Explain with specific numbers."
    )

    # Test 2 — Top active users
    ask_claude(
        "Who are the top 5 most active users "
        "based on total steps?"
    )

    # Test 3 — Reward leaderboard
    ask_claude(
        "Show me the reward points leaderboard. "
        "Who has the most points and badges?"
    )

    # Test 4 — Business insight
    ask_claude(
        "What percentage of users are sedentary? "
        "Give me a wellness summary."
    )

    # Test 5 — Specific user
    ask_claude(
        "Give me a complete profile of USR_test123 "
        "including their activity and rewards."
    )
