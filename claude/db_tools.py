# db_tools.py
# Defines what tools Claude can use
# Claude reads descriptions to decide which tool to call

TOOLS = [
    {
        "name": "query_athena",
        "description": """Query the Fitbit Gold layer daily summary data.
                         Use this for questions about:
                         - steps, calories, sleep, activity levels
                         - anomaly scores and fraud detection
                         - user activity trends and patterns
                         - comparing users or time periods
                         Database: fitbit_gold2
                         Table: daily_summary
                         Columns: user_id, day, total_steps, total_calories,
                                  active_minutes, sleep_minutes, avg_intensity,
                                  max_steps_in_minute, total_minutes_recorded,
                                  anomaly_score, activity_level, year, month""",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "Valid SQL query to run on Athena"
                }
            },
            "required": ["sql"]
        }
    },
    {
        "name": "get_user_rewards",
        "description": """Get a specific user's reward profile from DynamoDB.
                         Use this for questions about:
                         - user points balance
                         - badges earned
                         - workout count
                         - last activity date""",
        "input_schema": {
            "type": "object",
            "properties": {
                "user_id": {
                    "type": "string",
                    "description": "The user ID to look up e.g. USR_30c4ba1f"
                }
            },
            "required": ["user_id"]
        }
    },
    {
        "name": "get_all_rewards",
        "description": """Get all users reward profiles from DynamoDB.
                         Use this for questions about:
                         - leaderboard
                         - top users by points
                         - who earned which badges
                         - overall reward statistics""",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]
