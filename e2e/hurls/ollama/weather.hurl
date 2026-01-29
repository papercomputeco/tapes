# Ollama multi-turn conversation simulating arbitrary too calling capabilities.

POST http://localhost:8080/api/chat
{
  "model": "ministral-3:latest",
  "messages": [
    {
      "role": "user",
      "content": "what is the weather in tokyo?"
    },
    {
      "role": "assistant",
      "content": "",
      "tool_calls": [
        {
          "id": "call_abc123",
          "type": "function",
          "function": {
            "name": "get_weather",
            "arguments": {
                "city": "tokyo"
            }
          }
        }
      ]
    },
    {
      "role": "tool",
      "tool_call_id": "call_001",
      "content": "{\"temperature\": 22, \"unit\": \"celsius\", \"condition\": \"partly cloudy\", \"humidity\": 65}"
    },
    {
      "role": "assistant",
      "content": "The weather in Tokyo is currently 22°C and partly cloudy with 65% humidity."
    },
    {
      "role": "user",
      "content": "How about in New York? And compare the two for me."
    }
  ],
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "get_weather",
        "description": "Get the weather in a given city",
        "parameters": {
          "type": "object",
          "properties": {
            "city": {
              "type": "string",
              "description": "The city to get the weather for"
            }
          },
          "required": ["city"]
        }
      }
    }
  ],
  "stream": false
}

