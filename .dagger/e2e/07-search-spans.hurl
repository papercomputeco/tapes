# Span semantic search round trip: the query embeds via Ollama, runs
# vector similarity over the span embeddings written by the embed-spans
# backfill, and returns the matched span with its trace/turn context.

GET http://tapes-api:8081/v1/search/spans?query=retry%20backoff%20configuration&top_k=3
HTTP 200
[Asserts]
jsonpath "$.count" >= 1
jsonpath "$.results[0].trace_id" startsWith "trc_"
jsonpath "$.results[0].span_id" startsWith "llm_"
jsonpath "$.results[0].user_prompt" contains "retry backoff"
jsonpath "$.results[0].snippet" contains "backoff"
jsonpath "$.results[0].session_id" isString
jsonpath "$.results[0].score" isNumber
