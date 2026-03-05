LLM Agent Architecture

The system uses a multi-stage agent pipeline. Each stage has a clearly
separated responsibility.

This separation is critical because:

-   SQL generation must happen before data exists
-   Answer explanations must happen after SQL execution
-   Chart selection is a presentation concern, not a query concern

Therefore the agent operates in five sequential stages.

------------------------------------------------------------------------

Agent Execution Pipeline

    User Question
          │
          ▼
    1. Conversation / Clarification
          │
          ▼
    2. SQL Planning (LLM)
          │
          ▼
    3. SQL Execution (DB)
          │
          ▼
    4. Presentation Planning (charts)
          │
          ▼
    5. Result Explanation (LLM)
          │
          ▼
    Response to user

------------------------------------------------------------------------

1. Conversation / Clarification

Purpose:

Resolve ambiguity in the user question before SQL generation.

Typical ambiguities:

-   time period
-   aggregation level
-   top N
-   metric definitions
-   filters

Example:

User question:

    Show fund exposure by asset category

Possible interpretations:

-   latest report date
-   last year
-   last 4 quarters
-   specific fund vs all funds

The system may ask clarification questions.

------------------------------------------------------------------------

Implementation Options

Two possible approaches exist.

Option A (recommended): LLM Clarification Agent

Use a lightweight LLM prompt to detect ambiguity and generate
clarification questions.

Advantages:

-   flexible
-   handles natural language well
-   easier to extend

Option B: Rule-based intent detection

Detect patterns using predefined rules:

-   keywords for time
-   keywords for aggregation
-   keywords for metrics

Advantages:

-   deterministic
-   faster

Disadvantages:

-   brittle
-   limited coverage

------------------------------------------------------------------------

Recommended Strategy

Hybrid approach:

    Rules first
    LLM fallback

1.  Try rule-based interpretation
2.  If ambiguity detected → ask LLM for clarification

------------------------------------------------------------------------

Clarification Prompt (LLM)

Example system prompt:

    You are an assistant helping clarify analytical questions.

    Your task is NOT to generate SQL.

    Your task is to detect ambiguity in the user's question.

    If the question is ambiguous, ask a short clarification question.

    If the question is sufficiently clear, respond with:

    {
      "status": "clear"
    }

    If clarification is required:

    {
      "status": "clarify",
      "question": "..."
    }

Example output:

    {
      "status": "clarify",
      "question": "Do you want the exposure for the most recent reporting date or over a time period?"
    }

------------------------------------------------------------------------

2. SQL Planning (LLM)

This stage converts the clarified intent into SQL.

The LLM receives:

-   user question
-   schema context
-   SQL rules
-   domain rules

The output is a structured SQL plan.

Important:

This stage must not produce explanations or summaries.

Only query planning.

------------------------------------------------------------------------

SQL Planner Prompt

System prompt example:

    You are a SQL planner for Postgres.

    Your task is to generate a SQL query that answers the user's question.

    Rules:

    - Only SELECT or WITH queries are allowed.
    - Only tables from schema analytics.* may be used.
    - Use explicit joins.
    - Use CTEs instead of window functions.
    - Always include LIMIT <= 500 unless results are guaranteed small.

    Return ONLY JSON:

    {
      "sql": "...",
      "params": {},
      "result_shape": "...",
      "assumptions": []
    }

The SQL is then validated before execution.

------------------------------------------------------------------------

3. SQL Execution

The generated SQL is executed against PostgreSQL.

Execution safeguards:

-   statement timeout
-   SQL validation
-   SELECT-only enforcement
-   schema restrictions

Output format:

    {
      "columns": [...],
      "row_count": N,
      "rows": [...]
    }

------------------------------------------------------------------------

4. Presentation Planning (Chart Selection)

Chart type is not determined by the user query.

Instead a default chart is selected based on result structure.

Example mapping:

  Result Shape   Chart
  -------------- ------------
  timeseries     line chart
  ranking        bar chart
  table          table
  distribution   histogram

Example logic:

    if result_shape == "timeseries":
        chart = line
    elif result_shape == "ranking":
        chart = bar
    else:
        chart = table

Users may change charts in the frontend.

------------------------------------------------------------------------

5. Result Explanation (LLM)

This stage generates:

-   answer summary
-   suggested follow-up questions

Unlike SQL planning, this step has access to the data.

Inputs:

-   user question
-   SQL query
-   result metadata
-   sample rows

------------------------------------------------------------------------

Explanation Prompt

Example system prompt:

    You are a financial data analyst.

    You are given:

    - a user question
    - a SQL query
    - the resulting data

    Write a short answer summarizing the result.

    Guidelines:

    - Use plain language
    - Do not invent information not present in the data
    - Mention key trends or largest values

Example output:

    {
      "answer_brief": "...",
      "followups": [
        "...",
        "..."
      ]
    }

------------------------------------------------------------------------

Final Response Format

The API response combines all stages.

Example:

    {
      "plan": {
        "sql": "...",
        "params": {},
        "assumptions": []
      },
      "data": {
        "columns": [...],
        "rows": [...]
      },
      "chart": {
        "type": "line",
        "x": "...",
        "y": "...",
        "series": "..."
      },
      "answer_brief": "...",
      "followups": [...]
    }

------------------------------------------------------------------------

Why This Architecture

This design avoids several common LLM errors:

  Problem                            Solution
  ---------------------------------- --------------------------------------------
  LLM invents explanations           explanations generated after SQL execution
  LLM guesses chart types            chart chosen deterministically
  LLM generates invalid SQL          SQL validation layer
  LLM mixes planning and narration   responsibilities separated

------------------------------------------------------------------------

Future Improvements

Planned enhancements:

-   query caching
-   vector retrieval for schema context
-   query memory
-   user preference learning
-   interactive BI-style filtering
