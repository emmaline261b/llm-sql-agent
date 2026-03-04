#LLM SQL Agent (Local POC)

Local proof-of-concept system:

-   FastAPI API
-   PostgreSQL (Docker)
-   Real public data (SEC N-PORT)
-   Analytics layer (mini data warehouse)
-   Local LLM (Ollama)
-   /chat endpoint converting natural language → structured SQL plan
    (JSON)

The system runs fully locally.

------------------------------------------------------------------------

1. Requirements

-   macOS (Apple Silicon tested)
-   Homebrew
-   Docker Desktop
-   Python 3.11
-   Ollama

------------------------------------------------------------------------

2. Python Environment

2.1 Create virtual environment

    /opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv
    source .venv/bin/activate
    python --version

2.2 Install dependencies

    pip install fastapi uvicorn sqlalchemy psycopg[binary] python-dotenv httpx jsonschema pydantic alembic

------------------------------------------------------------------------

3. PostgreSQL (Docker)

3.1 docker-compose.yml

    services:
      db:
        image: postgres:16
        container_name: llm_sql_agent_db
        environment:
          POSTGRES_USER: app
          POSTGRES_PASSWORD: app
          POSTGRES_DB: app
        ports:
          - "5432:5432"
        volumes:
          - pgdata:/var/lib/postgresql/data

    volumes:
      pgdata:

3.2 Start database

    docker compose up -d
    docker ps

------------------------------------------------------------------------

4. Environment Variables

Create .env (do not commit):

    DATABASE_URL=postgresql+psycopg://app:app@localhost:5432/app

------------------------------------------------------------------------

5. Project Structure

    app.py
    db/
        engine.py
        schema.py
    etl/
    llm/
        client_ollama.py
        models.py
        planner.py
        validation.py
    semantic/
        llm_output.schema.json
        sql_planner.system.txt
    sql/
        analytics_build.sql
    docker-compose.yml

------------------------------------------------------------------------

6. Load SEC N-PORT Data

6.1 Create raw schema

    CREATE SCHEMA raw_nport;

6.2 Run ETL pipeline

    python etl/load_nport_raw.py

Verify:

    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'raw_nport';

------------------------------------------------------------------------

7. Create Analytics Layer

7.1 Initialize Alembic

    alembic init alembic

Edit alembic.ini:

    sqlalchemy.url = postgresql+psycopg://app:app@localhost:5432/app

7.2 Create migration

    alembic revision -m "create analytics schema"
    alembic upgrade head

------------------------------------------------------------------------

8. Build Analytics Tables

    python etl/build_analytics.py

------------------------------------------------------------------------

9. Install and Configure Ollama

    ollama --version
    ollama pull qwen2.5:7b-instruct
    curl http://localhost:11434/api/tags

------------------------------------------------------------------------

10. Run FastAPI

    uvicorn app:app --reload

Open:

http://127.0.0.1:8000/docs

------------------------------------------------------------------------

11. Test Chat Endpoint

    curl -X POST http://127.0.0.1:8000/chat   -H "Content-Type: application/json"   -d '{"question":"Show fund exposure by asset_category over time"}'

------------------------------------------------------------------------
------------------------------------------------------------------------
------------------------------------------------------------------------

#LLM SQL Agent — Run Instructions (Local)

This document describes how to start the application locally step by
step.

------------------------------------------------------------------------

0. Go to project directory

    cd /Users/malgosialasota/PycharmProjects/llm-sql-agent

------------------------------------------------------------------------

1. Activate Python environment

If .venv already exists:

    source .venv/bin/activate
    python --version

If starting from scratch:

    /opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv
    source .venv/bin/activate
    pip install fastapi uvicorn sqlalchemy psycopg[binary] python-dotenv httpx jsonschema pydantic alembic

------------------------------------------------------------------------

2. Verify .env file

Check that .env exists:

    ls -la .env

It must contain:

    DATABASE_URL=postgresql+psycopg://app:app@localhost:5432/app

------------------------------------------------------------------------

3. Start PostgreSQL (Docker)

    docker compose up -d
    docker ps

Optional DB connectivity check:

    python -c "from dotenv import load_dotenv; import os; from sqlalchemy import create_engine, text; load_dotenv(); e=create_engine(os.environ['DATABASE_URL']); print(e.connect().execute(text('select 1')).scalar())"

------------------------------------------------------------------------

4. Apply database migrations

    alembic upgrade head

------------------------------------------------------------------------

5. (Optional) Run ETL and build analytics

If raw data is not loaded:

    python etl/load_nport_raw.py

If analytics tables need rebuilding:

    python etl/build_analytics.py

------------------------------------------------------------------------

6. Start Ollama (LLM)

In a separate terminal:

    ollama serve

Verify:

    curl -s http://localhost:11434/api/tags | head

If model is missing:

    ollama pull qwen2.5:7b-instruct

------------------------------------------------------------------------

7. Start FastAPI

In another terminal (with active .venv):

    uvicorn app:app --reload

------------------------------------------------------------------------

8. Test the application

Swagger UI:

http://127.0.0.1:8000/docs

Health check:

    curl http://127.0.0.1:8000/health

Database check:

    curl http://127.0.0.1:8000/db-check

LLM planner test:

    curl -X POST http://127.0.0.1:8000/chat   -H "Content-Type: application/json"   -d '{"question":"Pokaż ekspozycję funduszy po asset_category w czasie"}'

------------------------------------------------------------------------

Minimal startup sequence (when everything is prepared)

1.  source .venv/bin/activate
2.  docker compose up -d
3.  ollama serve
4.  uvicorn app:app –reload
5.  Open /docs and test /chat
