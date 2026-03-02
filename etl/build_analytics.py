from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.environ["DATABASE_URL"])

def main():
    sql = open("sql/analytics_build.sql", "r", encoding="utf-8").read()
    with engine.begin() as conn:
        # POC: no timeout
        conn.execute(text("SET statement_timeout = 0;"))
        conn.execute(text(sql))
    print("OK: analytics populated")

if __name__ == "__main__":
    main()