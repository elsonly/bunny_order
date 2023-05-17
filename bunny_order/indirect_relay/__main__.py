from redis import Redis
import psycopg2
from datetime import datetime
import time

from bunny_order.config import Config

if __name__ == "__main__":
    redis_cli = Redis(
        Config.RS_HOST, Config.RS_PORT, db=Config.RS_DB, password=Config.RS_PASSWORD
    )

    conn = psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        database=Config.DB_DATABASE,
    )
    print("start indirect")
    while True:
        try:
            now = datetime.now().isoformat()
            redis_cli.set("test", now)
            print("redis", redis_cli.get("test"))
            with conn.cursor() as cur:
                cur.execute(f"insert into public.fortest(msg) values('{now}');")
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(f"select * from public.fortest limit 1;")
                print("db", cur.fetchall())

        except:
            conn.rollback()

        time.sleep(10)
