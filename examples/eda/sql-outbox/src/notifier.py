import psycopg


def run(conn: psycopg.Connection) -> None:
    # LISTEN ghost_channel inside a comment is not a consumer.
    conn.execute("LISTEN stock_changed")
    for notice in conn.notifies():
        print(notice.payload)
