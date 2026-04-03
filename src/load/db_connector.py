import psycopg2


def get_db_connection(host: str, database: str, user: str, password: str, port: int):
    conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
    cursor = conn.cursor()
    return cursor, conn
