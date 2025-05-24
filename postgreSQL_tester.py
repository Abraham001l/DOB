import psycopg

# Establish the connection
conn = psycopg.connect(
    dbname="",
    user="",
    password="",
    host="localhost",
    port=5432  # Default PostgreSQL port
)

# Create a cursor and execute a query
cur = conn.cursor()
# cur.execute("""SELECT * FROM weather;""")
# print(cur.fetchall())

# Clean up
cur.close()
conn.close()
