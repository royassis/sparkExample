# Read from hive with pyhive
from pyhive import hive

# conn = hive.Connection(host="localhost", port=10000, username="YOU")
conn = hive.Connection(host="localhost", port=10000)

cursor = conn.cursor()
cursor.execute("use testdb")
cursor.execute("SELECT * FROM testtable")

for result in cursor.fetchall():
  print(result)

# Read from hive with spark