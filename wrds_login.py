import wrds

conn = wrds.Connection(wrds_username='dachxiu')
conn.create_pgpass_file()