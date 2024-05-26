from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])
session = cluster.connect('crypto_keyspace')
number_of_entries_to_output = 10

while True:
    tables = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='crypto_keyspace';")
    table_list = [row.table_name for row in tables]

    print("Choose table: ")
    for i, table in enumerate(table_list):
        print(f"\t{i+1}. {table}")

    print("NOTE: This will only print first ", number_of_entries_to_output, " entries from the table.")
    choice = input("")
    if choice == "exit":
        break
    choice = int(choice)
    if choice < 1 or choice > len(table_list):
        print("Invalid choice. Try again.")
        continue

    selected_table = table_list[choice - 1]
    query = f"SELECT * FROM {selected_table} LIMIT {number_of_entries_to_output};"
    rows = session.execute(query)
    
    for row in rows:
        print(row)
