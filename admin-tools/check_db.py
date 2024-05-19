from pymongo import MongoClient

client = MongoClient("mongodb://mongodb:27017")
db = client.crypto_db

while True:
    collections = []
    print("Choose collection: ")
    for i, collection in enumerate(db.list_collection_names()):
        print(f"\t{i+1}. {collection}")
        collections.append(collection)
    choice = input("")
    if choice == "exit":
        break
    choice = int(choice)
    if choice < 1 or choice > len(collections):
        print("Invalid choice. Try again.")
        continue
    
    for doc in db[collections[choice-1]].find()[:10]:
        print(doc)
    print("")

exit()

db = client.crypto_db

# Verify connection
try:
    client.admin.command('ping')
    print("Connected to MongoDB")
except Exception as e:
    print(f"Failed to connect to MongoDB: {e}")

print("Contents of transactions_per_hour:")
for doc in db.transactions_per_hour.find():
    print(doc)

print("\nContents of aggregated_transactions:")
for doc in db.aggregated_transactions.find():
    print(doc)

print("\nContents of raw transactions:")
for doc in db.raw_transactions.find():
    #print(doc)
    pass


# List found databases
print("\nDatabases:")
for db_name in client.list_database_names():
    print(db_name)

# List found collections in the crypto_db database
print("\nCollections in 'crypto_db':")
for collection_name in db.list_collection_names():
    print(collection_name)