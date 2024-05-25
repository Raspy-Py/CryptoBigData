from pymongo import MongoClient

client = MongoClient("mongodb://mongodb:27017")
db = client.crypto_db
number_of_entries_to_output = 10

while True:
    collections = []
    print("Choose collection: ")
    for i, collection in enumerate(db.list_collection_names()):
        print(f"\t{i+1}. {collection}")
        collections.append(collection)
    print("NOTE: This will only print first ", number_of_entries_to_output, " entries from the collection.")
    choice = input("")
    if choice == "exit":
        break
    choice = int(choice)
    if choice < 1 or choice > len(collections):
        print("Invalid choice. Try again.")
        continue
    
    for doc in db[collections[choice-1]].find()[:number_of_entries_to_output]:
        print(doc)
