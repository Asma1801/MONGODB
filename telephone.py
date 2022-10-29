
import pymongo
import json
My_client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
db = My_client['telephone_db']
collection = db['tele_directory']
data = {
        "NAME":"Thonas",
        "Place":"Paris",
        "phone number":9028382938,
        "pincode":5167,
        "course":"Ds"}
collection.insert_one(data)


query={"NAME":"Thonas"}
update={'$set':{"qualification":"Btech","email":"Thonas23@hotmail.in"}}
collection.update_one(query,update)
update = collection.find()
for i in update:
    print(i)

collection.delete_one({"NAME":"Thonas"})
d = collection.find()
for i in d:
    print(i)
