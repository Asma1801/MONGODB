import pymongo
import json
import pandas as pd
from bson.objectid import ObjectId
My_client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
#CREATING A DATABASE
Database = My_client["student_db"]
Databases = My_client.list_database_names()
print(Databases)
if 'student_db' in Databases:
    print("Database exists")
else:
    print("No Database exists")
#CREAING COLLECTION
collection = Database["student_details"]
collections = Database.list_collection_names()
print(collections)
if "student_details" in collections:
    print("collection exists")
else:
    print("collection don't exists")
#CREATING DOCUMENT
'''with open("students (1).json") as file:
upload = json.load(file)
document = collection.insert_many(upload)
data = collection.find_one()
print(data)'''
print("Question : 1")
output = collection.aggregate ([{'$group':
                                {'_id': '$_id',
                                 "scores":{"$first":"$scores"},
                                'data': {'$push': "$$ROOT"}}},
                                        {'$unwind': "$data"},
                                        {'$match': {"data.scores.type": "exam"}},
                                        {'$sort': {"data.scores.score": -1}},
                                        {'$project':
                                        {'_id': 1, 'name': "$data.name", 'scores': "$scores"}},
                                        {'$limit': 1}])
for i in output:
    print(i)

print("Question : 2")
below_average = collection.find({"scores": {"$elemMatch": {"type": "exam", "score": {"$lt": 40}}}})
for i in below_average:
    print(i)

print("Question : 3")

fail = collection.find({"$and": [{"scores": {"$elemMatch": {"type": "exam", "score": {"$lt": 40}}}},
                                         {"scores": {"$elemMatch": {"type": "quiz", "score": {"$lt": 40}}}},
                                         {"scores": {"$elemMatch": {"type": "homework", "score": {"$lt": 40}}}},
                                         ]},{"Assign(P/F)":"Fail"})
Pass = collection.find({"$and": [{"scores": {"$elemMatch": {"type": "exam", "score": {"$gt": 40}}}},
                              {"scores": {"$elemMatch": {"type": "quiz", "score": {"$gt": 40}}}},
                              {"scores": {"$elemMatch": {"type": "homework", "score": {"$gt": 40}}}},
                              ]},{"Assign(P/F)":"Pass"}
                    )
for i in fail:
    print(i)
for i in Pass:
    print(i)

print("Question : 4")

new_collection = Database['sum_avg']

a = collection.aggregate([{"$unwind":"$scores"},
                          {"$group":{"_id":"$_id",
                                     "scores":{"$push":"$scores"},
                                     "Total": {"$sum":"$scores.score"},
                                     "avg":{"$avg":"$scores.score"}}},
                          {"$sort":{"_id":1}}])
for i in a:
    y = json.dumps(i)
    print(y)
    '''with open('new_file.json', 'a+') as f:
        a = f.writelines(y + ',' + '\n')
file = open("new_file.json")
upload = json.load(file)
document = new_collection.insert_many(upload)
data = new_collection.find_one()
print(data)'''

print("Question : 5")
new_collection1 = Database['avg below  and scores above 40% ']
below = new_collection.find({"$and":[{"scores": {"$elemMatch": {"type": "exam", "score": {"$gt": 40}}}},
                                     {"scores": {"$elemMatch": {"type": "quiz", "score": {"$gt": 40}}}},
                                     {"scores": {"$elemMatch": {"type": "homework", "score": {"$gt": 40}}}}]})
avg = new_collection.find({"avg":{"$lt": 40}})
for i in below:
    x = json.dumps(i)
    print(x)
    '''with open("new.json",'a+') as f:
        b = f.writelines(x+','+'\n')'''
print("%%%%%%%%%%%%%%")
for j in avg:
    y = json.dumps(j)
    print(y)
    '''with open('new.json', 'a+') as f:
        a = f.writelines(y +','+'\n')'''
file = open("new.json")
upload = json.load(file)
document = new_collection1.insert_many(upload)
data = new_collection1.find_one()
print(data)

print("Question : 6")
new_collection2 = Database['fail']
fail = collection.find({"$and": [{"scores": {"$elemMatch": {"type": "exam", "score": {"$lt": 40}}}},
                                 {"scores": {"$elemMatch": {"type": "quiz", "score": {"$lt": 40}}}},
                                 {"scores": {"$elemMatch": {"type": "homework", "score": {"$lt": 40}}}}]})
for i in fail:
    y = json.dumps(i)
    print(y)
    '''with open('fail.json', 'a+') as f:
            a = f.writelines(y + ',' + '\n')

file = open("fail.json")
upload = json.load(file)
document = new_collection2.insert_many(upload)
data = new_collection2.find_one()
print(data)'''

print("Question : 7 ")
new_collection3 = Database['pass']
Pass = collection.find({"$and": [{"scores": {"$elemMatch": {"type": "exam", "score": {"$gt": 40}}}},
                                 {"scores": {"$elemMatch": {"type": "quiz", "score": {"$gt": 40}}}},
                                 {"scores": {"$elemMatch": {"type": "homework", "score": {"$gt": 40}}}}]})
for i in Pass:
    y = json.dumps(i)
    print(y)
    '''with open('pass.json', 'a+') as f:
                a = f.writelines(y + ',' + '\n')

file = open("pass.json")
upload = json.load(file)
document = new_collection3.insert_many(upload)
data = new_collection3.find_one()
print(data)'''