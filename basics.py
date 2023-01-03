from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient

load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://szalashaska:{password}@myfirstcluster.p7nu8wj.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_string)

# List of db in cluster
dbs = client.list_database_names()
test_db = client.test

# List of collections in db
collections = test_db.list_collection_names()


def insert_test_doc():
    collection = test_db.test
    test_document = {"name": "Kamil", "type": "Test"}

    # Id have specific BSON Type (it is not just a simple int)
    inserted_id = collection.insert_one(test_document).inserted_id


# If database or collection does not exist, mongo creates it
production = client.production
person_collection = production.person_collection


def create_documents():
    first_names = ["Kamil", "Tim", "Sarah", "Jose", "Brad", "Jim"]
    last_names = ["Brown", "Smit", "Pitt", "Cooper", "Carter", "Black"]
    ages = [21, 29, 40, 60, 55, 16]
    docs = []

    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        # Inserting by one, not efficient
        # person_collection.insert_one(doc)
        docs.append(doc)

    person_collection.insert_many(docs)


printer = pprint.PrettyPrinter()


def find_all_people():
    # This will be cursor object, need to be converted to use it (list())
    people = person_collection.find()

    for person in people:
        printer.pprint(person)


def find_person(name):
    # You can specify many fields, values all values must match
    person = person_collection.find_one({"first_name": name})
    printer.pprint(person)


def count_all_people():
    # U need pass filter
    count = person_collection.count_documents(filter={})
    print("Number of people", count)


def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)


def get_age_range(min_age, max_age):

    # gte - grater than or equal
    # gt - grater than
    # lte - less or equal
    # lt - less than
    # qt - equal to

    query = {"$and": [{"age": {"$gte": min_age}}, {"age": {"$lte": max_age}}]}

    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)


def project_columns():
    # _id: 0 -> do not show id
    columns = {"_id": 0, "first_name": 1, "last_name": 1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)


def update_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    # $set - sets new field
    # $inc - increments field
    # $rename - renames field name

    all_updates = {
        "$set": {"new_field": True},
        "$inc": {"age": 1},
        "$rename": {"first_name": "first", "last_name": "last"},
    }

    person_collection.update_one({"_id": _id}, all_updates)


def update_person_by_id_delete_field(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})


def replace_one(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    new_doc = {"first_name": "Marlin", "last_name": "Drought", "age": 26}

    person_collection.replace_one({"_id": _id}, new_doc)


def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    person_collection.delete_one({"_id": _id})

    # Deletes all
    # person_collection.delete_many({})


# --------------- RELATIONSHIPS ---------------


address = {
    "_id": "63b42ef135d814fab2b2bc2b",
    "street": "Bay",
    "number": 2705,
    "city": "Los Angeles",
    "country": "United States",
    "zip": "94-107",
}


def add_address_embed(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    # $addToSet - creates array field
    person_collection.update_one({"_id": _id}, {"$addToSet": {"addresses": address}})


def add_address_relationship(person_id, new_address):
    new_address = new_address.copy()
    new_address["owner_id"] = person_id

    address_collection = production.address
    address_collection.insert_one(new_address)


add_address_relationship("63b42ef135d814fab2b2be2f", address)
