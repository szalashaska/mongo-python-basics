from dotenv import load_dotenv, find_dotenv
from datetime import datetime as dt
import os
import pprint
from pymongo import MongoClient


printer = pprint.PrettyPrinter()
load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://szalashaska:{password}@myfirstcluster.p7nu8wj.mongodb.net/?retryWrites=true&w=majority&authSource=admin"
client = MongoClient(connection_string)
production = client.production


def create_book_collection():
    book_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "Book Object Validation",
            "required": ["title", "authors", "publish_date", "type", "copies"],
            "properties": {
                "title": {
                    "bsonType": "string",
                    "description": "must be a string and is required",
                },
                "authors": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "objectId",
                        "description": "must be an objectId and is required",
                    },
                },
                "publish_date": {
                    "bsonType": "date",
                    "description": "must be a date in and is required",
                },
                "type": {
                    "enum": ["Fiction", "Non-Fiction"],
                    "description": "must be one of enum values and is required",
                },
                "copies": {
                    "bsonType": "int",
                    "minimum": 0,
                    "description": "'year' must be an integer grater than 0 and is required",
                },
            },
        }
    }
    try:
        production.create_collection("book")
    except Exception as e:
        print(e)

    production.command("collMod", "book", validator=book_validator)


def create_author_collection():
    author_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "Book Object Validation",
            "required": ["first_name", "last_name", "date_of_birth"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required",
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required",
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "must be a date in and is required",
                },
            },
        }
    }
    try:
        production.create_collection("author")
    except Exception as e:
        print(e)

    production.command("collMod", "author", validator=author_validator)


def create_data():
    authors = [
        {
            "first_name": "Tim",
            "last_name": "Roller",
            "date_of_birth": dt(2000, 7, 18),
        },
        {
            "first_name": "John",
            "last_name": "Potter",
            "date_of_birth": dt(1992, 1, 11),
        },
        {
            "first_name": "Alice",
            "last_name": "Wool",
            "date_of_birth": dt(1994, 5, 5),
        },
        {
            "first_name": "Jo",
            "last_name": "Fisher",
            "date_of_birth": dt(1962, 2, 7),
        },
        {
            "first_name": "Andy",
            "last_name": "Butcher",
            "date_of_birth": dt(1978, 9, 29),
        },
    ]
    author_collection = production.author
    author_ids = author_collection.insert_many(authors).inserted_ids

    books = [
        {
            "title": "Timon and Pumba",
            "authors": [author_ids[0]],
            "publish_date": dt.today(),
            "type": "Non-Fiction",
            "copies": 50,
        },
        {
            "title": "Gone with the wind",
            "authors": [author_ids[1]],
            "publish_date": dt(1999, 10, 11),
            "type": "Non-Fiction",
            "copies": 30,
        },
        {
            "title": "Avatar",
            "authors": [author_ids[2]],
            "publish_date": dt(2010, 5, 25),
            "type": "Fiction",
            "copies": 20,
        },
        {
            "title": "Inception",
            "authors": [author_ids[0], author_ids[3]],
            "publish_date": dt(1980, 10, 11),
            "type": "Fiction",
            "copies": 5,
        },
    ]

    book_collection = production.book
    book_collection.insert_many(books)


# Regular expressions
books_containing_a = production.book.find({"title": {"$regex": "a{1}"}})
# printer.pprint(list(books_containing_a))


# Join data, returns authors with books, that are asigned to them
authors_and_books = production.author.aggregate(
    [
        {
            "$lookup": {
                "from": "book",
                "localField": "_id",
                "foreignField": "authors",
                "as": "books",
            }
        },
    ]
)
# printer.pprint(list(authors_and_books))


# Add field with book count
# 1. Join table
# 2. Add field
# 3. Show only specific fields
authors_books_count = production.author.aggregate(
    [
        {
            "$lookup": {
                "from": "book",
                "localField": "_id",
                "foreignField": "authors",
                "as": "books",
            }
        },
        {
            "$addFields": {
                "total_books": {"$size": "$books"},
            }
        },
        {
            "$project": {"first_name": 1, "last_name": 1, "total_books": 1, "_id": 0},
        },
    ]
)

# printer.pprint(list(authors_books_count))

# Shows books with old authors
# 1. Join table
# 2. Change authors field to show age, map to calculate it
# 3. Show only objecs that match condition
# 4. Sort results
books_with_old_authors = production.book.aggregate(
    [
        {
            "$lookup": {
                "from": "author",
                "localField": "authors",
                "foreignField": "_id",
                "as": "authors",
            }
        },
        {
            "$set": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "in": {
                            "age": {
                                "$dateDiff": {
                                    "startDate": "$$this.date_of_birth",
                                    "endDate": "$$NOW",
                                    "unit": "year",
                                }
                            },
                            "first_name": "$$this.first_name",
                            "last_name": "$$this.last_name",
                        },
                    }
                }
            }
        },
        {
            "$match": {
                "$and": [
                    {"authors.age": {"$gte": 25}},
                    {"authors.age": {"$lte": 55}},
                ],
            }
        },
        {
            "$sort": {
                "age": 1,
            },
        },
    ]
)

# printer.pprint(list(books_with_old_authors))

import pyarrow
from pymongoarrow.api import Schema
from pymongoarrow.monkey import patch_all
import pymongoarrow as pma
from bson import ObjectId

patch_all()

author = Schema(
    {
        "_id": ObjectId,
        "first_name": pyarrow.string(),
        "last_name": pyarrow.string(),
        "date_of_birth": dt,
    }
)
df = production.author.find_pandas_all({}, schema=author)
# print(df.head())

arrow_table = production.author.find_arrow_all({}, schema=author)
# print(arrow_table)

nparrays = production.author.find_numpy_all({}, schema=author)
print(nparrays)
