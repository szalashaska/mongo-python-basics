from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
import json


printer = pprint.PrettyPrinter()
load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")
connection_string = f"mongodb+srv://szalashaska:{password}@myfirstcluster.p7nu8wj.mongodb.net/?retryWrites=true&w=majority&authSource=admin"
client = MongoClient(connection_string)
jepardy = client.jepardy
question = jepardy.jepardy


# We have created search index in our cluster with name: language_search
def normal_matching(query):
    result = question.aggregate(
        [
            {
                "$search": {
                    "index": "language_search",
                    "text": {
                        "query": query,
                        "path": "category",
                    },
                },
            }
        ]
    )

    printer.pprint(list(result))


# You can define how specific fuzzy matching can be in "fuzzy" object
def fuzzy_matching(query):
    result = question.aggregate(
        [
            {
                "$search": {
                    "index": "language_search",
                    "text": {
                        "query": query,
                        "path": "category",
                        "fuzzy": {},
                    },
                },
            }
        ]
    )

    printer.pprint(list(result))


# "synonyms": "mapping" matches the configuration of synonyms search -> "name": "mapping"
def synonym_search(query):
    result = question.aggregate(
        [
            {
                "$search": {
                    "index": "language_search",
                    "text": {
                        "query": query,
                        "path": "category",
                        "synonyms": "mapping",
                    },
                },
            }
        ]
    )

    printer.pprint(list(result))


# "tokenOrder": "sequential" -> order of query matters || "any"
# Make sure to save changes in your index, and wait for them to be aplied
def autocomplete(query):
    result = question.aggregate(
        [
            {
                "$search": {
                    "index": "language_search",
                    "autocomplete": {
                        "query": query,
                        "path": "question",
                        "tokenOrder": "sequential",
                        "fuzzy": {},
                    },
                },
            },
            {
                "$project": {
                    "_id": 0,
                    "question": 1,
                },
            },
        ]
    )

    printer.pprint(list(result))


def compound_search():
    result = question.aggregate(
        [
            {
                "$search": {
                    "index": "language_search",
                    "compound": {
                        "must": [
                            {
                                "text": {
                                    "query": ["COMPUTER", "CODING"],
                                    "path": "category",
                                }
                            }
                        ],
                        "mustNot": [
                            {
                                "text": {
                                    "query": "codes",
                                    "path": "category",
                                }
                            }
                        ],
                        "should": [
                            {
                                "text": {
                                    "query": "application",
                                    "path": "answer",
                                }
                            }
                        ],
                    },
                },
            },
            {
                "$project": {
                    "_id": 0,
                    "question": 1,
                    "answer": 1,
                    "category": 1,
                    "score": {"$meta": "searchScore"},
                },
            },
        ]
    )

    printer.pprint(list(result))


# Prioritize questions appering in later rounds
# "boost" multiplies score value by "value"
def relevance():
    result = question.aggregate(
        [
            {
                "$search": {
                    "index": "language_search",
                    "compound": {
                        "must": [
                            {
                                "text": {
                                    "query": "geography",
                                    "path": "category",
                                }
                            }
                        ],
                        "should": [
                            {
                                "text": {
                                    "query": "Final Jeopardy",
                                    "path": "round",
                                    "score": {"boost": {"value": 3.0}},
                                }
                            },
                            {
                                "text": {
                                    "query": "Double Jeopardy",
                                    "path": "round",
                                    "score": {"boost": {"value": 2.0}},
                                }
                            },
                        ],
                    },
                },
            },
            {
                "$project": {
                    "_id": 0,
                    "question": 1,
                    "answer": 1,
                    "round": 1,
                    "category": 1,
                    "score": {"$meta": "searchScore"},
                },
            },
            {
                "$limit": 100,
            },
        ]
    )

    printer.pprint(list(result))


relevance()
