from pymongo import MongoClient
from clickhouse_driver import Client
import json
from pprint import pprint

def store_into_mongodb(all_books): 
    client = MongoClient('mongodb://localhost:27017') 
    client.drop_database('bookstore')
    bookstore = client["bookstore"]
    books = bookstore["books"]
    books.insert_many(all_books)
    return books

def select_from_mongodb():
    client = MongoClient('mongodb://localhost:27017') 
    bookstore = client["bookstore"]
    books = bookstore["books"]
    query = {'name': 'Beyond Good and Evil'}
    pprint(books.find_one(query))
    query = {'price': {'$gt' : 50, '$lt' : 80}}
    projection = {"_id" : 0, "name" : 1, 'price': 1}
    for book in books.find(query, projection):
        pprint(book)
    projection = {"_id" : 0, "name" : 1}
    query = {"name" : {"$regex" : "[Mm]urder"}}
    for book in books.find(query, projection):
        pprint(book)


def store_into_clickhouse(all_books):
    client = Client(host = 'localhost')
    client.execute('DROP DATABASE IF EXISTS bookstore;')
    client.execute('CREATE DATABASE bookstore;')
    client.execute(
        '''
        CREATE TABLE bookstore.books (
            `id` UInt64, 
            `name` String, 
            `price` Float32, 
            `available` Int32, 
            `desc` String) 
            ENGINE = MergeTree() 
            PRIMARY KEY `id`;
        '''
    )
    id = 0
    for book in all_books:
        client.execute('INSERT INTO bookstore.books (id, name, price, available, desc) VALUES', 
                       [(id, book['name'], book['price'], book['available'], book['desc'])]) 
        id += 1


# load data as JSON doc 
with open('books.json', 'r') as f:   
    all_books = json.load(fp=f)

store_into_mongodb(all_books)
select_from_mongodb()
store_into_clickhouse(all_books)