import sys
from pathlib import Path

import pandas as pd
from pymongo import MongoClient, ASCENDING, HASHED


def menu(collection):
    print("\n         MENU        ")
    print("1. Search a record by ID")
    print("2. Search by Country key (with b-tree index)")
    print("3. Search by YearsCoding key (with hash index)")
    print("4. Analytics")
    print("5. Exit")

    choice = int(input("\nYour option: "))
    if choice == 1:
        record = collection.find_one({"Respondent": input("Type ID value: ")})
        print(record)

    elif choice == 2:
        records = collection.find({"Country": input("Type Country value: ")})
        print(records)

    elif choice == 3:
        records = collection.find({"YearsCoding": input("Type YearsCoding value: ")})
        print(records)

    elif choice == 4:
        print(f"1. How many brazilians answer the SO Survey? {collection.count_documents({'Country': 'Brazil'})}")

        result = list(collection.aggregate([
            {'$group': {'_id': '$Country', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]))[0]
        print(f"2. Which country answered the most to the survey? {result['_id']} with {result['count']} answers")

        results = list(collection.aggregate([
            {'$match': {'$text': {'$search': 'Female'}}},
            {'$group': {'_id': '$Gender', 'count': {'$sum': 1}}},
            {'$match': {'count': {'$gt': 1}}}
        ]))

        results = [result['count'] for result in results]
        print(f"3. How many women answer this survey? {sum(results)}")

        results = list(collection.aggregate([
            {'$match': {'$text': {'$search': 'Male'}}},
            {'$group': {'_id': '$Gender', 'count': {'$sum': 1}}},
            {'$match': {'count': {'$gt': 1}}}
        ]))

        results = [result['count'] for result in results]
        print(f"4. How many mans answer this survey? {sum(results)}")


    elif choice == 5:
        sys.exit(0)

    menu(collection)


class FileHandler:
    def __init__(self, database):
        self.__file_path__ = './files/'
        self.__encoding__ = 'latin-1'

        if not Path(f'{self.__file_path__}/data.csv').exists():
            print("Oops, source file is missing (data.csv). Please download it from kaggle.")
            print("https://www.kaggle.com/stackoverflow/stack-overflow-2018-developer-survey")
            sys.exit(0)

        option = input("Do you wanna skip insertions? (y/n): ").lower()

        if option == 'n':
            database.insert(self.__get_file_data__())

    def __get_file_data__(self):
        df = pd.read_csv(
            filepath_or_buffer=f'{self.__file_path__}data.csv',
            sep=',',
            dtype=str,
            engine='c',
            usecols=['Respondent', 'Country', 'Age', 'Gender', 'YearsCoding'],
            encoding=self.__encoding__
        )

        return df.to_dict("records")


class DatabaseHandler:
    def __init__(self):
        self.database = None
        self.collection = None
        self.__init_database__()

    def __create_indexes__(self):
        self.collection.create_index([
            ('Country', ASCENDING),
            ('YearsCoding', HASHED),
            ('Gender', TEXT)
        ])

    def __init_database__(self):
        self.database = MongoClient('mongodb://localhost:27017')['StackOverflow']
        self.collection = self.database.get_collection('Survey')
        self.__create_indexes__()

    def insert(self, documents):
        self.collection.insert_many(documents)


if __name__ == '__main__':
    db_handler = DatabaseHandler()
    file_handler = FileHandler(db_handler)

    menu(db_handler.collection)
