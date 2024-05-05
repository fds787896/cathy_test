import csv
import json
import pymongo

class CSVToMongoDB:
    def __init__(self, mongo_host, mongo_port, mongo_db, demographic_csv, medications_csv):
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_db = mongo_db
        self.demographic_csv = demographic_csv
        self.medications_csv = medications_csv

    def connect_to_mongodb(self):
        client = pymongo.MongoClient(self.mongo_host, self.mongo_port)
        db = client[self.mongo_db]
        return db

    def read_csv_to_dict(self, csv_file):
        data = {}
        with open(csv_file, 'r', encoding='ISO-8859-1') as file:
            reader = csv.DictReader(file)
            for row in reader:
                seqn = row['SEQN']
                data[seqn] = row
        return data

    def merge_data(self):
        demographic_data = self.read_csv_to_dict(self.demographic_csv)
        medications_data = self.read_csv_to_dict(self.medications_csv)

        for seqn, demographic_record in demographic_data.items():
            if seqn in medications_data:
                medications_list = medications_data[seqn]
                demographic_record['MEDICATIONS'] = json.dumps(medications_list)

        return demographic_data

    def insert_into_mongodb(self):
        db = self.connect_to_mongodb()
        collection = db["demographic_integration"]

        data = self.merge_data()
        for seqn, demographic_record in data.items():
            collection.insert_one(demographic_record)

# 設置MongoDB連線參數
MONGO_HOST = "172.21.0.2"
MONGO_PORT = 27017
MONGO_DB = "test"

# 設置CSV路徑
DEMOGRAPHIC_CSV = 'demographic.csv'
MEDICATIONS_CSV = 'medications.csv'

# 創建CSVToMongoDB對象跟調用
csv_to_mongodb = CSVToMongoDB(MONGO_HOST, MONGO_PORT, MONGO_DB, DEMOGRAPHIC_CSV, MEDICATIONS_CSV)
csv_to_mongodb.insert_into_mongodb()

