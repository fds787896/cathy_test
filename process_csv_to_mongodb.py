from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import csv
import json
from pymongo import MongoClient
import os

class CSVToMongoDB:
    def __init__(self, mongo_host, mongo_port, mongo_db, csv_directory, csv_files):
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_db = mongo_db
        self.csv_directory = csv_directory
        self.csv_files = csv_files

    def connect_to_mongodb(self):
        """
        建立 MongoDB 連接
        """
        client = MongoClient(self.mongo_host, self.mongo_port)
        db = client[self.mongo_db]
        return db

    def read_csv_to_json(self, csv_file_path, encoding='utf-8'):
        """
        從 CSV 檔案中讀取數據並轉換為 JSON
        """
        with open(csv_file_path, 'r', encoding=encoding) as file:
            reader = csv.DictReader(file)
            json_data = [row for row in reader]
        return json_data

    def insert_json_into_mongodb(self, db, collection_name, json_data):
        """
        將 JSON 數據插入到 MongoDB 中的指定集合中
        """
        collection = db[collection_name]
        collection.insert_many(json_data)

    def process_csv_files(self):
        """
        處理每個 CSV 檔案並插入到 MongoDB 中
        """
        # 連線 MongoDB
        db = self.connect_to_mongodb()

        # 處理每個 CSV 檔案
        for csv_file in self.csv_files:
            collection_name = csv_file.split('.')[0]

            # 構建 CSV 檔案的完整路徑
            csv_file_path = os.path.join(self.csv_directory, csv_file)

            # 使用不同的編碼方式打開 CSV 檔案
            encoding = 'ISO-8859-1' if collection_name == 'medications' else 'utf-8'
            
            # 讀取 CSV 檔案並轉換為 JSON
            json_data = self.read_csv_to_json(csv_file_path, encoding)

            # 將 JSON 數據插入到 MongoDB 中的對應集合
            self.insert_json_into_mongodb(db, collection_name, json_data)

# 定義 MongoDB 連接設定
MONGO_HOST = "172.20.0.3"
MONGO_PORT = 27017
MONGO_DB = "test"

# 定義 CSV 檔案列表
CSV_FILES = ['demographic.csv', 'diet.csv', 'examination.csv', 'labs.csv', 'questionnaire.csv', 'medications.csv']

# CSV 檔案目錄路徑
CSV_DIRECTORY = '/opt/airflow/share/'

# 定義 DAG
default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 5, 6),  # 開始日期為 2024 年 5 月 6 日
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_csv_to_mongodb',
    default_args=default_args,
    description='Process CSV files and insert into MongoDB',
    schedule_interval='@daily',  # 每天执行一次
)

# 初始化 CSVToMongoDB 对象
csv_to_mongodb = CSVToMongoDB(MONGO_HOST, MONGO_PORT, MONGO_DB, CSV_DIRECTORY, CSV_FILES)

# 定義 PythonOperator
process_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=csv_to_mongodb.process_csv_files,
    dag=dag,
)

# 設置 DAG 的 Task 依賴
process_task
