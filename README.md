# cathy_test

操作步驟
1. 修改docker-compose.yaml裡面的鏡像參數,把airflow-postgres-v3,airflow-webserver-v3,airflow-mongo-v3
   分別替換為david-postgres,david-airflow,david-mongo
2. 接著使用docker compose up -d指令把所有的容器服務帶起來
3. 使用docker exec -it airflow-webserver /bin/bash進入容器
4. 使用airflow webserver把airflow服務帶起來
5. 接下來使用airflow dags list查看目前dag
6. 如需要看etl程式碼請到/opt/airflow/process_csv_to_mongodb.py裡面查看
7. 加分題部分在/opt/airflow/advanced/advanced.py裡面
8. 如果需要查看mongodb數據,請輸入docker exec -it airflow-mongo /bin/bash
9. 在容器裡面只用mongosh指令可直接連到數據庫
10. 數據在test數據庫裡面,使用use test切換到目標數據庫
11. 使用show tables;查看需求數據collection
12. 加分題的collection名稱為demographic-integration這張表
