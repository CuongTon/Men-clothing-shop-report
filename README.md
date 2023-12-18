# Some key work remmeber to change later
1. changelater: delete **_airlow**
2. Current date when run data_mart_etl
3. Check AWS S3 ability
4. I added two new field shop_id and item_id, I am consider to add shop_name or not
5. I will use Lambda to combine mutiple file in S3 > I don't think it's a good idea when I will spend a lot of get/put requests to S3. Hence, my solution is I just store final json file after I merge from multiple json files.
6. Remember to change variable name for 3 files in spark_app_etl 

# What would I do next?
1. ~~Adjust Spark ETL: change variable name *done*> test *done*> design which step will have shop_name *done by adding name in scratching part*~~
2. ~~Adjust combine_data: be more professional *done*~~
3. ~~Change data_mart: add more field *done*~~
4. ~~Change airflow: add task, how to make task run every 9 hours *Done*. How to make task run mutiple selenium concurrency. Continue with multiple profile
5. Adjust PowerBI and ODBC connection
6. What should I do when one item is deleted
7. Check the time on Power BI why the date is 2 two days later from now.
8. ~~How to run airflow server with one click *done*~~