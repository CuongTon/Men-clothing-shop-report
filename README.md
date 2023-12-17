# Some key work remmeber to change later
1. changelater: delete **_airlow**
2. Current date when run data_mart_etl
3. Check AWS S3 ability
4. I added two new field shop_id and item_id, I am consider to add shop_name or not
5. I will use Lambda to combine mutiple file in S3 > I don't think it's a good idea when I will spend a lot of get/put requests to S3. Hence, my solution is I just store final json file after I merge from multiple json files.
6. Remember to change variable name for 3 files in spark_app_etl 