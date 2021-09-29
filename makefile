DOWNLOADS_DIR=in


all: download_groceries download_airbnb download_iris part1 part2 part2_airflow
	@echo "Success :)"

download_groceries:
	wget https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv -P $(DOWNLOADS_DIR)

download_airbnb:
	rm -rf /tmp/learning_spark_repo
	git clone --depth 1 https://github.com/databricks/LearningSparkV2.git /tmp/learning_spark_repo
	cp -r /tmp/learning_spark_repo/mlflow-project-example/data/sf-airbnb-clean.parquet $(DOWNLOADS_DIR)
	rm -rf /tmp/learning_spark_repo

download_iris:
	wget https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data -P $(DOWNLOADS_DIR)

part1:
	spark-submit src/truata/part1.py 2> /dev/null

part2:
	spark-submit src/truata/part2.py 2> /dev/null

part2_airflow:
	cp ./src/truata/task_2_5.py ~/airflow/dags/
	airflow dags backfill truata_airflow -s 2021-09-28 -e 2021-09-28

part3:
	spark-submit src/truata/part3.py 2> /dev/null

test:
	pytest tests/
