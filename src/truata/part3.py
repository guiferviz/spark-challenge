from pprint import pprint

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.classification import LogisticRegression

from truata.spark import get_spark
from truata.config import IRIS_IN_FILE
from truata.config import IRIS_OUT_FILE
from truata.utils import write_text_to_file


class Part3:
    def __init__(self, spark):
        self.spark = spark

    def get_schema(self):
        return T.StructType([
            T.StructField("sepal_length", T.FloatType(), False),
            T.StructField("sepal_width", T.FloatType(), False),
            T.StructField("petal_length", T.FloatType(), False),
            T.StructField("petal_width", T.FloatType(), False),
            T.StructField("class", T.StringType(), False),
        ])

    def task_1_read(self):
        df = (
            self.spark
            .read
            .format("csv")
            .option("header", "false")
            .schema(self.get_schema())
            .load(IRIS_IN_FILE)
        )
        print("First 5 lines:")
        pprint(df.take(5))
        return df

    def task_2_create_model(self):
        return LogisticRegression(
            maxIter=20,
            regParam=0.4,
        )

    def task_2_spark_logistic_regression(self, df):
        #self.predict()
        pass

    def inference(self):
        pred_data = self.spark.createDataFrame(
            [
                (5.1, 3.5, 1.4, 0.2),
                (6.2, 3.4, 5.4, 2.3),
            ],
            ["sepal_length", "sepal_width", "petal_length", "petal_width"]
        )

    def run(self):
        df = self.task_1_read()
        self.task_2_spark_logistic_regression(df)


def main():
    spark = get_spark()
    pipeline = Part3(spark)
    pipeline.run()


if __name__ == "__main__":
    main()
