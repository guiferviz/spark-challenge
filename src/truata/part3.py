from pprint import pprint

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

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
        df_train, df_test = df.randomSplit([.8, .2], seed=124)
        model = self.train(df_train)
        self.test(df_test, model)
        self.inference(model)

    def train(self, df_train):
        # NOTE: mlflow or any other experiment registry should be used here.
        model = self.task_2_create_model()
        vector_assembler = VectorAssembler(
            inputCols=[
                "sepal_length", "sepal_width", "petal_length", "petal_width",
            ],
            outputCol="features",
        )
        string_indexer = StringIndexer(
            inputCol="class",
            outputCol="label",
            stringOrderType="alphabetAsc",
        )
        pipeline = Pipeline(stages=[vector_assembler, string_indexer, model])
        pipeline_model = pipeline.fit(df_train)
        print("Evaluating training")
        self.evaluate(pipeline_model, df_train)
        return pipeline_model

    def evaluate(self, model, df):
        pred = model.transform(df)
        evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
        acc = evaluator.evaluate(pred)
        print("Accuracy", acc)

    def test(self, df_test, model):
        print("Evaluating test")
        self.evaluate(model, df_test)

    def inference(self, model):
        pred_data = self.spark.createDataFrame(
            [
                (0, 5.1, 3.5, 1.4, 0.2),
                (1, 6.2, 3.4, 5.4, 2.3),
            ],
            [
                "id", "sepal_length", "sepal_width", "petal_length",
                "petal_width",
            ],
        )
        pred = model.transform(pred_data).select("id", "prediction").collect()
        pred = sorted(pred, key=lambda x: x.id)
        # As we are sorting classes by name ascending in the StringIndexer...
        class_name = ["Iris Setosa", "Iris Versicolour", "Iris Virginica"]
        pred_text = "class\n"
        pred_text += "\n".join([class_name[int(i.prediction)] for i in pred])
        write_text_to_file(pred_text, IRIS_OUT_FILE)

    def run(self):
        df = self.task_1_read()
        self.task_2_spark_logistic_regression(df)


def main():
    spark = get_spark()
    pipeline = Part3(spark)
    pipeline.run()


if __name__ == "__main__":
    main()
