from pprint import pprint

import pyspark.sql.functions as F

from truata.spark import get_spark
from truata.config import AIRBNB_IN_FILE
from truata.config import AIRBNB_TASK_2_OUT_FILE
from truata.config import AIRBNB_TASK_3_OUT_FILE
from truata.config import AIRBNB_TASK_4_OUT_FILE
from truata.utils import write_text_to_file


class Part2:
    def __init__(self, spark):
        self.spark = spark

    def task_1_read(self):
        df = self.spark.read.format("parquet").load(AIRBNB_IN_FILE)
        print("First 2 lines:")
        pprint(df.take(2))
        return df

    def task_2_agg(self, df):
        df_agg = (
            df
            .agg(F.min("price"), F.max("price"), F.count("price"))
        )
        agg_text = "min_price, max_price, row_count\n"
        agg_text += ", ".join([str(i) for i in df_agg.collect()[0]])
        write_text_to_file(agg_text, AIRBNB_TASK_2_OUT_FILE)

    def task_3_average(self, df):
        df_avg = (
            df
            .filter(F.col("price") > 5000)
            .filter(F.col("review_scores_value") == 10)
            .agg(F.avg("bedrooms"), F.avg("bathrooms"))
        )
        avg_text = "avg_bathrooms, avg_bedrooms\n"
        avg_text += ", ".join([str(i) for i in df_avg.collect()[0]])
        write_text_to_file(avg_text, AIRBNB_TASK_3_OUT_FILE)

    def task_4_low_price_high_rating(self, df):
        best = (
            df
            .orderBy(F.col("price").asc(), F.col("review_scores_value").desc())
            .select("accommodates")
        )
        accommodates_text = str(best.take(1)[0][0])
        write_text_to_file(accommodates_text, AIRBNB_TASK_4_OUT_FILE)

    def run(self):
        df = self.task_1_read()
        self.task_2_agg(df)
        self.task_3_average(df)
        self.task_4_low_price_high_rating(df)


def main():
    spark = get_spark()
    pipeline = Part2(spark)
    pipeline.run()


if __name__ == "__main__":
    main()
