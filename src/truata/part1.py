from pprint import pprint

from truata.spark import get_spark
from truata.config import GROCERIES_IN_FILE
from truata.config import GROCERIES_TASK_2A_OUT_FILE
from truata.config import GROCERIES_TASK_2B_OUT_FILE
from truata.config import GROCERIES_TASK_3_OUT_FILE
from truata.utils import write_text_to_file


class Part1:
    def __init__(self, spark):
        self.spark = spark
        self.sc = spark.sparkContext

    def task_1_read(self):
        rdd = self.sc.textFile(GROCERIES_IN_FILE)
        print("First 5 lines:")
        pprint(rdd.take(5))
        return rdd

    def task_2_split_products(self, rdd):
        # NOTE: this won't work with commas inside quoted strings.
        rdd = rdd.flatMap(lambda line: line.split(","))
        print("First 5 products:")
        pprint(rdd.take(5))
        return rdd

    def task_2_write_unique_products(self, rdd):
        print("Total products:", rdd.count())
        distinct_products = rdd.distinct()
        distinct_products_text = "\n".join(distinct_products.collect())
        write_text_to_file(distinct_products_text, GROCERIES_TASK_2A_OUT_FILE)

        n_distinct_products = distinct_products.count()
        print("Distinct products:", n_distinct_products)
        count_text = f"Count:\n{n_distinct_products}"
        write_text_to_file(count_text, GROCERIES_TASK_2B_OUT_FILE)

    def task_3_top_purchased(self, rdd, top=5):
        rdd_count = (
            rdd
            .map(lambda word: (word, 1))
            .reduceByKey(lambda a, b: a + b)
            .sortBy(lambda x: x[1], ascending=False)
        )
        top_5_text = "\n".join([str(i) for i in rdd_count.take(top)])
        write_text_to_file(top_5_text, GROCERIES_TASK_3_OUT_FILE)

    def run(self):
        rdd = self.task_1_read()
        rdd = self.task_2_split_products(rdd)
        self.task_2_write_unique_products(rdd)
        self.task_3_top_purchased(rdd)


def main():
    spark = get_spark()
    pipeline = Part1(spark)
    pipeline.run()


if __name__ == "__main__":
    main()
