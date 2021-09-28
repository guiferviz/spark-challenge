from truata.part1 import Part1


def test_split_products(spark):
    part = Part1(spark)
    rdd = spark.sparkContext.parallelize(["a,b", "c"])
    rdd_out = part.task_2_split_products(rdd)
    assert rdd_out.collect() == ["a", "b", "c"]


def test_all_part1(spark, mocker):
    part = Part1(spark)
    rdd_test = spark.sparkContext.parallelize(["a,b", "b"])
    mocker.patch.object(part, "task_1_read", return_value=rdd_test)
    write_mock = mocker.patch("truata.part1.write_text_to_file")

    part.run()

    calls = write_mock.call_args_list
    assert calls[0][0][0] == "a\nb"
    assert calls[1][0][0] == "Count:\n2"
    assert calls[2][0][0] == "('b', 2)\n('a', 1)"
