from truata.part2 import Part2


def test_all_part2(spark, mocker):
    part = Part2(spark)
    df = spark.createDataFrame(
        [
            [10000.0, 10, 4, 10.0, 1],
            [5001.0,   5, 2, 10.0, 2],
            [5001.0,   5, 2, 9.99, 3],
            [5000.0,   0, 0, 10.0, 4],
        ],
        "price float, bedrooms int, bathrooms int, review_scores_value float,"
        "accommodates int",
    )
    mocker.patch.object(part, "task_1_read", return_value=df)
    write_mock = mocker.patch("truata.part2.write_text_to_file")

    part.run()

    calls = write_mock.call_args_list
    assert calls[0][0][0] == "min_price, max_price, row_count\n5000.0, 10000.0, 4"
    assert calls[1][0][0] == "avg_bathrooms, avg_bedrooms\n7.5, 3.0"
    assert calls[2][0][0] == "4"
