from truata.part3 import Part3


def test_all_part3(spark, mocker):
    part = Part3(spark)
    df = spark.createDataFrame(
        [
            [4.8,3.4,1.9,0.2,"Iris-setosa"],
            [4.9,2.4,3.3,1.0,"Iris-versicolor"],
            [4.9,2.5,4.5,1.7,"Iris-virginica"],
        ]*10,
        "sepal_length float, sepal_width float, petal_length float,"
        "petal_width float, class string",
    )
    mocker.patch.object(part, "task_1_read", return_value=df)
    write_mock = mocker.patch("truata.part3.write_text_to_file")

    part.run()

    calls = write_mock.call_args_list
    assert calls[0][0][0] == "class\nIris Setosa\nIris Versicolour"
