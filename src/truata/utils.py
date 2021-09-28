def write_text_to_file(text, filename):
    # NOTE: This approach goes against parallelization but it is needed if we
    # want to have just one output file.
    # Another approach would be using Spark saveAsTextFile with one partition
    # and rename the output part files, but it's not more efficient: at some
    # time all the content is in one node.
    with open(filename, "w") as file:
        file.write(text)
