import os

packages = os.getenv("DALMATIAN_TEST_SPARK_PACKAGES")
if packages and "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {packages} pyspark-shell"
