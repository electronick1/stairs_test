from utils import function_as_step, run_pipelines

from pyspark import SparkContext
from pyspark.sql import SparkSession

# Set up the Spark context and the streaming context
sc = SparkContext(appName="PysparkNotebook")

#
# def test_spark_producer(app):
#     producer_output = []
#
#     @app.consumer()
#     def callback_func(a):
#         producer_output.append(a)
#
#     @app.pipeline()
#     def spark_simple_pipeline(pipeline, a, b):
#         return a.subscribe_consumer(callback_func)
#
#     @app.spark_producer(spark_simple_pipeline)
#     def spark_producer():
#         """
#         Simple producer which yields data to `main` pipeline.
#         More information here: http://stairspy.com/#producer
#         """
#
#         spark = SparkSession \
#             .builder \
#             .getOrCreate()
#
#         f = sc.textFile("test.row", 10)
#         df = spark.read.json(f)
#
#         return df
#
#     spark_producer.flush()
#     spark_producer()
#     spark_simple_pipeline.compile()
#     run_pipelines(app)
#
#     assert len(producer_output) == 20
