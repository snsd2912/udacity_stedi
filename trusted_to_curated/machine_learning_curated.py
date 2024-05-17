import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1715859036422 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-sanglv/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1715859036422")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1715859035116 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-sanglv/step_trainer/trusted/"]}, transformation_ctx="step_trainer_trusted_node1715859035116")

# Script generated for node Join
Join_node1715859043016 = Join.apply(frame1=step_trainer_trusted_node1715859035116, frame2=accelerometer_trusted_node1715859036422, keys1=["sensorReadingTime"], keys2=["timestamp"], transformation_ctx="Join_node1715859043016")

# Script generated for node Amazon S3
AmazonS3_node1715859176102 = glueContext.write_dynamic_frame.from_options(frame=Join_node1715859043016, connection_type="s3", format="json", connection_options={"path": "s3://stedi-sanglv/machine-learning/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1715859176102")

job.commit()