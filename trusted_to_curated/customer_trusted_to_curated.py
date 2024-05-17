import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer trsuted
customertrsuted_node1715856746714 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-sanglv/customer/trusted/"], "recurse": True}, transformation_ctx="customertrsuted_node1715856746714")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1715856748038 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-sanglv/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1715856748038")

# Script generated for node Join
Join_node1715856752121 = Join.apply(frame1=customertrsuted_node1715856746714, frame2=accelerometer_trusted_node1715856748038, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1715856752121")

# Script generated for node Drop Fields
DropFields_node1715856757688 = DropFields.apply(frame=Join_node1715856752121, paths=["z", "user", "y", "x", "timestamp"], transformation_ctx="DropFields_node1715856757688")

# Script generated for node Drop Duplicates
DropDuplicates_node1715857655249 =  DynamicFrame.fromDF(DropFields_node1715856757688.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1715857655249")

# Script generated for node Amazon S3
AmazonS3_node1715856760500 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1715857655249, connection_type="s3", format="json", connection_options={"path": "s3://stedi-sanglv/customer/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1715856760500")

job.commit()