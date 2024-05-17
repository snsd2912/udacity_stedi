import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1715841996208 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-sanglv/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1715841996208")

# Script generated for node Filter
Filter_node1715843668044 = Filter.apply(frame=AmazonS3_node1715841996208, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Filter_node1715843668044")

# Script generated for node Amazon S3
AmazonS3_node1715843778276 = glueContext.write_dynamic_frame.from_options(frame=Filter_node1715843668044, connection_type="s3", format="json", connection_options={"path": "s3://stedi-sanglv/customer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1715843778276")

job.commit()