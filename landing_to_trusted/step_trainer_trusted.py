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

# Script generated for node trainer landing
trainerlanding_node1715851302508 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-sanglv/step_trainer/landing/"], "recurse": True}, transformation_ctx="trainerlanding_node1715851302508")

# Script generated for node customer trusted
customertrusted_node1715851301244 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-sanglv/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1715851301244")

# Script generated for node Join
Join_node1715851305575 = Join.apply(frame1=trainerlanding_node1715851302508, frame2=customertrusted_node1715851301244, keys1=["serialNumber"], keys2=["serialNumber"], transformation_ctx="Join_node1715851305575")

# Script generated for node Drop Fields
DropFields_node1715851310422 = DropFields.apply(frame=Join_node1715851305575, paths=["`.serialNumber`", "birthDay", "registrationDate", "shareWithResearchAsOfDate", "customerName", "shareWithFriendsAsOfDate", "email", "lastUpdateDate", "phone", "shareWithPublicAsOfDate"], transformation_ctx="DropFields_node1715851310422")

# Script generated for node Amazon S3
AmazonS3_node1715851313645 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1715851310422, connection_type="s3", format="json", connection_options={"path": "s3://stedi-sanglv/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1715851313645")

job.commit()