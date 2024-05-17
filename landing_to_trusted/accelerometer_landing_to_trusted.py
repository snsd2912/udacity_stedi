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

# Script generated for node customer trusted
customertrusted_node1715847320804 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-sanglv/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1715847320804")

# Script generated for node accelerometer landing
accelerometerlanding_node1715847320036 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-sanglv/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1715847320036")

# Script generated for node Join
Join_node1715847323940 = Join.apply(frame1=customertrusted_node1715847320804, frame2=accelerometerlanding_node1715847320036, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1715847323940")

# Script generated for node Drop Fields
DropFields_node1715847845949 = DropFields.apply(frame=Join_node1715847323940, paths=["serialNumber", "birthDay", "registrationDate", "shareWithResearchAsOfDate", "customerName", "shareWithFriendsAsOfDate", "email", "lastUpdateDate", "phone", "shareWithPublicAsOfDate"], transformation_ctx="DropFields_node1715847845949")

# Script generated for node Amazon S3
AmazonS3_node1715847325484 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1715847845949, connection_type="s3", format="json", connection_options={"path": "s3://stedi-sanglv/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1715847325484")

job.commit()