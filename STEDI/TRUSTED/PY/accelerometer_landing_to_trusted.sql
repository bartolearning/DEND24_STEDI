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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1714474555032 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://btolakdend23/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrustedZone_node1714474555032")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1714474529949 = glueContext.create_dynamic_frame.from_catalog(database="dend23", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1714474529949")

# Script generated for node Join
Join_node1714474622460 = Join.apply(frame1=CustomerTrustedZone_node1714474555032, frame2=AccelerometerLanding_node1714474529949, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1714474622460")

# Script generated for node Drop Fields
DropFields_node1714538502299 = DropFields.apply(frame=Join_node1714474622460, paths=["user"], transformation_ctx="DropFields_node1714538502299")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714474689380 = glueContext.getSink(path="s3://btolakdend23/accelometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1714474689380")
AccelerometerTrusted_node1714474689380.setCatalogInfo(catalogDatabase="dend23",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1714474689380.setFormat("json")
AccelerometerTrusted_node1714474689380.writeFrame(DropFields_node1714538502299)
job.commit()