import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1714794016214 = glueContext.create_dynamic_frame.from_catalog(database="dend23", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1714794016214")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714794021028 = glueContext.create_dynamic_frame.from_catalog(database="dend23", table_name="accelometer_trusted", transformation_ctx="AccelerometerTrusted_node1714794021028")

# Script generated for node Customer in Accelerometer
SqlQuery0 = '''
select * 
from ct
where serialnumber in (select distinct serialnumber from at)
'''
CustomerinAccelerometer_node1714794100208 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"at":AccelerometerTrusted_node1714794021028, "ct":CustomerTrusted_node1714794016214}, transformation_ctx = "CustomerinAccelerometer_node1714794100208")

# Script generated for node Customer Curated
CustomerCurated_node1714794656792 = glueContext.write_dynamic_frame.from_options(frame=CustomerinAccelerometer_node1714794100208, connection_type="s3", format="json", connection_options={"path": "s3://btolakdend23/customer/curated/", "partitionKeys": []}, transformation_ctx="CustomerCurated_node1714794656792")

job.commit()