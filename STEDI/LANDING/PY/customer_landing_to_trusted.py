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

# Script generated for node Accelometer Trusted
AccelometerTrusted_node1714792572332 = glueContext.create_dynamic_frame.from_catalog(database="dend23", table_name="accelometer_trusted", transformation_ctx="AccelometerTrusted_node1714792572332")

# Script generated for node Customer Trusted
CustomerTrusted_node1714792297983 = glueContext.create_dynamic_frame.from_catalog(database="dend23", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1714792297983")

# Script generated for node SQL Query
SqlQuery0 = '''
select * 
from ct
where serialnumber in (select distinct serialnumber from at)

'''
SQLQuery_node1714792489043 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"ct":CustomerTrusted_node1714792297983, "at":AccelometerTrusted_node1714792572332}, transformation_ctx = "SQLQuery_node1714792489043")

# Script generated for node Customer Curated
CustomerCurated_node1714792519479 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1714792489043, connection_type="s3", format="json", connection_options={"path": "s3://btolakdend23/customer/curated/", "partitionKeys": []}, transformation_ctx="CustomerCurated_node1714792519479")

job.commit()