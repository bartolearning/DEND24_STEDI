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

# Script generated for node Customer Curated
CustomerCurated_node1714795125892 = glueContext.create_dynamic_frame.from_catalog(database="dend23", table_name="customer_curated", transformation_ctx="CustomerCurated_node1714795125892")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1714795096371 = glueContext.create_dynamic_frame.from_catalog(database="dend23", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1714795096371")

# Script generated for node Step Trainer in Customer
SqlQuery0 = '''
select * 
from stl
where serialNumber in (select distinct serialNumber from cc)
'''
StepTrainerinCustomer_node1714795149858 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"stl":StepTrainerLanding_node1714795096371, "cc":CustomerCurated_node1714795125892}, transformation_ctx = "StepTrainerinCustomer_node1714795149858")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1714795308531 = glueContext.write_dynamic_frame.from_options(frame=StepTrainerinCustomer_node1714795149858, connection_type="s3", format="json", connection_options={"path": "s3://btolakdend23/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1714795308531")

job.commit()