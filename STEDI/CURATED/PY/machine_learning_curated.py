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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714795840502 = glueContext.create_dynamic_frame.from_catalog(database="dend23", table_name="accelometer_trusted", transformation_ctx="AccelerometerTrusted_node1714795840502")

# Script generated for node Customer Curated
CustomerCurated_node1714795810653 = glueContext.create_dynamic_frame.from_catalog(database="dend23", table_name="accelometer_curated", transformation_ctx="CustomerCurated_node1714795810653")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1714795867266 = glueContext.create_dynamic_frame.from_catalog(database="dend23", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1714795867266")

# Script generated for node Main Join
SqlQuery0 = '''
with join_table as 
(
select distinct cc.email
, cc.serialnumber
, ac.timestamp
, ac.x
, ac.y
, ac.z
from customer_curated cc
left join accelometer_trusted ac on ac.email=cc.email
)

select stt.sensorreadingtime
, stt.serialnumber
, stt.distancefromobject
, jt.timestamp
, jt.x
, jt.y
, jt.z
from step_trainer_trusted stt
join join_table jt on stt.sensorreadingtime = jt.timestamp
'''
MainJoin_node1714796036699 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1714795867266, "customer_curated":CustomerCurated_node1714795810653, "accelometer_trusted":AccelerometerTrusted_node1714795840502}, transformation_ctx = "MainJoin_node1714796036699")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1714796421993 = glueContext.write_dynamic_frame.from_options(frame=MainJoin_node1714796036699, connection_type="s3", format="json", connection_options={"path": "s3://btolakdend23/machine_learning/curated/", "partitionKeys": []}, transformation_ctx="MachineLearningCurated_node1714796421993")

job.commit()