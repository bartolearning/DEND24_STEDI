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

# Script generated for node cust_landing
cust_landing_node1714364843667 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://btolakdend23/customer/landing/customer-1691348231425.json"], "recurse": True}, transformation_ctx="cust_landing_node1714364843667")

# Script generated for node Filter
Filter_node1714364850278 = Filter.apply(frame=cust_landing_node1714364843667, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Filter_node1714364850278")

# Script generated for node cust_trusted
cust_trusted_node1714364861910 = glueContext.getSink(path="s3://btolakdend23/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="cust_trusted_node1714364861910")
cust_trusted_node1714364861910.setCatalogInfo(catalogDatabase="dend23",catalogTableName="customer_trusted")
cust_trusted_node1714364861910.setFormat("json")
cust_trusted_node1714364861910.writeFrame(Filter_node1714364850278)
job.commit()