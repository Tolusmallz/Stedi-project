import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1735854877400 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1735854877400")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1735854875745 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1735854875745")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1735854872732 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1735854872732")

# Script generated for node SQL Query
SqlQuery5992 = '''
SELECT
    ct.customername,
    ct.email,
    ct.phone,
    ct.birthday,
    ct.serialnumber,
    st.sensorreadingtime,
    st.distancefromobject,
   at.user,
    at.timestamp,
   at.x,
   at.y,
   at.z
FROM
    ct
INNER JOIN
     st
    ON ct.email = st.email
INNER JOIN
    at
    ON ct.email = at.user AND st.sensorreadingtime = at.timestamp
WHERE
    ct.sharewithresearchasofdate IS NOT NULL;
'''
SQLQuery_node1735855040194 = sparkSqlQuery(glueContext, query = SqlQuery5992, mapping = {"ct":CustomerTrusted_node1735854877400, "st":StepTrainerTrusted_node1735854872732, "at":AccelerometerTrusted_node1735854875745}, transformation_ctx = "SQLQuery_node1735855040194")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1735855040194, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1735854757418", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1735857814596 = glueContext.getSink(path="s3://tolus-stedi-datalake-bucket/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1735857814596")
AmazonS3_node1735857814596.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1735857814596.setFormat("json")
AmazonS3_node1735857814596.writeFrame(SQLQuery_node1735855040194)
job.commit()