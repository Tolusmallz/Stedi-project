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
CustomerTrusted_node1735720080829 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://tolus-stedi-datalake-bucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1735720080829")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1735720248696 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://tolus-stedi-datalake-bucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1735720248696")

# Script generated for node Inner Join
SqlQuery6180 = '''
SELECT 
    c.serialnumber, 
    c.sharewithpublicasofdate, 
    c.birthday, 
    c.registrationdate, 
    c.sharewithresearchasofdate, 
    c.customername, 
    c.sharewithfriendsasofdate, 
    c.email, 
    c.lastupdatedate, 
    c.phone, 
    a.user AS user, 
    a.timestamp AS timestamp, 
    a.x AS x, 
    a.y AS y, 
    a.z AS z
    FROM 
        c
INNER JOIN 
        a
ON 
    c.email = a.user
    WHERE 
    c.sharewithresearchasofdate IS NOT NULL 
    and c.sharewithresearchasofdate != 0
'''
InnerJoin_node1735790223705 = sparkSqlQuery(glueContext, query = SqlQuery6180, mapping = {"c":CustomerTrusted_node1735720080829, "a":AccelerometerTrusted_node1735720248696}, transformation_ctx = "InnerJoin_node1735790223705")

# Script generated for node Drop Fields and Duplicates
SqlQuery6179 = '''
SELECT DISTINCT customername,email,phone,birthday,serialnumber,registrationdate,lastupdatedate,sharewithresearchasofdate,
sharewithpublicasofdate,sharewithfriendsasofdate 
FROM myDataSource
'''
DropFieldsandDuplicates_node1735718428684 = sparkSqlQuery(glueContext, query = SqlQuery6179, mapping = {"myDataSource":InnerJoin_node1735790223705}, transformation_ctx = "DropFieldsandDuplicates_node1735718428684")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1735718428684, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1735717385311", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1735717977782 = glueContext.getSink(path="s3://tolus-stedi-datalake-bucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1735717977782")
CustomerCurated_node1735717977782.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1735717977782.setFormat("json")
CustomerCurated_node1735717977782.writeFrame(DropFieldsandDuplicates_node1735718428684)
job.commit()