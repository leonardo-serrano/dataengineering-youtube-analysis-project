
# AWS Glue pyspark job: de-on-youtube-parquet-analytics-version

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1676838888168 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned",
    table_name="cleaned_statistics_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1676838888168",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1676838905019 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned",
    table_name="raw_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1676838905019",
)

# Script generated for node Join
Join_node1676838987889 = Join.apply(
    frame1=AWSGlueDataCatalog_node1676838888168,
    frame2=AWSGlueDataCatalog_node1676838905019,
    keys1=["id"],
    keys2=["category_id"],
    transformation_ctx="Join_node1676838987889",
)

# Script generated for node Amazon S3
AmazonS3_node1676839422481 = glueContext.getSink(
    path="s3://de-on-youtube-analytics-saeast1-dev-x",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1676839422481",
)
AmazonS3_node1676839422481.setCatalogInfo(
    catalogDatabase="db_youtube_analytics", catalogTableName="final_analytics"
)
AmazonS3_node1676839422481.setFormat("glueparquet")
AmazonS3_node1676839422481.writeFrame(Join_node1676838987889)
job.commit()
