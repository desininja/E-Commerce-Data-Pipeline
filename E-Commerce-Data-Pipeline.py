import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import gs_derived

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

# Script generated for node Amazon S3 Transaction
AmazonS3Transaction_node1722857522618 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-e-commerce/transactions/"], "recurse": True}, transformation_ctx="AmazonS3Transaction_node1722857522618")

# Script generated for node Customer table Redshift
CustomertableRedshift_node1722857599265 = glueContext.create_dynamic_frame.from_catalog(database="e-commerce-project-database", table_name="dev_e_comm_data_mart_dim_customers", redshift_tmp_dir="s3://aws-glue-assets-025066280149-us-east-1/temporary/", transformation_ctx="CustomertableRedshift_node1722857599265")

# Script generated for node Product table Redshift
ProducttableRedshift_node1722857661149 = glueContext.create_dynamic_frame.from_catalog(database="e-commerce-project-database", table_name="dev_e_comm_data_mart_dim_products", redshift_tmp_dir="s3://aws-glue-assets-025066280149-us-east-1/temporary/", transformation_ctx="ProducttableRedshift_node1722857661149")

# Script generated for node Renamed keys for Cust-Trans Join
RenamedkeysforCustTransJoin_node1722858838915 = ApplyMapping.apply(frame=CustomertableRedshift_node1722857599265, mappings=[("customer_id", "int", "cust_customer_id", "int"), ("first_name", "string", "cust_first_name", "string"), ("last_name", "string", "cust_last_name", "string"), ("email", "string", "cust_email", "string"), ("membership_level", "string", "cust_membership_level", "string")], transformation_ctx="RenamedkeysforCustTransJoin_node1722858838915")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1722858880733 = ApplyMapping.apply(frame=ProducttableRedshift_node1722857661149, mappings=[("product_id", "int", "prod_product_id", "int"), ("product_name", "string", "prod_product_name", "string"), ("category", "string", "prod_category", "string"), ("price", "decimal", "prod_price", "decimal"), ("supplier_id", "int", "prod_supplier_id", "int")], transformation_ctx="RenamedkeysforJoin_node1722858880733")

# Script generated for node Cust-Trans Join
CustTransJoin_node1722858589083 = Join.apply(frame1=AmazonS3Transaction_node1722857522618, frame2=RenamedkeysforCustTransJoin_node1722858838915, keys1=["customer_id"], keys2=["cust_customer_id"], transformation_ctx="CustTransJoin_node1722858589083")

# Script generated for node Join
Join_node1722858591972 = Join.apply(frame1=CustTransJoin_node1722858589083, frame2=RenamedkeysforJoin_node1722858880733, keys1=["product_id"], keys2=["prod_product_id"], transformation_ctx="Join_node1722858591972")

# Script generated for node Derived Column
DerivedColumn_node1722859129204 = Join_node1722858591972.gs_derived(colName="total_transaction_amount", expr="quantity*price")

# Script generated for node SQL Query
SqlQuery3398 = '''
select *,
case 
    when total_transaction_amount <=100 then 'Small'
    when total_transaction_amount <=500 then 'Medium'
    else 'Large' end as Transaction_category

from myDataSource
'''
SQLQuery_node1722859425252 = sparkSqlQuery(glueContext, query = SqlQuery3398, mapping = {"myDataSource":DerivedColumn_node1722859129204}, transformation_ctx = "SQLQuery_node1722859425252")

# Script generated for node Select Fields
SelectFields_node1722859861246 = SelectFields.apply(frame=SQLQuery_node1722859425252, paths=["customer_id", "cust_first_name", "cust_last_name", "cust_email", "cust_membership_level", "transaction_id", "product_id", "quantity", "price", "transaction_date", "payment_type", "status", "prod_product_name", "prod_category", "prod_supplier_id", "total_transaction_amount", "Transaction_category"], transformation_ctx="SelectFields_node1722859861246")

# Script generated for node Change Schema
ChangeSchema_node1722860034388 = ApplyMapping.apply(frame=SelectFields_node1722859861246, mappings=[("customer_id", "string", "customer_id", "int"), ("cust_first_name", "string", "first_name", "varchar"), ("cust_last_name", "string", "last_name", "varchar"), ("cust_email", "string", "email", "varchar"), ("cust_membership_level", "string", "membership_level", "varchar"), ("transaction_id", "string", "transaction_id", "varchar"), ("product_id", "string", "product_id", "int"), ("quantity", "string", "quantity", "int"), ("price", "string", "price", "decimal"), ("transaction_date", "string", "transaction_date", "date"), ("payment_type", "string", "payment_type", "varchar"), ("status", "string", "status", "varchar"), ("prod_product_name", "string", "product_name", "varchar"), ("prod_category", "string", "category", "varchar"), ("prod_supplier_id", "int", "supplier_id", "int"), ("total_transaction_amount", "double", "total_transaction_amount", "decimal"), ("Transaction_category", "string", "Transaction_category", "varchar")], transformation_ctx="ChangeSchema_node1722860034388")

# Script generated for node Amazon Redshift
AmazonRedshift_node1722861213874 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1722860034388, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO e_comm_data_mart.transactions_fact USING e_comm_data_mart.transactions_fact_temp_wvuhew ON transactions_fact.transaction_id = transactions_fact_temp_wvuhew.transaction_id WHEN MATCHED THEN UPDATE SET customer_id = transactions_fact_temp_wvuhew.customer_id, first_name = transactions_fact_temp_wvuhew.first_name, last_name = transactions_fact_temp_wvuhew.last_name, email = transactions_fact_temp_wvuhew.email, membership_level = transactions_fact_temp_wvuhew.membership_level, transaction_id = transactions_fact_temp_wvuhew.transaction_id, product_id = transactions_fact_temp_wvuhew.product_id, quantity = transactions_fact_temp_wvuhew.quantity, price = transactions_fact_temp_wvuhew.price, transaction_date = transactions_fact_temp_wvuhew.transaction_date, payment_type = transactions_fact_temp_wvuhew.payment_type, status = transactions_fact_temp_wvuhew.status, product_name = transactions_fact_temp_wvuhew.product_name, category = transactions_fact_temp_wvuhew.category, supplier_id = transactions_fact_temp_wvuhew.supplier_id, total_transaction_amount = transactions_fact_temp_wvuhew.total_transaction_amount, Transaction_category = transactions_fact_temp_wvuhew.Transaction_category WHEN NOT MATCHED THEN INSERT VALUES (transactions_fact_temp_wvuhew.customer_id, transactions_fact_temp_wvuhew.first_name, transactions_fact_temp_wvuhew.last_name, transactions_fact_temp_wvuhew.email, transactions_fact_temp_wvuhew.membership_level, transactions_fact_temp_wvuhew.transaction_id, transactions_fact_temp_wvuhew.product_id, transactions_fact_temp_wvuhew.quantity, transactions_fact_temp_wvuhew.price, transactions_fact_temp_wvuhew.transaction_date, transactions_fact_temp_wvuhew.payment_type, transactions_fact_temp_wvuhew.status, transactions_fact_temp_wvuhew.product_name, transactions_fact_temp_wvuhew.category, transactions_fact_temp_wvuhew.supplier_id, transactions_fact_temp_wvuhew.total_transaction_amount, transactions_fact_temp_wvuhew.Transaction_category); DROP TABLE e_comm_data_mart.transactions_fact_temp_wvuhew; END;", "redshiftTmpDir": "s3://aws-glue-assets-025066280149-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "e_comm_data_mart.transactions_fact_temp_wvuhew", "connectionName": "redshift-connection", "preactions": "CREATE TABLE IF NOT EXISTS e_comm_data_mart.transactions_fact (customer_id INTEGER, first_name VARCHAR, last_name VARCHAR, email VARCHAR, membership_level VARCHAR, transaction_id VARCHAR, product_id INTEGER, quantity INTEGER, price DECIMAL, transaction_date DATE, payment_type VARCHAR, status VARCHAR, product_name VARCHAR, category VARCHAR, supplier_id INTEGER, total_transaction_amount DECIMAL, Transaction_category VARCHAR); DROP TABLE IF EXISTS e_comm_data_mart.transactions_fact_temp_wvuhew; CREATE TABLE e_comm_data_mart.transactions_fact_temp_wvuhew (customer_id INTEGER, first_name VARCHAR, last_name VARCHAR, email VARCHAR, membership_level VARCHAR, transaction_id VARCHAR, product_id INTEGER, quantity INTEGER, price DECIMAL, transaction_date DATE, payment_type VARCHAR, status VARCHAR, product_name VARCHAR, category VARCHAR, supplier_id INTEGER, total_transaction_amount DECIMAL, Transaction_category VARCHAR);"}, transformation_ctx="AmazonRedshift_node1722861213874")

job.commit()