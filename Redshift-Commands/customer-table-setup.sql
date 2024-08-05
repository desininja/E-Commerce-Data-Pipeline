create SCHEMA e_comm_data_mart;


create TABLE e_comm_data_mart.dim_customers (
    customer_id int ENCODE LZO,
    first_name VARCHAR(255) ENCODE LZO,
    last_name VARCHAR(255) ENCODE LZO,
    email VARCHAR(255) ENCODE LZO,
    membership_level VARCHAR(255) ENCODE LZO

);

COPY e_comm_data_mart.dim_customers 
from 's3://project-e-commerce/customers-data/customer_dimension.csv'
IAM_ROLE 'arn:aws:iam::025066280149:role/Redshift-Access'
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1';

select * from e_comm_data_mart.dim_customers limit 4;