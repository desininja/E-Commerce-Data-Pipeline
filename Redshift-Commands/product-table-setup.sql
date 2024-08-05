create table e_comm_data_mart.dim_products (
    product_id INT ENCODE LZO,
    product_name VARCHAR(255) ENCODE LZO,
    category VARCHAR(255) ENCODE LZO,
    price DECIMAL(10,2) ENCODE DELTA,
    supplier_id INT ENCODE LZO
)
DISTSTYLE KEY
DISTKEY(category);


COPY e_comm_data_mart.dim_products
FROM 's3://project-e-commerce/products-data/product_dimension.csv'
IAM_ROLE 'arn:aws:iam::025066280149:role/Redshift-Access'
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1';


select * from e_comm_data_mart.dim_products limit 4;