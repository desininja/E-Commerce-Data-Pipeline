CREATE TABLE e_comm_data_mart.transactions_fact (


    customer_id INT ENCODE lzo,
    first_name VARCHAR(255) ENCODE lzo,
    last_name VARCHAR(255) ENCODE lzo,
    email VARCHAR(255) ENCODE lzo,
    membership_level VARCHAR(255) ENCODE lzo,
    transaction_id VARCHAR(255) ENCODE lzo,
    product_id INT ENCODE lzo,
    quantity INT ENCODE lzo,
    price DECIMAL(10,2) ENCODE DELTA,
    transaction_date DATE ENCODE BYTEDICT,
    payment_type VARCHAR(255) ENCODE lzo,
    status VARCHAR(255) ENCODE lzo,
    product_name VARCHAR(255) ENCODE lzo,
    category VARCHAR(255) ENCODE lzo,
    supplier_id INT ENCODE lzo,
    total_transaction_amount DECIMAL(10,2) ENCODE DELTA,
    Transaction_category VARCHAR(255) ENCODE lzo

)
DISTSTYLE KEY
DISTKEY(Transaction_category)
SORTKEY(transaction_date)