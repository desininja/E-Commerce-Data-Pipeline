import csv
import random
from faker import Faker
from datetime import datetime, timedelta

# Function to read data from CSV files
def read_csv(file_name):
    data = []
    with open(file_name, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

# Read product dimension data
product_dimension = read_csv("product_dimension.csv")

# Read customer dimension data
customer_dimension = read_csv("customer_dimension.csv")

# Define payment types and transaction statuses
payment_types = ["Credit Card", "Debit Card", "Cash", "PayPal"]
transaction_statuses = ["Completed", "Pending", "Failed"]

# Generate daily transaction data
def generate_daily_transactions(num_transactions):
    transactions = []
    for i in range(num_transactions):
        product = random.choice(product_dimension)
        customer = random.choice(customer_dimension)
        quantity = random.randint(1, 5)
        price = float(product['price'])
        transaction_date = datetime.now()
        payment_type = random.choice(payment_types)
        status = random.choice(transaction_statuses)
        
        transaction = {
            "transaction_id": 'TXN'+str(random.randint(99999999,99999999999)),
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "price": price,
            "transaction_date": transaction_date.strftime('%Y-%m-%d'),
            "payment_type": payment_type,
            "status": status
        }
        transactions.append(transaction)
    return transactions

# Generate daily transactions
daily_transactions = generate_daily_transactions(100)
transaction_date = datetime.now()
# Write daily transactions to CSV file
csv_file = f"transactions_{transaction_date.strftime('%Y-%m-%d')}.csv"
with open(csv_file, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=["transaction_id", "customer_id", "product_id", "quantity", "price", "transaction_date", "payment_type", "status"])
    writer.writeheader()
    for transaction in daily_transactions:
        writer.writerow(transaction)

print("Data written to", csv_file)