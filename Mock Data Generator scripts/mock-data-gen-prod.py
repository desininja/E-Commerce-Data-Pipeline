import csv
import random

# Sample data for categories and prices
categories = ["Electronics", "Clothing", "Home & Kitchen", "Beauty", "Sports & Outdoors"]
prices = [49.99, 29.99, 89.99, 14.50, 199.00, 799.99, 149.99]

# Generate sample supplier IDs
supplier_ids = list(range(101, 108))

# Function to generate random data for the product dimension table
def generate_product_data(num_rows):
    products = []
    for product_id in range(1, num_rows + 1):
        product_name = "Product_" + str(product_id)
        category = random.choice(categories)
        price = random.choice(prices)
        supplier_id = random.choice(supplier_ids)
        products.append((product_id, product_name, category, price, supplier_id))
    return products

# Generate sample product data with unique product names and IDs
num_rows = 50
product_data = generate_product_data(num_rows)

# Write data to CSV file
csv_file = "product_dimension.csv"
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write header
    writer.writerow(["product_id", "product_name", "category", "price", "supplier_id"])
    # Write data
    for product in product_data:
        writer.writerow(product)

print("Data written to", csv_file)
