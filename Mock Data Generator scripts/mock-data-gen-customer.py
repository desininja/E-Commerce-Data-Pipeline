import random
from faker import Faker
import csv

# Define membership levels
membership_levels = ["Basic", "Gold", "Platinum"]
email_address = ['@gmail.com','@yahoo','@hotmail']
# Create Faker instance for random data generation
faker = Faker()

# Generate customer data
customers = []
for i in range(100):  # Generate data for 100 customers
  
  customer_id= i + 1
  first_name = faker.first_name()
  last_name = faker.last_name()
  email= first_name+last_name+random.choice(email_address)
  membership_level= random.choice(membership_levels)
  customers.append([customer_id,first_name,last_name,email,membership_level])

print(customers)
csv_file = "customer_dimension.csv"
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write header
    writer.writerow(["customer_id", "first_name", "last_name", "email", "membership_level"])
    # Write data
    for customer in customers:
        writer.writerow(customer)

print("Data written to", csv_file)