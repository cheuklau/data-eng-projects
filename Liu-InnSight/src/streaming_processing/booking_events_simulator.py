import random
from datetime import datetime, timedelta
# Import Faker which generates fake data
# Documentation: https://faker.readthedocs.io/en/master/
from faker import Faker

# Create Faker object
fake = Faker()

# Define paramters
# Generate 1 million records spanning 2017 to now
record_num = 1000000
min_year = 2017
max_year = datetime.now().year

# Determine starting and ending datetime objects
start = datetime(min_year, 1, 1, 00, 00, 00)
years = max_year - min_year + 1
end = start + timedelta(days=365 * years)

# Write each record as an entry into 'booking.txt' with format:
# timestamp, zipcode, url, price
file = open('booking.txt', 'r+')
for i in range(record_num):
    random_date = start + (end - start) * random.random()
    zipcode = fake.address().split()[-1]
    listing_id = random.randint(10000, 999999)
    price = random.randint(100, 800)
    info_list = [str(random_date), str(zipcode), str(listing_id), str(price)]
    seperator = ','
    newline = seperator.join(info_list)
    file.write(newline + '\n')
file.close()
