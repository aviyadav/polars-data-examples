import random
import csv
from datetime import datetime, timedelta
from utils import time_it

@time_it
def generate_data():
    countries = ['US', 'DE', 'IN', 'JP', 'FR', 'GB', 'CA', 'AU', 'BR', 'MX']
    num_users = 1000
    years = [2022, 2023, 2024, 2025]

    # Generate events
    total_events = 0
    for year in years:
        with open(f'data/events_{year}.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['user_id', 'country', 'event_date', 'revenue'])
            for user_id in range(1, num_users + 1):
                num_events = random.randint(1, 500)
                for _ in range(num_events):
                    country = random.choice(countries)
                    start_date = datetime(year, 1, 1)
                    end_date = datetime(year, 12, 31)
                    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
                    event_date = random_date.strftime('%Y-%m-%d')
                    revenue = round(random.uniform(0, 1000), 2)
                    writer.writerow([f'user_{user_id}', country, event_date, revenue])
                    total_events += 1

    print(f"Generated {total_events} events")

    # Generate orders
    with open('data/orders.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['user_id', 'country_code', 'gross_revenue', 'tax', 'discount'])
        for user_id in range(1, num_users + 1):
            num_orders = random.randint(50, 200)  # at least 100k total
            for _ in range(num_orders):
                country_code = random.choice(countries)
                gross_revenue = round(random.uniform(10, 2000), 2)
                tax = round(gross_revenue * 0.1, 2)
                discount = round(random.uniform(0, gross_revenue * 0.2), 2)
                writer.writerow([f'user_{user_id}', country_code, gross_revenue, tax, discount])

    print("Generated orders.csv")

if __name__ == "__main__":
    generate_data()