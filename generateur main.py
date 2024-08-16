import csv
import random
from datetime import datetime, timedelta

def generate_and_write_transactions(filename, num_transactions):
    services = [("service1", 1), ("service2", 2), ("service3", 3), ("service4", 4), 
                ("service5", 5), ("service6", 6), ("service7", 7), ("service8", 8), 
                ("service9", 9), ("service10", 10)]

    start_date = datetime(2023, 7, 23)
    end_date = datetime(2024, 8, 13)
    current_date = start_date

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['reference', 'amount', 'date', 'heure', 'service', 'id_service'])

        while num_transactions > 0:
            # Ensure at least one transaction per service each day
            daily_transactions = []
            for service, id_service in services:
                reference = random.randint(1000, 9999)
                amount = f"{random.randint(1, 500)}K"
                time = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
                daily_transactions.append([reference, amount, current_date.strftime("%d/%m/%y"), time, service, id_service])
                num_transactions -= 1
                if num_transactions <= 0:
                    break

            # Add additional random transactions for the day
            additional_transactions = random.randint(0, 2500 - len(daily_transactions))
            for _ in range(min(additional_transactions, num_transactions)):
                reference = random.randint(1000, 9999)
                amount = f"{random.randint(1, 500)}K"
                time = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
                service, id_service = random.choice(services)
                daily_transactions.append([reference, amount, current_date.strftime("%d/%m/%y"), time, service, id_service])
                num_transactions -= 1
                if num_transactions <= 0:
                    break

            # Write the daily transactions to the file
            writer.writerows(daily_transactions)

            current_date += timedelta(days=1)
            if current_date > end_date:
                current_date = start_date

# Generate and write 400,000 transactions to a CSV file
generate_and_write_transactions('transactionsmain3.csv', 600000)

print("Fichier CSV généré avec succès.")
