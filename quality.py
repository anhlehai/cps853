
import csv

total_rows = 0
missing_datetime = 0
invalid_time = 0
invalid_fine = 0
missing_location = 0

passed_rows = 0
failed_rows = 0

# Open the parking tickets dataset
with open("../Parking_Tags_Data_2025.csv", "r", encoding="cp1252") as file:
    # Skip blank lines before the header
    while True:
        position = file.tell()
        line = file.readline()

        if not line:
            raise ValueError("CSV file has no header")

        if line.strip():
            file.seek(position)
            break

    reader = csv.DictReader(file)

    # Check every parking ticket
    for row in reader:
        total_rows += 1

        date = row["DATE_OF_INFRACTION"]
        time = row["TIME_OF_INFRACTION"]
        fine = row["SET_FINE_AMOUNT"]
        location = row["LOCATION2"]

        # Assume the row is valid until a check fails
        row_valid = True

        # Check 1: Missing date or time
        if not date or not time:
            missing_datetime += 1
            row_valid = False

        # Check 2: Invalid time
        if time:
            if len(time) != 4 or not time.isdigit():
                invalid_time += 1
                row_valid = False
            else:
                hour = int(time[:2])
                minute = int(time[2:])

                if hour > 23 or minute > 59:
                    invalid_time += 1
                    row_valid = False

        # Check 3: Invalid fine amount
        if not fine:
            invalid_fine += 1
            row_valid = False
        else:
            try:
                fine_amount = float(fine)

                if fine_amount < 0:
                    invalid_fine += 1
                    row_valid = False

            except ValueError:
                invalid_fine += 1
                row_valid = False

        # Check 4: Missing street/location
        if not location or not location.strip():
            missing_location += 1
            row_valid = False

        # Count rows that passed or failed
        if row_valid:
            passed_rows += 1
        else:
            failed_rows += 1

# Display the results
print("\n===== QUALITY CHECK RESULTS =====")
print("Total rows:", total_rows)
print("Rows missing date or time:", missing_datetime)
print("Rows with invalid time:", invalid_time)
print("Rows with invalid fine amount:", invalid_fine)
print("Rows missing street/location:", missing_location)

print("\n===== ROW COUNTS =====")
print("Rows entering quality checks:", total_rows)
print("Rows passing all checks:", passed_rows)
print("Rows failing at least one check:", failed_rows)
print("Total accounted for:", passed_rows + failed_rows)
