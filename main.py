from google.cloud import bigquery
import datetime

def fetch_upcoming_birthdays():
    """
    Fetches birthdays for today from BigQuery.
    """
    client = bigquery.Client()
    query = """
        SELECT name, email, birthday
        FROM `your_project_id.birthday_reminder.birthdays`
        WHERE birthday = CURRENT_DATE()
        AND reminder_sent = false;
    """
    query_job = client.query(query)
    results = query_job.result()

    return [{"name": row.name, "email": row.email} for row in results]

def send_email_reminders(birthdays):
    """
    Sends email reminders using Gmail API or any SMTP library.
    """
    for person in birthdays:
        print(f"Sending reminder to {person['name']} at {person['email']}")

def main():
    """
    Main function to fetch and send reminders.
    """
    print("Fetching today's birthdays...")
    birthdays = fetch_upcoming_birthdays()

    if not birthdays:
        print("No birthdays today!")
        return

    print(f"Found {len(birthdays)} birthdays. Sending reminders...")
    send_email_reminders(birthdays)
    print("All reminders sent!")

if __name__ == "__main__":
    main()
