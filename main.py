from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import traceback

def fetch_upcoming_birthdays():
    """
    Fetches birthdays for today from BigQuery.
    """
    key_path = './key1.json'
    client = bigquery.Client.from_service_account_json(key_path)

    try:
        query = """
            SELECT *
            FROM `birthday-reminder-01.dataset01.table01`
            WHERE
            remindersent = false
            ;
        """
            # WHERE birth-date = CURRENT_DATE()
        query_job = client.query(query)
        results = query_job.result()
        print(f"\n type of result : {type(results)}")
        data = [{"name": row.name, "birthdate":row.birthdate } for row in results]

        print(f"\n type of data1 : {type(data)}")
        data = pd.DataFrame(data)
        print(f"\n type of data2 : {type(data)}")
        
        # filter data to get today's birthdays and age of the person
        data2 = filterData(data)
        return data2

    except :
        print("Exception while calling biq-gquery !")
        traceback.print_exc();  


def filterData(df):
    print("\n df from bigquery : ",df ,"\n ")

    today = datetime.now()
    today_day = today.day
    today_month = today.month
    today_year = today.year

    # Extract day and month from the birthday column
    df['day'] = pd.to_datetime(df['birthdate']).dt.day
    df['month'] = pd.to_datetime(df['birthdate']).dt.month
    df['year'] = pd.to_datetime(df['birthdate']).dt.year

    # Filter rows where day and month match today's day and month
    today_birthdays = df[(df['day'] == today_day) & (df['month'] == today_month)]   
    
    # Calculate age 
    today_birthdays.loc[:, 'age'] = today_year - today_birthdays['year']
    print("\n today's birthdays with age \n",today_birthdays) 

    #now extract only name an age in asceding order of name and remove first column as well and create a df and not a dictionary only with name and age
    today_birthdays = today_birthdays[['name','age']].sort_values('name')
    #remove index column
    # today_birthdays.reset_index(drop=True, inplace=True)
    return today_birthdays


def send_email_reminders(birthdays):
    """
    Sends email reminders using Gmail API or any SMTP library.
    """
    #print the message for each person - include their name and age
    for index, row in birthdays.iterrows():
        print(f"Sending email to {row['name']} for their {row['age']}th birthday!")

def main():
    """
    Main function to fetch and send reminders.
    """
    print("Fetching today's birthdays...")
    birthdays = fetch_upcoming_birthdays()

    # print(f"\n All saved birthdays\n: {birthdays} ")
    print("\n Sending reminders ... \n ")
    send_email_reminders(birthdays)
    print("\n All reminders sent!")

if __name__ == "__main__":
    main()
