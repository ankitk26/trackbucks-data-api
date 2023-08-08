from datetime import datetime
from os import getenv

from fastapi import FastAPI
from parse_email import get_df, parse_email
from select_inbox import search_inbox
from supabase import Client, create_client

SUPABASE_URL = "https://qjgsmbsouzljaczqfkkv.supabase.co"
SUPABASE_KEY = getenv("SUPABASE_KEY")
# "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFqZ3NtYnNvdXpsamFjenFma2t2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzY0ODU4NTcsImV4cCI6MTk5MjA2MTg1N30.1JD0F9Qc93_mfHCCeTLWPFhtq6TtQOWpeHkIHfpL-RI"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()


@app.get("/transactions")
def get_transactions():
    response = supabase.table("transactions").select("*").execute()
    return response.data


@app.post("/transactions")
def populate_all_transactions():
    mail_ids = search_inbox()
    email_data = parse_email(mail_ids)

    email_df = get_df(email_data)

    for index, row in email_df.iterrows():
        curr_dict = email_df.iloc[index].to_dict()
        curr_dict["payment_mode"] = "UPI"
        data, count = supabase.table("transactions").insert(curr_dict).execute()

    return {"message": "Rows added", "mails_count": email_df.shape[0]}


@app.post("/new-transactions")
def add_new_transactions():
    try:
        last_transaction_timestamp = (
            supabase.table("transactions")
            .select("transaction_date")
            .order("transaction_date", desc=True)
            .limit(1)
            .single()
            .execute()
            .data["transaction_date"]
        )
        dt = datetime.fromisoformat(last_transaction_timestamp[:-6])
        last_transaction_date = dt.replace(hour=0, minute=0, second=0, microsecond=0)

        transactions_on_latest_date = (
            supabase.table("transactions")
            .select("*")
            .gte("transaction_date", last_transaction_date)
            .execute()
        )

        existing_mail_ids = list(
            map(lambda x: x["mail_id"], transactions_on_latest_date.data)
        )
        mail_ids = search_inbox(last_transaction_date, fetch_type="latest")
        unpublished_mail_ids = [
            x for x in mail_ids if int(x.decode("UTF-8")) not in existing_mail_ids
        ]

        if len(unpublished_mail_ids) == 0:
            return {"message": "All upto date"}

        email_data = parse_email(unpublished_mail_ids)
        email_df = get_df(email_data)

        for index, row in email_df.iterrows():
            curr_dict = email_df.iloc[index].to_dict()
            curr_dict["payment_mode"] = "UPI"
            data, count = supabase.table("transactions").insert(curr_dict).execute()

        return {"message": "Rows added", "mails_count": email_df.shape[0]}

        # return {
        #     "last_transaction_timestamp": last_transaction_timestamp,
        #     "last_date_transactions": list(
        #         map(lambda x: x["mail_id"], transactions_on_latest_date.data)
        #     ),
        #     "mail_ids": mail_ids,
        #     "unpublished_mail_ids": unpublished_mail_ids,
        # }

    except Exception as e:
        return {"message": str(e)}
