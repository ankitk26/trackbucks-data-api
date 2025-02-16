import email
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from app.search_inbox import get_mail_connection


def get_parsed_emails(mail_ids):
    mail_connection = get_mail_connection()

    parsed_mail_data = {
        "UPI Ref. No.": [],
        "To VPA": [],
        "From VPA": [],
        "Payee Name": [],
        "Amount": [],
        "Transaction Date": [],
    }

    for mail_id in mail_ids:
        result, fetched_mail_data = mail_connection.fetch(mail_id, "(RFC822)")

        if not fetched_mail_data or not isinstance(fetched_mail_data[0], tuple):
            continue  # Skip if no valid data found

        raw_mail = email.message_from_bytes(fetched_mail_data[0][1])

        # Walk through mail content
        for part in raw_mail.walk():
            # Get mail body
            body = part.get_payload(decode=True)

            if body:
                # Parse body content
                soup = BeautifulSoup(body, "html.parser")

                # Get span with required class_name
                spans = soup.find_all("span", class_="gmailmsg")

                for span in spans:
                    # Only get span which contains UPI Ref No
                    if (
                        "UPI Ref. No. " in span.text
                        and "Transaction Status: FAILED" not in span.text
                    ):
                        # Get key:value pairs by splitting
                        lines = str(span).split("<br/>")
                        for line in lines:
                            # Only get the line which contains ':' and does not start with '<'
                            if not line.startswith("<") and ":" in line:
                                pay_key, pay_val = map(str.strip, line.split(":", 1))
                                if pay_key in parsed_mail_data:
                                    parsed_mail_data[pay_key].append(pay_val)

    return parsed_mail_data


def get_mail_dataframe(email_data):
    df = pd.DataFrame(email_data)

    # Rename columns
    df = df.rename(
        columns={
            "UPI Ref. No.": "upi_ref_no",
            "Amount": "amount",
            "From VPA": "sender_upi",
            "To VPA": "receiver_upi",
            "Payee Name": "receiver_name",
            "Transaction Date": "transaction_date",
        }
    )

    # Convert data types
    df["amount"] = df["amount"].astype(float)
    df["upi_ref_no"] = df["upi_ref_no"].astype(
        str
    )  # Keeping as string to avoid precision loss
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], format="%d/%m/%Y %H:%M:%S"
    )

    return df
