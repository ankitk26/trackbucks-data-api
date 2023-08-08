import email
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from .select_inbox import get_mail


def parse_email(mail_ids):
    mail = get_mail()

    data = {
        "UPI Ref. No.": [],
        "mail_id": [],
        "To VPA": [],
        "From VPA": [],
        "Payee Name": [],
        "Amount": [],
        "Transaction Date": [],
    }

    for mail_id in mail_ids:
        r, mail_data = mail.fetch(mail_id, "(RFC822)")
        raw_mail = email.message_from_bytes(mail_data[0][1])

        # Walk through mail content
        for part in raw_mail.walk():
            # Get mail body
            body = part.get_payload(decode=True)

            if body is not None:
                # Parse body content
                soup = BeautifulSoup(body, "html.parser")

                # Get span with required class_name
                spans = soup.find_all("span", class_="gmailmsg")

                for span in spans:
                    # Only get span which contains UPI Ref No
                    if span.text.find("UPI Ref. No. ") != -1:
                        # Get key:value pairs by splitting
                        lines = str(span).split("<br/>")
                        for line in lines:
                            # Only get the line which contains ':' and does not start with '<'
                            if not line.startswith("<") and line.find(":") != -1:
                                [pay_key, pay_val] = line.strip().split(":", 1)
                                pay_key = pay_key.strip()
                                pay_val = pay_val.strip()
                                if pay_key in data:
                                    data[pay_key].append(pay_val)
                        data["mail_id"].append(mail_id.decode("UTF-8"))

    return data


def get_df(email_data):
    df = pd.DataFrame(email_data)
    df = df.rename(
        columns={
            "UPI Ref. No.": "upi_ref_id",
            "Amount": "amount",
            "From VPA": "sender_upi",
            "To VPA": "receiver_upi",
            "Payee Name": "payee_name",
            "Transaction Date": "transaction_date",
        }
    )
    df["amount"] = df["amount"].astype(float)
    df["mail_id"] = df["mail_id"].apply(lambda x: int(x))
    df["upi_ref_id"] = df["upi_ref_id"].apply(lambda x: int(x))

    # Subtracted time to maintain same datetime in Firebase
    df["transaction_date"] = df["transaction_date"].apply(
        lambda x: str(datetime.strptime(x, "%d/%m/%Y %H:%M:%S"))
    )

    return df
