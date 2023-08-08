import imaplib
from os import getenv

# USER = "myselfankit51@gmail.com"
# PWD = "uhqvnpjxtzilmzhe"
# CHECK_MAIL = "UPI@SC.COM"
USER = getenv("USER")
PWD = getenv("PWD")
SMTP_SERVER = "imap.gmail.com"
SMTP_PORT = 993
CHECK_MAIL = getenv("CHECK_MAIL")


def get_decoded_ids(mails):
    mail_ids = mails[1][0].split()
    if len(mail_ids) == 0:
        return []
    return mail_ids


def search(key, value, con):
    result, data = con.search(None, key, '"{}"'.format(value))
    return data


def get_all_mails(mail):
    all_data = mail.search(None, "FROM", f'"{CHECK_MAIL}"')
    return get_decoded_ids(all_data)


def get_unpublished_mails(mail, latest_date):
    # Convert timestamp to 'DD-MMM-YYYY' format
    formatted_dt = latest_date.strftime("%d-%b-%Y")
    print(formatted_dt)

    unpublished_mails = mail.search(
        None, "FROM", f'"{CHECK_MAIL}"', "SINCE", formatted_dt.upper()
    )

    return get_decoded_ids(unpublished_mails)


def get_mail():
    mail = imaplib.IMAP4_SSL(SMTP_SERVER)
    mail.login(USER, PWD)
    mail.select("INBOX", readonly=True)
    return mail


def search_inbox(latest_date="", fetch_type="all"):
    # Establish mail connection
    mail = get_mail()

    if fetch_type == "all":
        all_mail_ids = get_all_mails(mail)
        return all_mail_ids

    latest_mail_ids = get_unpublished_mails(mail, latest_date)
    return latest_mail_ids
