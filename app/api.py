from datetime import datetime
from fastapi import APIRouter, HTTPException
from psycopg2 import DatabaseError, OperationalError, InterfaceError
from contextlib import closing
from app.db import get_connection, release_connection, init_db
from app.mail.parse_email import get_mail_dataframe, get_parsed_emails
from app.mail.search_inbox import get_mail_ids

router = APIRouter()


# Fetch all transactions from transactions table
# This endpoint will not be used in the client. This is just for testing purpose
@router.get("/transactions")
def get_transactions():
    conn = get_connection()
    try:
        with closing(conn.cursor()) as cursor:
            cursor.execute("SELECT * FROM transactions")
            transactions = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            return [dict(zip(column_names, row)) for row in transactions]
    except DatabaseError as e:
        print(f"Error fetching transactions: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        release_connection(conn)


# Generic function to process transactions from the mailbox
# This can be used for fetching latest transactions or doing a full refresh
def process_transactions(cursor, mail_df=None):
    if mail_df is not None and mail_df.empty:
        return

    # Get distinct receiver_id and receiver_name from mail_df dataframe
    # Sort by transaction_date so that old names are added first and then if it has updated, it can get updated
    distinct_receivers = mail_df.sort_values("transaction_date")[
        ["receiver_upi", "receiver_name"]
    ].drop_duplicates()

    # Convert dataframe to list of tuples containing two values - receiver_id and receiver_name
    receiver_data = list(distinct_receivers.itertuples(index=False, name=None))

    # Insert receivers into "receiver" table
    # ON CONFLICT - this will update the name with the latest name if a receiver_upi is matched
    cursor.executemany(
        """
            INSERT INTO receiver (receiver_upi, receiver_name, category_id)
            VALUES (%s, %s, 0)
            ON CONFLICT (receiver_upi) DO UPDATE
            SET receiver_name = EXCLUDED.receiver_name
        """,
        receiver_data,
    )

    # Fetch receiver_id mappings
    receiver_mapping = {}

    # Get list of receivers that were newly added into receiver table
    receiver_upis = distinct_receivers["receiver_upi"].tolist()
    if receiver_upis:
        # Fetch the receiver_id for all receiver_upi(s) added newly in receiver table
        cursor.execute(
            """
            SELECT receiver_id, receiver_upi FROM receiver where receiver_upi = ANY(%s)
            """,
            (receiver_upis,),
        )
        # Set key as receiver_upi and value as receiver_id in receiver_mapping dictionary
        receiver_mapping = {row[1]: row[0] for row in cursor.fetchall()}

    # Prepare transactions
    transaction_data = []
    for _, row in mail_df.iterrows():
        # Get receiver_id processed above
        receiver_id = receiver_mapping.get(row.receiver_upi)
        # Append tuple of transaction in transaction_data list
        transaction_data.append(
            (
                row.upi_ref_no,
                row.amount,
                row.sender_upi,
                receiver_id,
                row.transaction_date,
                "UPI",
                None,  # category_id
                0,  # is_overwritten_category
            )
        )

    # Insert all transactions. If any repetitive upi_ref_no is found, do nothing and continue insert operation
    cursor.executemany(
        """
        INSERT INTO transactions (upi_ref_no, amount, sender_upi, receiver_id, transaction_date, payment_mode, category_id, is_category_overwritten)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (upi_ref_no) DO NOTHING
    """,
        transaction_data,
    )


# Clears and reloads the receiver & transactions tables from scratch
@router.post("/full-refresh-transactions")
def full_refresh_transactions():
    mail_ids = get_mail_ids()
    parsed_mail_data = get_parsed_emails(mail_ids)
    mail_df = get_mail_dataframe(parsed_mail_data)

    # Connect to DB as above operation can take atleast 10 minutes
    # Hence this endpoint will be called rarely
    init_db()

    # Get connection to DB
    conn = get_connection()
    try:
        # Close connection once DB transaction is completed
        with closing(conn.cursor()) as cursor:
            # Truncate transaction and receiver table
            cursor.execute(
                "TRUNCATE TABLE transactions, receiver RESTART IDENTITY CASCADE;"
            )
            # Commit above operation
            conn.commit()

            # Process all transactions. Pass the cursor and mail_df as parameters
            process_transactions(cursor, mail_df)

            # Above function had some insert operations. Commit those changes
            conn.commit()

            # Return success response if everything went fine
            return {"status": "ok", "message": "Full refresh completed successfully"}
    except (OperationalError, InterfaceError) as e:
        # Rollback in case some error occured during transaction
        if conn and not conn.closed:
            conn.rollback()

        # Return failure response
        return {"status": "error", "message": "Database connection lost. Please retry."}
    except Exception as e:
        if conn and not conn.closed:
            conn.rollback()
        print(f"Error occurred: {e}")
        raise e
    finally:
        release_connection(conn)


@router.post("/new-transactions")
def populate_new_transactions():
    # Get DB connection
    conn = get_connection()
    try:
        with closing(conn.cursor()) as cursor:
            # Get latest transaction date before fetching emails
            cursor.execute("SELECT MAX(transaction_date) FROM transactions")
            max_transaction_date = cursor.fetchone()[0]
    except Exception as e:
        release_connection(conn)
        return {
            "status": "error",
            "message": "Something went wrong",
        }
        raise e
    finally:
        # Release connection
        release_connection(conn)

    if max_transaction_date is None:
        max_transaction_date = datetime(2023, 1, 1)

    # Read latest transactions based on max_transaction_date passed
    mail_ids = get_mail_ids(latest_date=max_transaction_date)

    # Get mail data in dictionary
    parsed_mail_data = get_parsed_emails(mail_ids)

    # Convert mail in dictionary format to a dataframe
    mail_df = get_mail_dataframe(parsed_mail_data)

    if mail_df.empty:
        return {"response": "ok", "message": "No new transactions found."}

    # Connect to DB as above operation just to be safe DB is not closed due to being idle for long time
    init_db()

    # Reconnect to database
    conn = get_connection()
    try:
        with closing(conn.cursor()) as cursor:
            process_transactions(cursor, mail_df)
            conn.commit()

            return {
                "status": "ok",
                "message": "Incremental transactions processed successfully",
            }
    except (OperationalError, InterfaceError) as e:
        if conn and not conn.closed:
            conn.rollback()
        return {
            "status": "error",
            "message": "Database connection lost. Please retry",
        }
    except Exception as e:
        if conn and not conn.closed:
            conn.rollback()
        print(f"Error occurred: {e}")
        raise e
    finally:
        release_connection(conn)
