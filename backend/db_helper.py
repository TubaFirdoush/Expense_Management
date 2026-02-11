import mysql.connector
from contextlib import contextmanager
import logging_setup

from logging_setup import setup_logger

logger = setup_logger('db_helper')


@contextmanager
def get_db_cursor(commit=False):
    connection = mysql.connector.connect(
        host="localhost",
        port=3307,
        user="root",
        password="Root",
        database="expense_manager"
    )

    cursor = connection.cursor(dictionary=True)
    yield cursor
    if commit:
        connection.commit()
    print("Closing cursor")
    cursor.close()
    connection.close()


# def fetch_all_records():
#     query = "SELECT * from expenses"
#
#     with get_db_cursor() as cursor:
#         cursor.execute(query)
#         expenses = cursor.fetchall()
#         for expense in expenses:
#             print(expense)


def fetch_expenses_for_date(expense_date):
    logger.info(f"Fetching expenses for date {expense_date}")
    with get_db_cursor() as cursor:
        cursor.execute("SELECT * FROM expenses WHERE expense_date = %s", (expense_date,))
        expenses = cursor.fetchall()
        for expense in expenses:
            print(expense)
        return expenses

def insert_expense(expense_date, amount, category, notes):
    logger.info(f"Inserting expense for date: {expense_date}, amount: {amount}, category: {category}, notes: {notes}")
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            "INSERT INTO expenses (expense_date, amount, category, notes) VALUES (%s, %s, %s, %s)",
            (expense_date, amount, category, notes)
        )


def delete_expenses_for_date(expense_date):
    with get_db_cursor(commit=True) as cursor:
        cursor.execute("DELETE FROM expenses WHERE expense_date = %s", (expense_date,))


def fetch_expense_summary(start_date, end_date):
    logger.info(f"Fetching expense summary for date {start_date} to {end_date}")
    with get_db_cursor() as cursor:
        cursor.execute(
''' SELECT category, SUM(amount) as total 
             FROM expenses WHERE expense_date
             BETWEEN %s AND %s  
             GROUP BY category; ''',
    (start_date, end_date)
        )
        data = cursor.fetchall()
        return data

    '''
    SELECT category, SUM(amount) as total FROM expenses WHERE expense_date
    BETWEEN "%s" AND "%s" GROUP BY category;

    '''
import os
if __name__ == "__main__":

    # with get_db_cursor() as cursor:
    # fetch_all_records()
    # fetch_expenses_for_date("2024-08-01")
    # insert_expense("2024-08-25", 40, "Food", "Panipuri")
    # delete_expenses_for_date("2024-08-25")
    expenses = fetch_expenses_for_date("2024-07-30")
    print(expenses)
    summary = fetch_expense_summary("2024-08-01", "2024-08-05")
    for expense in summary:
        print(expense)
