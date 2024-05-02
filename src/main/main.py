import os
import sqlite3
import csv

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def setup_database():
    """Set up the SQLite database with the required tables."""
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
                        callId INTEGER PRIMARY KEY,
                        phoneNumber TEXT,
                        startTimeEpoch INTEGER,
                        endTimeEpoch INTEGER,
                        callDirection TEXT,
                        userId INTEGER,
                        FOREIGN KEY (userId) REFERENCES users(userId)
                    )'''
                   )


def load_and_clean_users(file_path):
    """Load user data from a CSV file into the users table."""
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) == 3 and all(row):
                cursor.execute('INSERT INTO users (userId, firstName, lastName) VALUES (NULL, ?, ?)', (row[0], row[1]))


def load_and_clean_call_logs(file_path):
    """Load call data from a CSV file into the callLogs table."""
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) == 6 and all(row):
                cursor.execute('''INSERT INTO callLogs (callId, phoneNumber, startTimeEpoch, endTimeEpoch, callDirection, userId)
                                 VALUES (NULL, ?, ?, ?, ?, ?)''', (row[0], row[1], row[2], row[3], row[4]))


def calculate_user_analytics():
    """Calculate user analytics: average call duration and number of calls per user."""
    user_stats = {}

    cursor.execute('''SELECT userId, AVG(endTimeEpoch - startTimeEpoch) AS avgDuration, COUNT(*) AS numCalls
                     FROM callLogs
                     GROUP BY userId''')
    rows = cursor.fetchall()
    for row in rows:
        user_id, avg_duration, num_calls = row
        user_stats[user_id] = {'avgDuration': avg_duration, 'numCalls': num_calls}

    return user_stats


def write_user_analytics(user_stats, file_path):
    """Save user analytics data to a CSV file."""
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['userId', 'avgDuration', 'numCalls']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for user_id, stats in user_stats.items():
            writer.writerow({'userId': user_id, 'avgDuration': stats['avgDuration'], 'numCalls': stats['numCalls']})


def save_ordered_call_logs_to_csv(file_path):
    """Save ordered call logs to a CSV file."""
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['callId', 'phoneNumber', 'startTimeEpoch', 'endTimeEpoch', 'callDirection', 'userId']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        cursor.execute('''SELECT * FROM callLogs ORDER BY userId, startTimeEpoch''')
        rows = cursor.fetchall()
        for row in rows:
            writer.writerow({'callId': row[0], 'phoneNumber': row[1], 'startTimeEpoch': row[2], 'endTimeEpoch': row[3], 'callDirection': row[4], 'userId': row[5]})


def write_ordered_calls(file_path):
    """Save ordered call logs to a CSV file."""
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['callId', 'phoneNumber', 'startTimeEpoch', 'endTimeEpoch', 'callDirection', 'userId']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        cursor.execute('''SELECT * FROM callLogs ORDER BY userId, startTimeEpoch''')
        rows = cursor.fetchall()
        for row in rows:
            writer.writerow({'callId': row[0], 'phoneNumber': row[1], 'startTimeEpoch': row[2], 'endTimeEpoch': row[3], 'callDirection': row[4], 'userId': row[5]})


def main():
    # Set up database
    setup_database()

    # Load user data
    user_data_path = os.path.join("resources", "users.csv")
    load_and_clean_users(user_data_path)

    # Load call data
    call_data_path = os.path.join("resources", "callLogs.csv")
    load_and_clean_call_logs(call_data_path)

    # Calculate user analytics
    user_stats = calculate_user_analytics()

    # Save user analytics to CSV
    user_analytics_path = os.path.join("resources", "testUserAnalytics.csv")
    write_user_analytics(user_stats, user_analytics_path)

    # Save ordered call logs to CSV
    ordered_call_logs_path = os.path.join("resources", "testOrderedCalls.csv")
    save_ordered_call_logs_to_csv(ordered_call_logs_path)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
