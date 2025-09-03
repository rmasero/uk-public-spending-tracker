import sqlite3

DB_NAME = "spend.db"

def detect_anomalies():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Large payments > £100k
    c.execute("SELECT id, council, supplier, amount_gbp, payment_date FROM payments WHERE amount_gbp > 100000")
    large_payments = c.fetchall()
    
    # Frequent payments >5 per month
    c.execute('''
        SELECT id, council, supplier, COUNT(*) as cnt, SUM(amount_gbp)
        FROM payments
        GROUP BY council, supplier, strftime('%Y-%m', payment_date)
        HAVING cnt > 5
    ''')
    frequent_payments = c.fetchall()
    
    conn.close()
    return large_payments, frequent_payments
