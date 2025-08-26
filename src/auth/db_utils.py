import sqlite3

# TODO convert to DB utils class?
users_db = 'users.db'
users_table = 'users'

def init_db():
    conn = sqlite3.connect(users_db)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {users_table}
        (
            email TEXT PRIMARY KEY,
            payment_txn_hash TEXT,
            payment_chain TEXT,
            bypass_payment BOOL,
            remarks TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()

def is_premium_user(email):
    """
    check if user is premium
    """
    conn = sqlite3.connect(users_db)
    c = conn.cursor()

    # TODO simplify logic, check for existence or length 1?
    c.execute(f"SELECT COUNT(*) FROM {users_table} WHERE email = ?", (email,))
    result = c.fetchone()
    conn.close()

    if result:
        email = result
        if email:
            return True
    return False

def upgrade_to_premium(email: str, payment_txn_hash: str, payment_chain: str):
    """
    only adds to DB if user has valid payment, else login does not affect persistent storage
    """
    if is_premium_user(email):
        return  # if somehow anyone pays again while being premium user, do nothing

    # TODO validate this and reload if needed after this is successful
    verify_valid_payment(email, payment_txn_hash, payment_chain)

    conn = sqlite3.connect(users_db)
    c = conn.cursor()
    c.execute(f"""
        UPDATE {users_table} SET email = ?, payment_txn_hash = ? payment_chain = ?
    """, (email, payment_txn_hash, payment_chain))
    conn.commit()
    conn.close()

def verify_valid_payment(email, payment_txn_hash: str, payment_chain: str):
    # this is assumed only callable if not premium, as not able to check twice
    # TODO check chain using rpc or alchemy methods
    # also check that is not already in DB else return error message
    # TODO