from database import *
from dotenv import load_dotenv
from pathlib import Path
import uuid

load_dotenv(Path(__file__).parent / ".env", override=True)

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from databricks_client import get_connection

app = FastAPI(title="Global Bank API")

# ----------------------------
# CORS
# ----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Models
# ----------------------------
class Transaction(BaseModel):
    user_id: int
    currency: str
    amount: float = Field(..., gt=0)

class Transfer(BaseModel):
    from_user: int
    to_user: int
    currency: str
    amount: float = Field(..., gt=0)

# ----------------------------
# Helpers
# ----------------------------
def ensure_wallet(cur, user_id, currency):

    cur.execute("""
        SELECT balance
        FROM banking.bank_core.wallets
        WHERE user_id = ? AND currency = ?
    """, (user_id, currency))

    row = cur.fetchone()

    if not row:
        print(f"💡 Creating {currency} wallet for User {user_id}")

        cur.execute("""
            INSERT INTO banking.bank_core.wallets
            (user_id, currency, balance, updated_at)
            VALUES (?, ?, 0, current_timestamp())
        """, (user_id, currency))

        return 0

    return float(row[0])


def record_txn(cur,
               from_user,
               to_user,
               from_currency,
               to_currency,
               from_amount,
               to_amount,
               fx_rate=1):

    txn_id = str(uuid.uuid4())

    cur.execute("""
        INSERT INTO banking.bank_core.transactions
        (txn_id, from_user, to_user,
         from_currency, to_currency,
         from_amount, to_amount,
         fx_rate, txn_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp())
    """, (
        txn_id,
        from_user,
        to_user,
        from_currency,
        to_currency,
        from_amount,
        to_amount,
        fx_rate
    ))

# ----------------------------
# Health
# ----------------------------
@app.get("/")
def health():
    return {"status": "API running"}

# ----------------------------
# BALANCE
# ----------------------------
@app.get("/balance/{user_id}")
def get_balance(user_id: int):

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                WITH txn_impact AS (

                    SELECT
                        from_user AS user_id,
                        from_currency AS currency,
                        -from_amount AS amount
                    FROM banking.bank_core.transactions
                    WHERE from_user IS NOT NULL

                    UNION ALL

                    SELECT
                        to_user AS user_id,
                        to_currency AS currency,
                        to_amount AS amount
                    FROM banking.bank_core.transactions
                    WHERE to_user IS NOT NULL
                )

                SELECT
                    currency,
                    SUM(amount) AS balance
                FROM txn_impact
                WHERE user_id = ?
                GROUP BY currency
            """, (user_id,))

            rows = cur.fetchall()

        if not rows:
            raise HTTPException(404, "User not found")

        return {
            "user_id": user_id,
            "wallets": [
                {"currency": r[0], "balance": float(r[1] or 0)}
                for r in rows
            ]
        }

    finally:
        conn.close()

# ----------------------------
# DEPOSIT
# ----------------------------
@app.post("/deposit")
def deposit(tx: Transaction):

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            ensure_wallet(cur, tx.user_id, tx.currency)

            cur.execute("""
                UPDATE banking.bank_core.wallets
                SET balance = balance + ?
                WHERE user_id = ? AND currency = ?
            """, (tx.amount, tx.user_id, tx.currency))

            record_txn(
                cur,
                from_user=None,
                to_user=tx.user_id,
                from_currency=tx.currency,
                to_currency=tx.currency,
                from_amount=0,
                to_amount=tx.amount
            )

            cur.execute("""
                SELECT balance
                FROM banking.bank_core.wallets
                WHERE user_id = ? AND currency = ?
            """, (tx.user_id, tx.currency))

            row = cur.fetchone()

            if not row:
                raise HTTPException(500, "Balance fetch failed")

        conn.commit()

        return {
            "status": "SUCCESS",
            "currency": tx.currency,
            "new_balance": float(row[0])
        }

    finally:
        conn.close()

# ----------------------------
# WITHDRAW
# ----------------------------
@app.post("/withdraw")
def withdraw(tx: Transaction):

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT balance
                FROM banking.bank_core.wallets
                WHERE user_id = ? AND currency = ?
            """, (tx.user_id, tx.currency))

            row = cur.fetchone()

            if not row:
                raise HTTPException(400, "Wallet not found")

            balance = float(row[0])

            if balance < tx.amount:
                raise HTTPException(400, "Insufficient funds")

            cur.execute("""
                UPDATE banking.bank_core.wallets
                SET balance = balance - ?
                WHERE user_id = ? AND currency = ?
            """, (tx.amount, tx.user_id, tx.currency))

            record_txn(
                cur,
                from_user=tx.user_id,
                to_user=None,
                from_currency=tx.currency,
                to_currency=tx.currency,
                from_amount=tx.amount,
                to_amount=0
            )

            cur.execute("""
                SELECT balance
                FROM banking.bank_core.wallets
                WHERE user_id = ? AND currency = ?
            """, (tx.user_id, tx.currency))

            row = cur.fetchone()

            if not row:
                raise HTTPException(500, "Balance fetch failed")

        conn.commit()

        return {
            "status": "SUCCESS",
            "currency": tx.currency,
            "new_balance": float(row[0])
        }

    finally:
        conn.close()

# ----------------------------
# TRANSFER
# ----------------------------
@app.post("/transfer")
def transfer(tx: Transfer):

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT balance
                FROM banking.bank_core.wallets
                WHERE user_id = ? AND currency = ?
            """, (tx.from_user, tx.currency))

            sender = cur.fetchone()

            if not sender:
                raise HTTPException(400, "Sender wallet not found")

            sender_balance = float(sender[0])

            if sender_balance < tx.amount:
                raise HTTPException(400, "Insufficient funds")

            ensure_wallet(cur, tx.to_user, tx.currency)

            cur.execute("""
                UPDATE banking.bank_core.wallets
                SET balance = balance - ?
                WHERE user_id = ? AND currency = ?
            """, (tx.amount, tx.from_user, tx.currency))

            cur.execute("""
                UPDATE banking.bank_core.wallets
                SET balance = balance + ?
                WHERE user_id = ? AND currency = ?
            """, (tx.amount, tx.to_user, tx.currency))

            record_txn(
                cur,
                from_user=tx.from_user,
                to_user=tx.to_user,
                from_currency=tx.currency,
                to_currency=tx.currency,
                from_amount=tx.amount,
                to_amount=tx.amount
            )

        conn.commit()

        return {
            "status": "SUCCESS",
            "currency": tx.currency,
            "message": "Transfer completed"
        }

    finally:
        conn.close()

# ----------------------------
# TRANSACTIONS
# ----------------------------
@app.get("/transactions/{user_id}")
def get_transactions(user_id: int):

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""

                WITH ledger_view AS (

                    SELECT
                        txn_id,
                        txn_timestamp,
                        from_user AS user_id,
                        from_currency AS currency,
                        -from_amount AS amount
                    FROM banking.bank_core.transactions
                    WHERE from_user IS NOT NULL

                    UNION ALL

                    SELECT
                        txn_id,
                        txn_timestamp,
                        to_user AS user_id,
                        to_currency AS currency,
                        to_amount AS amount
                    FROM banking.bank_core.transactions
                    WHERE to_user IS NOT NULL
                ),

                balance_calc AS (

                    SELECT
                        txn_id,
                        txn_timestamp,
                        user_id,
                        currency,
                        amount,

                        SUM(amount) OVER (
                            PARTITION BY user_id, currency
                            ORDER BY txn_timestamp, txn_id
                        ) AS running_balance

                    FROM ledger_view
                )

                SELECT
                    txn_id,
                    currency,
                    amount,
                    running_balance,
                    txn_timestamp
                FROM balance_calc
                WHERE user_id = ?
                ORDER BY txn_timestamp DESC

            """, (user_id,))

            rows = cur.fetchall() 


        txns = []

        for r in rows:
            amount = float(r[2])
            balance = float(r[3])

            txns.append({
                "txn_id": r[0],
                "user_id": user_id,
                "from": r[1],
                "to": r[1],
                "prev_balance": balance - amount,
                "debit": abs(amount) if amount < 0 else 0,
                "credit": amount if amount > 0 else 0,
                "new_balance": balance,
                "time": str(r[4])
            })

        return {"transactions": txns}

    finally:
        conn.close()