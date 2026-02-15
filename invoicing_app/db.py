import sqlite3

import click
from flask import current_app, g


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(current_app.config["DATABASE"])
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    with current_app.open_resource("schema.sql") as file:
        db.executescript(file.read().decode("utf8"))


def ensure_items_table():
    db = get_db()
    exists = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='items'"
    ).fetchone()

    if exists is None:
        db.execute(
            """
            CREATE TABLE items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              item_number TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL,
              price REAL,
              vat_amount REAL,
              unit TEXT,
              description TEXT,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        db.commit()
        return

    columns = db.execute("PRAGMA table_info(items)").fetchall()
    column_names = {column[1] for column in columns}
    price_not_null = any(column[1] == "price" and column[3] == 1 for column in columns)
    unit_not_null = any(column[1] == "unit" and column[3] == 1 for column in columns)

    needs_migration = "vat_amount" not in column_names or price_not_null or unit_not_null

    if needs_migration:
        db.executescript(
            """
            ALTER TABLE items RENAME TO items_old;

            CREATE TABLE items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              item_number TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL,
              price REAL,
              vat_amount REAL,
              unit TEXT,
              description TEXT,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            INSERT INTO items (id, item_number, name, price, vat_amount, unit, description, created_at)
            SELECT id, item_number, name, price, NULL, unit, description, created_at
            FROM items_old;

            DROP TABLE items_old;
            """
        )
        db.commit()
        return

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          item_number TEXT NOT NULL UNIQUE,
          name TEXT NOT NULL,
                    price REAL,
                    vat_amount REAL,
                    unit TEXT,
          description TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.commit()


def ensure_customers_table():
        db = get_db()
        db.execute(
                """
                CREATE TABLE IF NOT EXISTS customers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_number TEXT NOT NULL UNIQUE,
                    customer_type TEXT,
                    customer_name TEXT NOT NULL,
                    customer_tax_number TEXT,
                    registration_name TEXT,
                    phone_number TEXT,
                    address TEXT,
                    website TEXT,
                    country TEXT,
                    address_2 TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
        )

        columns = db.execute("PRAGMA table_info(customers)").fetchall()
        column_names = {column[1] for column in columns}
        if "customer_type" not in column_names:
            db.execute("ALTER TABLE customers ADD COLUMN customer_type TEXT")
            db.execute(
                "UPDATE customers SET customer_type = 'Company' WHERE customer_type IS NULL OR customer_type = ''"
            )
        db.commit()


def ensure_vendors_table():
    db = get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS vendors (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          vendor_number TEXT NOT NULL UNIQUE,
          vendor_type TEXT,
          vendor_name TEXT NOT NULL,
          vendor_tax_number TEXT,
          registration_name TEXT,
          phone_number TEXT,
          address TEXT,
          website TEXT,
          country TEXT,
          address_2 TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    columns = db.execute("PRAGMA table_info(vendors)").fetchall()
    column_names = {column[1] for column in columns}
    if "vendor_type" not in column_names:
        db.execute("ALTER TABLE vendors ADD COLUMN vendor_type TEXT")
        db.execute(
            "UPDATE vendors SET vendor_type = 'Company' WHERE vendor_type IS NULL OR vendor_type = ''"
        )
    db.commit()


def ensure_invoices_tables():
    db = get_db()
    db.execute("PRAGMA foreign_keys = ON")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS invoices (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          invoice_number TEXT NOT NULL UNIQUE,
          invoice_date TEXT NOT NULL,
          customer_id INTEGER,
          customer_name TEXT,
          customer_tax_number TEXT,
          registration_name TEXT,
          phone_number TEXT,
          address TEXT,
          website TEXT,
          country TEXT,
          address_2 TEXT,
          subtotal REAL NOT NULL DEFAULT 0,
          vat_total REAL NOT NULL DEFAULT 0,
          total REAL NOT NULL DEFAULT 0,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_items (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          invoice_id INTEGER NOT NULL,
          item_id INTEGER,
          item_name TEXT NOT NULL,
          quantity REAL NOT NULL DEFAULT 1,
          unit TEXT,
          price REAL NOT NULL DEFAULT 0,
          vat_amount REAL NOT NULL DEFAULT 0,
          line_total REAL NOT NULL DEFAULT 0,
          FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE
        )
        """
    )
    db.commit()


def ensure_purchase_invoices_tables():
        db = get_db()
        db.execute("PRAGMA foreign_keys = ON")
        db.execute(
                """
                CREATE TABLE IF NOT EXISTS purchase_invoices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    purchase_number TEXT NOT NULL UNIQUE,
                    purchase_date TEXT NOT NULL,
                    vendor_id INTEGER,
                    vendor_name TEXT,
                    vendor_tax_number TEXT,
                    registration_name TEXT,
                    phone_number TEXT,
                    address TEXT,
                    website TEXT,
                    country TEXT,
                    address_2 TEXT,
                    subtotal REAL NOT NULL DEFAULT 0,
                    vat_total REAL NOT NULL DEFAULT 0,
                    total REAL NOT NULL DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
        )
        db.execute(
                """
                CREATE TABLE IF NOT EXISTS purchase_invoice_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    purchase_invoice_id INTEGER NOT NULL,
                    item_id INTEGER,
                    item_name TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 1,
                    unit TEXT,
                    price REAL NOT NULL DEFAULT 0,
                    vat_amount REAL NOT NULL DEFAULT 0,
                    line_total REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY (purchase_invoice_id) REFERENCES purchase_invoices(id) ON DELETE CASCADE
                )
                """
        )
        db.commit()


def ensure_expenses_table():
        db = get_db()
        db.execute(
                """
                CREATE TABLE IF NOT EXISTS expenses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    expense_number TEXT NOT NULL UNIQUE,
                    expense_date TEXT NOT NULL,
                    title TEXT NOT NULL,
                    category TEXT,
                    payment_method_id INTEGER,
                    amount REAL NOT NULL DEFAULT 0,
                    notes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
        )

        columns = db.execute("PRAGMA table_info(expenses)").fetchall()
        column_names = {column[1] for column in columns}
        if "payment_method_id" not in column_names:
            db.execute("ALTER TABLE expenses ADD COLUMN payment_method_id INTEGER")

        db.commit()


def ensure_payment_tables():
        db = get_db()
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS payment_methods (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              method_type TEXT NOT NULL,
              account_identifier TEXT,
              details TEXT,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS payment_currencies (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              code TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL,
              symbol TEXT,
              is_crypto INTEGER NOT NULL DEFAULT 0,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS payment_transactions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              transaction_date TEXT NOT NULL,
              transaction_type TEXT NOT NULL,
              reference_type TEXT,
              reference_id INTEGER,
              amount REAL NOT NULL,
              currency_code TEXT NOT NULL,
              method_id INTEGER,
              notes TEXT,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        defaults = [
            ("USD", "US Dollar", "$", 0),
            ("EUR", "Euro", "€", 0),
            ("TRY", "Turkish Lira", "₺", 0),
            ("AED", "Dirham", "د.إ", 0),
            ("BTC", "Bitcoin", "₿", 1),
            ("ETH", "Ethereum", "Ξ", 1),
        ]
        for code, name, symbol, is_crypto in defaults:
            db.execute(
                """
                INSERT OR IGNORE INTO payment_currencies (code, name, symbol, is_crypto)
                VALUES (?, ?, ?, ?)
                """,
                (code, name, symbol, is_crypto),
            )

        db.commit()


def ensure_users_table():
        db = get_db()
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT NOT NULL UNIQUE,
              full_name TEXT,
              password_hash TEXT NOT NULL,
              is_active INTEGER NOT NULL DEFAULT 1,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        columns = db.execute("PRAGMA table_info(users)").fetchall()
        column_names = {column[1] for column in columns}
        if "full_name" not in column_names:
            db.execute("ALTER TABLE users ADD COLUMN full_name TEXT")
        if "is_active" not in column_names:
            db.execute("ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")

        db.commit()


@click.command("init-db")
def init_db_command():
    init_db()
    click.echo("Initialized the database.")


def init_app(app):
    app.cli.add_command(init_db_command)
