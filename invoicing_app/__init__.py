import os
import json
import shutil
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from flask import Flask, redirect, render_template, request, url_for, send_file, session
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from werkzeug.security import check_password_hash, generate_password_hash

from .db import (
    close_db,
    ensure_customers_table,
    ensure_expenses_table,
    ensure_invoices_tables,
    ensure_items_table,
    ensure_payment_tables,
    ensure_purchase_invoices_tables,
    ensure_users_table,
    ensure_vendors_table,
    get_db,
    init_app as init_db_app,
)


class AppUser(UserMixin):
    def __init__(self, row):
        self.id = str(row["id"])
        self.username = row["username"]
        self.full_name = row["full_name"]
        self._is_active = bool(row["is_active"])

    @property
    def is_active(self):
        return self._is_active


def _ensure_default_admin(db):
    existing = db.execute("SELECT id FROM users LIMIT 1").fetchone()
    if existing is not None:
        return

    db.execute(
        """
        INSERT INTO users (username, full_name, password_hash, is_active)
        VALUES (?, ?, ?, ?)
        """,
        (
            "admin",
            "Administrator",
            generate_password_hash("admin123"),
            1,
        ),
    )
    db.commit()


def _generate_item_number(db):
    for _ in range(10):
        item_number = f"ITM-{uuid.uuid4().hex[:8].upper()}"
        exists = db.execute(
            "SELECT 1 FROM items WHERE item_number = ? LIMIT 1", (item_number,)
        ).fetchone()
        if exists is None:
            return item_number
    raise RuntimeError("Could not generate a unique item number")


def _generate_customer_number(db):
    for _ in range(10):
        customer_number = f"CUS-{uuid.uuid4().hex[:8].upper()}"
        exists = db.execute(
            "SELECT 1 FROM customers WHERE customer_number = ? LIMIT 1", (customer_number,)
        ).fetchone()
        if exists is None:
            return customer_number
    raise RuntimeError("Could not generate a unique customer number")


def _generate_vendor_number(db):
    for _ in range(10):
        vendor_number = f"VEN-{uuid.uuid4().hex[:8].upper()}"
        exists = db.execute(
            "SELECT 1 FROM vendors WHERE vendor_number = ? LIMIT 1", (vendor_number,)
        ).fetchone()
        if exists is None:
            return vendor_number
    raise RuntimeError("Could not generate a unique vendor number")


def _generate_invoice_number(db):
    for _ in range(10):
        invoice_number = f"INV-{uuid.uuid4().hex[:8].upper()}"
        exists = db.execute(
            "SELECT 1 FROM invoices WHERE invoice_number = ? LIMIT 1", (invoice_number,)
        ).fetchone()
        if exists is None:
            return invoice_number
    raise RuntimeError("Could not generate a unique invoice number")


def _ensure_unique_invoice_number(db, desired_number, exclude_invoice_id=None):
    candidate = (desired_number or "").strip()
    if not candidate:
        return _generate_invoice_number(db)

    if exclude_invoice_id is None:
        existing = db.execute(
            "SELECT id FROM invoices WHERE invoice_number = ? LIMIT 1", (candidate,)
        ).fetchone()
    else:
        existing = db.execute(
            "SELECT id FROM invoices WHERE invoice_number = ? AND id != ? LIMIT 1",
            (candidate, exclude_invoice_id),
        ).fetchone()

    if existing is None:
        return candidate
    return _generate_invoice_number(db)


def _to_decimal_or_default(value, default="0"):
    raw_value = (value or "").strip()
    if raw_value == "":
        raw_value = default
    try:
        return Decimal(raw_value)
    except InvalidOperation:
        return Decimal(default)


def _get_default_currency_code(db):
    preferred = db.execute(
        """
        SELECT code
        FROM payment_currencies
        WHERE is_crypto = 0
        ORDER BY CASE code
            WHEN 'TRY' THEN 0
            WHEN 'USD' THEN 1
            WHEN 'EUR' THEN 2
            ELSE 9
        END, code
        LIMIT 1
        """
    ).fetchone()
    if preferred is not None and preferred["code"]:
        return preferred["code"]

    fallback = db.execute(
        "SELECT code FROM payment_currencies ORDER BY code LIMIT 1"
    ).fetchone()
    if fallback is not None and fallback["code"]:
        return fallback["code"]

    return "USD"


def _generate_purchase_number(db):
    for _ in range(10):
        purchase_number = f"PUR-{uuid.uuid4().hex[:8].upper()}"
        exists = db.execute(
            "SELECT 1 FROM purchase_invoices WHERE purchase_number = ? LIMIT 1", (purchase_number,)
        ).fetchone()
        if exists is None:
            return purchase_number
    raise RuntimeError("Could not generate a unique purchase number")


def _ensure_unique_purchase_number(db, desired_number, exclude_purchase_id=None):
    candidate = (desired_number or "").strip()
    if not candidate:
        return _generate_purchase_number(db)

    if exclude_purchase_id is None:
        existing = db.execute(
            "SELECT id FROM purchase_invoices WHERE purchase_number = ? LIMIT 1", (candidate,)
        ).fetchone()
    else:
        existing = db.execute(
            "SELECT id FROM purchase_invoices WHERE purchase_number = ? AND id != ? LIMIT 1",
            (candidate, exclude_purchase_id),
        ).fetchone()

    if existing is None:
        return candidate
    return _generate_purchase_number(db)


def _asset_key(item_id, item_name):
    if item_id is not None:
        return f"id:{item_id}"
    normalized_name = (item_name or "").strip().lower()
    return f"name:{normalized_name}"


def _generate_expense_number(db):
    for _ in range(10):
        expense_number = f"EXP-{uuid.uuid4().hex[:8].upper()}"
        exists = db.execute(
            "SELECT 1 FROM expenses WHERE expense_number = ? LIMIT 1", (expense_number,)
        ).fetchone()
        if exists is None:
            return expense_number
    raise RuntimeError("Could not generate a unique expense number")


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    from datetime import timedelta
    app.config.from_mapping(
        SECRET_KEY="dev",
        DATABASE=os.path.join(app.instance_path, "invoicing.sqlite"),
        PERMANENT_SESSION_LIFETIME=timedelta(minutes=30),
    )
    @app.before_request
    def make_session_permanent():
        session.permanent = True

    if test_config is None:
        app.config.from_pyfile("config.py", silent=True)
    else:
        app.config.update(test_config)

    try:
        os.makedirs(app.instance_path, exist_ok=True)
    except OSError:
        pass

    init_db_app(app)
    app.teardown_appcontext(close_db)

    with app.app_context():
        ensure_items_table()
        ensure_customers_table()
        ensure_vendors_table()
        ensure_invoices_tables()
        ensure_purchase_invoices_tables()
        ensure_expenses_table()
        ensure_payment_tables()
        ensure_users_table()
        _ensure_default_admin(get_db())

    login_manager = LoginManager()
    login_manager.login_view = "login"
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        db = get_db()
        ensure_users_table()
        row = db.execute(
            """
            SELECT id, username, full_name, is_active
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()
        if row is None:
            return None
        return AppUser(row)

    @app.before_request
    def require_login_for_app_pages():
        allowed_endpoints = {
            "login",
            "static",
        }
        if request.endpoint in allowed_endpoints:
            return None
        if request.endpoint is None:
            return None
        if current_user.is_authenticated:
            return None
        return redirect(url_for("login"))

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if current_user.is_authenticated:
            return redirect(url_for("dashboard"))

        if request.method == "POST":
            db = get_db()
            ensure_users_table()

            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")

            user_row = db.execute(
                """
                SELECT id, username, full_name, password_hash, is_active
                FROM users
                WHERE username = ?
                """,
                (username,),
            ).fetchone()

            if (
                user_row is not None
                and user_row["is_active"]
                and check_password_hash(user_row["password_hash"], password)
            ):
                login_user(AppUser(user_row), remember=True)
                return redirect(url_for("dashboard"))

            return render_template(
                "login.html",
                page_title="Login",
                active_menu="",
                error_message="Invalid username or password.",
            )

        return render_template(
            "login.html",
            page_title="Login",
            active_menu="",
            error_message="",
        )

    @app.post("/logout")
    @login_required
    def logout():
        logout_user()
        return redirect(url_for("login"))

    @app.route("/users")
    @login_required
    def users_page():
        db = get_db()
        ensure_users_table()

        users = db.execute(
            """
            SELECT id, username, full_name, is_active, created_at
            FROM users
            ORDER BY id DESC
            """
        ).fetchall()

        return render_template(
            "users.html",
            page_title="Users",
            active_menu="Users",
            users=users,
        )

    @app.post("/users/add")
    @login_required
    def add_user():
        db = get_db()
        ensure_users_table()

        username = request.form.get("username", "").strip()
        full_name = request.form.get("full_name", "").strip() or None
        password = request.form.get("password", "")
        is_active = 1 if request.form.get("is_active") == "on" else 0

        if not username or not password:
            return redirect(url_for("users_page"))

        existing = db.execute(
            "SELECT id FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if existing is not None:
            return redirect(url_for("users_page"))

        db.execute(
            """
            INSERT INTO users (username, full_name, password_hash, is_active)
            VALUES (?, ?, ?, ?)
            """,
            (username, full_name, generate_password_hash(password), is_active),
        )
        db.commit()
        return redirect(url_for("users_page"))

    @app.post("/users/edit")
    @login_required
    def edit_user():
        db = get_db()
        ensure_users_table()

        user_id = request.form.get("user_id", "").strip()
        username = request.form.get("username", "").strip()
        full_name = request.form.get("full_name", "").strip() or None
        is_active = 1 if request.form.get("is_active") == "on" else 0

        if not user_id or not username:
            return redirect(url_for("users_page"))

        try:
            parsed_user_id = int(user_id)
        except ValueError:
            return redirect(url_for("users_page"))

        existing = db.execute(
            "SELECT id FROM users WHERE username = ? AND id != ?",
            (username, parsed_user_id),
        ).fetchone()
        if existing is not None:
            return redirect(url_for("users_page"))

        db.execute(
            """
            UPDATE users
            SET username = ?, full_name = ?, is_active = ?
            WHERE id = ?
            """,
            (username, full_name, is_active, parsed_user_id),
        )
        db.commit()
        return redirect(url_for("users_page"))

    @app.post("/users/delete")
    @login_required
    def delete_user():
        db = get_db()
        ensure_users_table()

        user_id = request.form.get("user_id", "").strip()
        try:
            parsed_user_id = int(user_id)
        except ValueError:
            return redirect(url_for("users_page"))

        if str(parsed_user_id) == current_user.get_id():
            return redirect(url_for("users_page"))

        db.execute("DELETE FROM users WHERE id = ?", (parsed_user_id,))
        db.commit()
        return redirect(url_for("users_page"))

    @app.post("/users/reset-password")
    @login_required
    def reset_user_password():
        db = get_db()
        ensure_users_table()

        user_id = request.form.get("user_id", "").strip()
        new_password = request.form.get("new_password", "")
        if not user_id or not new_password:
            return redirect(url_for("users_page"))

        try:
            parsed_user_id = int(user_id)
        except ValueError:
            return redirect(url_for("users_page"))

        db.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (generate_password_hash(new_password), parsed_user_id),
        )
        db.commit()
        return redirect(url_for("users_page"))

    @app.route("/settings")
    @login_required
    def settings_page():
        status = request.args.get("status", "").strip()
        return render_template(
            "settings.html",
            page_title="Settings",
            active_menu="Settings",
            status=status,
        )

    @app.get("/settings/backup")
    @login_required
    def backup_database():
        database_path = app.config["DATABASE"]
        if not os.path.exists(database_path):
            return redirect(url_for("settings_page", status="backup-missing"))

        filename = f"invoicing-backup-{date.today().isoformat()}.sqlite"
        return send_file(database_path, as_attachment=True, download_name=filename)

    @app.post("/settings/restore")
    @login_required
    def restore_database():
        uploaded_file = request.files.get("backup_file")
        if uploaded_file is None or uploaded_file.filename is None or uploaded_file.filename.strip() == "":
            return redirect(url_for("settings_page", status="restore-no-file"))

        database_path = app.config["DATABASE"]
        os.makedirs(os.path.dirname(database_path), exist_ok=True)

        if os.path.exists(database_path):
            backups_dir = os.path.join(app.instance_path, "backups")
            os.makedirs(backups_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            safety_backup_path = os.path.join(backups_dir, f"pre-restore-{timestamp}.sqlite")
            shutil.copy2(database_path, safety_backup_path)

        temp_path = database_path + ".restore"
        with open(temp_path, "wb") as handle:
            shutil.copyfileobj(uploaded_file.stream, handle)

        close_db()
        os.replace(temp_path, database_path)

        db = get_db()
        ensure_users_table()
        _ensure_default_admin(db)

        return redirect(url_for("settings_page", status="restore-ok"))

    @app.route("/")
    def index():
        if not current_user.is_authenticated:
            return redirect(url_for("login"))
        return redirect(url_for("dashboard"))

    @app.route("/dashboard")
    @login_required
    def dashboard():
        db = get_db()
        ensure_invoices_tables()
        ensure_purchase_invoices_tables()
        ensure_expenses_table()
        ensure_payment_tables()

        def _previous_period_bounds(selected_period, current_start):
            if selected_period == "year":
                prev_start = date(current_start.year - 1, 1, 1)
                prev_end = date(current_start.year - 1, 12, 31)
                return prev_start, prev_end

            if selected_period == "quarter":
                current_quarter = ((current_start.month - 1) // 3) + 1
                if current_quarter == 1:
                    prev_year = current_start.year - 1
                    prev_quarter = 4
                else:
                    prev_year = current_start.year
                    prev_quarter = current_quarter - 1
                prev_start_month = (prev_quarter - 1) * 3 + 1
                prev_start = date(prev_year, prev_start_month, 1)
                prev_end = date(prev_year, prev_start_month + 2, 1)
                if prev_start_month + 2 == 12:
                    prev_end = date(prev_year, 12, 31)
                else:
                    prev_end = date(prev_year, prev_start_month + 3, 1) - timedelta(days=1)
                return prev_start, prev_end

            if current_start.month == 1:
                prev_start = date(current_start.year - 1, 12, 1)
                prev_end = date(current_start.year, 1, 1) - timedelta(days=1)
            else:
                prev_start = date(current_start.year, current_start.month - 1, 1)
                prev_end = date(current_start.year, current_start.month, 1) - timedelta(days=1)
            return prev_start, prev_end

        def _trend(current_value, previous_value, positive_is_good=True):
            current_decimal = Decimal(str(current_value))
            previous_decimal = Decimal(str(previous_value))
            diff = current_decimal - previous_decimal

            if diff == 0:
                return {
                    "label": "No change",
                    "icon": "→",
                    "css": "text-muted",
                }

            if previous_decimal == 0:
                sign = "+" if diff > 0 else "-"
                direction_icon = "↑" if diff > 0 else "↓"
                is_good = (diff > 0 and positive_is_good) or (diff < 0 and not positive_is_good)
                return {
                    "label": f"{sign}{abs(float(diff)):.2f} vs prev",
                    "icon": direction_icon,
                    "css": "text-success" if is_good else "text-danger",
                }

            percentage = (diff / previous_decimal) * Decimal("100")
            direction_icon = "↑" if diff > 0 else "↓"
            is_good = (diff > 0 and positive_is_good) or (diff < 0 and not positive_is_good)
            sign = "+" if percentage > 0 else ""
            return {
                "label": f"{sign}{float(percentage):.1f}% vs prev",
                "icon": direction_icon,
                "css": "text-success" if is_good else "text-danger",
            }

        period = request.args.get("period", "month").strip().lower()
        if period not in {"month", "quarter", "year"}:
            period = "month"

        today = date.today()
        if period == "year":
            start_date = date(today.year, 1, 1)
            period_label = f"This Year ({today.year})"
        elif period == "quarter":
            quarter_start_month = ((today.month - 1) // 3) * 3 + 1
            start_date = date(today.year, quarter_start_month, 1)
            period_label = f"This Quarter (Q{((today.month - 1) // 3) + 1} {today.year})"
        else:
            start_date = date(today.year, today.month, 1)
            period_label = today.strftime("This Month (%b %Y)")

        start_iso = start_date.isoformat()
        end_iso = today.isoformat()

        previous_start, previous_end = _previous_period_bounds(period, start_date)
        previous_start_iso = previous_start.isoformat()
        previous_end_iso = previous_end.isoformat()

        sales_total_raw = db.execute(
            """
            SELECT COALESCE(SUM(total), 0) AS total
            FROM invoices
            WHERE invoice_date >= ? AND invoice_date <= ?
            """,
            (start_iso, end_iso),
        ).fetchone()
        purchase_total_raw = db.execute(
            """
            SELECT COALESCE(SUM(total), 0) AS total
            FROM purchase_invoices
            WHERE purchase_date >= ? AND purchase_date <= ?
            """,
            (start_iso, end_iso),
        ).fetchone()
        expense_total_raw = db.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM expenses
            WHERE expense_date >= ? AND expense_date <= ?
            """,
            (start_iso, end_iso),
        ).fetchone()

        sales_total = Decimal(str(sales_total_raw["total"] or 0))
        purchase_total = Decimal(str(purchase_total_raw["total"] or 0))
        expense_total = Decimal(str(expense_total_raw["total"] or 0))
        net_profit = sales_total - purchase_total - expense_total

        prev_sales_raw = db.execute(
            """
            SELECT COALESCE(SUM(total), 0) AS total
            FROM invoices
            WHERE invoice_date >= ? AND invoice_date <= ?
            """,
            (previous_start_iso, previous_end_iso),
        ).fetchone()
        prev_purchases_raw = db.execute(
            """
            SELECT COALESCE(SUM(total), 0) AS total
            FROM purchase_invoices
            WHERE purchase_date >= ? AND purchase_date <= ?
            """,
            (previous_start_iso, previous_end_iso),
        ).fetchone()
        prev_expenses_raw = db.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM expenses
            WHERE expense_date >= ? AND expense_date <= ?
            """,
            (previous_start_iso, previous_end_iso),
        ).fetchone()

        prev_sales_total = Decimal(str(prev_sales_raw["total"] or 0))
        prev_purchase_total = Decimal(str(prev_purchases_raw["total"] or 0))
        prev_expense_total = Decimal(str(prev_expenses_raw["total"] or 0))
        prev_net_profit = prev_sales_total - prev_purchase_total - prev_expense_total

        receivables_raw = db.execute(
            """
            SELECT COALESCE(SUM(i.total), 0) - COALESCE(SUM(r.received_amount), 0) AS outstanding
            FROM invoices i
            LEFT JOIN (
                SELECT reference_id, SUM(amount) AS received_amount
                FROM payment_transactions
                WHERE reference_type = 'invoice'
                  AND transaction_type = 'invoice_receipt'
                GROUP BY reference_id
            ) r ON r.reference_id = i.id
            """
        ).fetchone()
        payables_raw = db.execute(
            """
            SELECT COALESCE(SUM(p.total), 0) - COALESCE(SUM(pay.paid_amount), 0) AS outstanding
            FROM purchase_invoices p
            LEFT JOIN (
                SELECT reference_id, SUM(amount) AS paid_amount
                FROM payment_transactions
                WHERE reference_type = 'purchase'
                  AND transaction_type = 'purchase_payment'
                GROUP BY reference_id
            ) pay ON pay.reference_id = p.id
            """
        ).fetchone()

        receivables = Decimal(str(receivables_raw["outstanding"] or 0))
        payables = Decimal(str(payables_raw["outstanding"] or 0))

        prev_receivables_raw = db.execute(
            """
            SELECT COALESCE(SUM(i.total), 0) - COALESCE(SUM(r.received_amount), 0) AS outstanding
            FROM invoices i
            LEFT JOIN (
                SELECT reference_id, SUM(amount) AS received_amount
                FROM payment_transactions
                WHERE reference_type = 'invoice'
                  AND transaction_type = 'invoice_receipt'
                  AND transaction_date <= ?
                GROUP BY reference_id
            ) r ON r.reference_id = i.id
            WHERE i.invoice_date <= ?
            """,
            (previous_end_iso, previous_end_iso),
        ).fetchone()
        prev_payables_raw = db.execute(
            """
            SELECT COALESCE(SUM(p.total), 0) - COALESCE(SUM(pay.paid_amount), 0) AS outstanding
            FROM purchase_invoices p
            LEFT JOIN (
                SELECT reference_id, SUM(amount) AS paid_amount
                FROM payment_transactions
                WHERE reference_type = 'purchase'
                  AND transaction_type = 'purchase_payment'
                  AND transaction_date <= ?
                GROUP BY reference_id
            ) pay ON pay.reference_id = p.id
            WHERE p.purchase_date <= ?
            """,
            (previous_end_iso, previous_end_iso),
        ).fetchone()

        prev_receivables = Decimal(str(prev_receivables_raw["outstanding"] or 0))
        prev_payables = Decimal(str(prev_payables_raw["outstanding"] or 0))

        payments_in_raw = db.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM payment_transactions
            WHERE transaction_type = 'invoice_receipt'
              AND transaction_date >= ? AND transaction_date <= ?
            """,
            (start_iso, end_iso),
        ).fetchone()
        payments_out_raw = db.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM payment_transactions
            WHERE transaction_type IN ('purchase_payment', 'expense_payment')
              AND transaction_date >= ? AND transaction_date <= ?
            """,
            (start_iso, end_iso),
        ).fetchone()

        payments_in = Decimal(str(payments_in_raw["total"] or 0))
        payments_out = Decimal(str(payments_out_raw["total"] or 0))

        received_vat_raw = db.execute(
            """
            SELECT COALESCE(SUM(
                CASE WHEN i.total > 0 THEN (i.vat_total / i.total) * t.amount ELSE 0 END
            ), 0) AS total
            FROM payment_transactions t
            JOIN invoices i ON i.id = t.reference_id
            WHERE t.reference_type = 'invoice'
              AND t.transaction_type = 'invoice_receipt'
              AND t.transaction_date >= ? AND t.transaction_date <= ?
            """,
            (start_iso, end_iso),
        ).fetchone()
        paid_vat_raw = db.execute(
            """
            SELECT COALESCE(SUM(
                CASE WHEN p.total > 0 THEN (p.vat_total / p.total) * t.amount ELSE 0 END
            ), 0) AS total
            FROM payment_transactions t
            JOIN purchase_invoices p ON p.id = t.reference_id
            WHERE t.reference_type = 'purchase'
              AND t.transaction_type = 'purchase_payment'
              AND t.transaction_date >= ? AND t.transaction_date <= ?
            """,
            (start_iso, end_iso),
        ).fetchone()

        received_vat = Decimal(str(received_vat_raw["total"] or 0))
        paid_vat = Decimal(str(paid_vat_raw["total"] or 0))
        vat_balance = received_vat - paid_vat

        month_cursor = date(today.year, today.month, 1)
        month_keys = []
        for _ in range(12):
            month_keys.append(month_cursor.strftime("%Y-%m"))
            if month_cursor.month == 1:
                month_cursor = date(month_cursor.year - 1, 12, 1)
            else:
                month_cursor = date(month_cursor.year, month_cursor.month - 1, 1)
        month_keys.reverse()

        trend_start = f"{month_keys[0]}-01"
        invoices_trend_rows = db.execute(
            """
            SELECT strftime('%Y-%m', invoice_date) AS period, COALESCE(SUM(total), 0) AS total
            FROM invoices
            WHERE invoice_date >= ?
            GROUP BY strftime('%Y-%m', invoice_date)
            """,
            (trend_start,),
        ).fetchall()
        purchases_trend_rows = db.execute(
            """
            SELECT strftime('%Y-%m', purchase_date) AS period, COALESCE(SUM(total), 0) AS total
            FROM purchase_invoices
            WHERE purchase_date >= ?
            GROUP BY strftime('%Y-%m', purchase_date)
            """,
            (trend_start,),
        ).fetchall()
        expenses_trend_rows = db.execute(
            """
            SELECT strftime('%Y-%m', expense_date) AS period, COALESCE(SUM(amount), 0) AS total
            FROM expenses
            WHERE expense_date >= ?
            GROUP BY strftime('%Y-%m', expense_date)
            """,
            (trend_start,),
        ).fetchall()

        invoices_map = {row["period"]: float(row["total"] or 0) for row in invoices_trend_rows if row["period"]}
        purchases_map = {row["period"]: float(row["total"] or 0) for row in purchases_trend_rows if row["period"]}
        expenses_map = {row["period"]: float(row["total"] or 0) for row in expenses_trend_rows if row["period"]}

        chart_labels = [datetime.strptime(key, "%Y-%m").strftime("%b %Y") for key in month_keys]
        chart_sales = [invoices_map.get(key, 0.0) for key in month_keys]
        chart_purchases = [purchases_map.get(key, 0.0) for key in month_keys]
        chart_expenses = [expenses_map.get(key, 0.0) for key in month_keys]
        chart_net = [
            chart_sales[index] - chart_purchases[index] - chart_expenses[index]
            for index in range(len(month_keys))
        ]

        recent_invoices = db.execute(
            """
            SELECT
                i.id,
                i.invoice_number,
                i.invoice_date,
                i.customer_name,
                i.total,
                COALESCE(r.received_amount, 0) AS paid_amount,
                MAX(i.total - COALESCE(r.received_amount, 0), 0) AS outstanding_amount
            FROM invoices i
            LEFT JOIN (
                SELECT reference_id, SUM(amount) AS received_amount
                FROM payment_transactions
                WHERE reference_type = 'invoice'
                  AND transaction_type = 'invoice_receipt'
                GROUP BY reference_id
            ) r ON r.reference_id = i.id
            ORDER BY i.id DESC
            LIMIT 5
            """
        ).fetchall()

        recent_purchases = db.execute(
            """
            SELECT
                p.id,
                p.purchase_number,
                p.purchase_date,
                p.vendor_name,
                p.total,
                COALESCE(pay.paid_amount, 0) AS paid_amount,
                MAX(p.total - COALESCE(pay.paid_amount, 0), 0) AS outstanding_amount
            FROM purchase_invoices p
            LEFT JOIN (
                SELECT reference_id, SUM(amount) AS paid_amount
                FROM payment_transactions
                WHERE reference_type = 'purchase'
                  AND transaction_type = 'purchase_payment'
                GROUP BY reference_id
            ) pay ON pay.reference_id = p.id
            ORDER BY p.id DESC
            LIMIT 5
            """
        ).fetchall()

        purchased_rows = db.execute(
            """
            SELECT item_id, item_name, unit, COALESCE(SUM(quantity), 0) AS purchased_qty
            FROM purchase_invoice_items
            GROUP BY item_id, item_name, unit
            """
        ).fetchall()
        sold_rows = db.execute(
            """
            SELECT item_id, item_name, unit, COALESCE(SUM(quantity), 0) AS sold_qty
            FROM invoice_items
            GROUP BY item_id, item_name, unit
            """
        ).fetchall()

        stock_map = {}
        for row in purchased_rows:
            key = _asset_key(row["item_id"], row["item_name"])
            stock_map[key] = {
                "item_name": row["item_name"] or "-",
                "unit": row["unit"] or "pcs",
                "purchased": Decimal(str(row["purchased_qty"] or 0)),
                "sold": Decimal("0"),
            }

        for row in sold_rows:
            key = _asset_key(row["item_id"], row["item_name"])
            if key not in stock_map:
                stock_map[key] = {
                    "item_name": row["item_name"] or "-",
                    "unit": row["unit"] or "pcs",
                    "purchased": Decimal("0"),
                    "sold": Decimal("0"),
                }
            stock_map[key]["sold"] = Decimal(str(row["sold_qty"] or 0))

        stock_alerts = []
        for entry in stock_map.values():
            available = entry["purchased"] - entry["sold"]
            if available <= Decimal("5"):
                stock_alerts.append(
                    {
                        "item_name": entry["item_name"],
                        "unit": entry["unit"],
                        "available": float(available),
                    }
                )
        stock_alerts.sort(key=lambda value: value["available"])
        stock_alerts = stock_alerts[:5]

        return render_template(
            "index.html",
            page_title="Dashboard",
            active_menu="Dashboard",
            period=period,
            period_label=period_label,
            sales_total=float(sales_total),
            purchase_total=float(purchase_total),
            expense_total=float(expense_total),
            net_profit=float(net_profit),
            receivables=float(receivables),
            payables=float(payables),
            sales_trend=_trend(sales_total, prev_sales_total, positive_is_good=True),
            purchases_trend=_trend(purchase_total, prev_purchase_total, positive_is_good=False),
            expenses_trend=_trend(expense_total, prev_expense_total, positive_is_good=False),
            net_profit_trend=_trend(net_profit, prev_net_profit, positive_is_good=True),
            receivables_trend=_trend(receivables, prev_receivables, positive_is_good=False),
            payables_trend=_trend(payables, prev_payables, positive_is_good=False),
            received_vat=float(received_vat),
            paid_vat=float(paid_vat),
            vat_balance=float(vat_balance),
            payments_in=float(payments_in),
            payments_out=float(payments_out),
            chart_labels=json.dumps(chart_labels),
            chart_sales=json.dumps(chart_sales),
            chart_purchases=json.dumps(chart_purchases),
            chart_expenses=json.dumps(chart_expenses),
            chart_net=json.dumps(chart_net),
            recent_invoices=recent_invoices,
            recent_purchases=recent_purchases,
            stock_alerts=stock_alerts,
        )

    @app.route("/items")
    def items_page():
        db = get_db()
        ensure_items_table()
        search_query = request.args.get("q", "").strip()

        if search_query:
            like_query = f"%{search_query}%"
            items = db.execute(
                """
                SELECT id, item_number, name, price, vat_amount, unit, description
                FROM items
                WHERE item_number LIKE ?
                   OR name LIKE ?
                   OR unit LIKE ?
                   OR description LIKE ?
                ORDER BY id DESC
                """,
                (like_query, like_query, like_query, like_query),
            ).fetchall()
        else:
            items = db.execute(
                """
                SELECT id, item_number, name, price, vat_amount, unit, description
                FROM items
                ORDER BY id DESC
                """
            ).fetchall()

        return render_template(
            "items.html",
            page_title="Items",
            active_menu="Items",
            items=items,
            search_query=search_query,
        )

    @app.post("/items/add")
    def add_item():
        db = get_db()
        ensure_items_table()

        name = request.form.get("name", "").strip()
        unit = request.form.get("unit", "").strip()
        description = request.form.get("description", "").strip()
        raw_price = request.form.get("price", "").strip()
        raw_vat_amount = request.form.get("vat_amount", "").strip()

        if not name:
            return redirect(url_for("items_page"))

        price = None
        vat_amount = None

        if raw_price:
            try:
                price = Decimal(raw_price)
            except InvalidOperation:
                return redirect(url_for("items_page"))

        if raw_vat_amount:
            try:
                vat_amount = Decimal(raw_vat_amount)
            except InvalidOperation:
                return redirect(url_for("items_page"))

        normalized_unit = unit or None
        normalized_description = description or None

        item_number = _generate_item_number(db)
        db.execute(
            """
            INSERT INTO items (item_number, name, price, vat_amount, unit, description)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                item_number,
                name,
                float(price) if price is not None else None,
                float(vat_amount) if vat_amount is not None else None,
                normalized_unit,
                normalized_description,
            ),
        )
        db.commit()
        return redirect(url_for("items_page"))

    @app.post("/items/edit")
    def edit_item():
        db = get_db()
        ensure_items_table()

        item_id = request.form.get("item_id", "").strip()
        name = request.form.get("name", "").strip()
        unit = request.form.get("unit", "").strip()
        description = request.form.get("description", "").strip()
        raw_price = request.form.get("price", "").strip()
        raw_vat_amount = request.form.get("vat_amount", "").strip()

        if not item_id or not name:
            return redirect(url_for("items_page"))

        try:
            parsed_item_id = int(item_id)
        except ValueError:
            return redirect(url_for("items_page"))

        price = None
        vat_amount = None

        if raw_price:
            try:
                price = Decimal(raw_price)
            except InvalidOperation:
                return redirect(url_for("items_page"))

        if raw_vat_amount:
            try:
                vat_amount = Decimal(raw_vat_amount)
            except InvalidOperation:
                return redirect(url_for("items_page"))

        normalized_unit = unit or None
        normalized_description = description or None

        db.execute(
            """
            UPDATE items
            SET name = ?, price = ?, vat_amount = ?, unit = ?, description = ?
            WHERE id = ?
            """,
            (
                name,
                float(price) if price is not None else None,
                float(vat_amount) if vat_amount is not None else None,
                normalized_unit,
                normalized_description,
                parsed_item_id,
            ),
        )
        db.commit()
        return redirect(url_for("items_page"))

    @app.post("/items/delete")
    def delete_item():
        db = get_db()
        ensure_items_table()

        item_id = request.form.get("item_id", "").strip()
        try:
            parsed_item_id = int(item_id)
        except ValueError:
            return redirect(url_for("items_page"))

        db.execute("DELETE FROM items WHERE id = ?", (parsed_item_id,))
        db.commit()
        return redirect(url_for("items_page"))

    @app.route("/customers")
    def customers_page():
        db = get_db()
        ensure_customers_table()
        search_query = request.args.get("q", "").strip()

        if search_query:
            like_query = f"%{search_query}%"
            customers = db.execute(
                """
                SELECT
                    id,
                    customer_number,
                    customer_type,
                    customer_name,
                    customer_tax_number,
                    registration_name,
                    phone_number,
                    address,
                    website,
                    country,
                    address_2
                FROM customers
                WHERE customer_number LIKE ?
                         OR customer_type LIKE ?
                   OR customer_name LIKE ?
                   OR customer_tax_number LIKE ?
                   OR registration_name LIKE ?
                   OR phone_number LIKE ?
                   OR address LIKE ?
                   OR website LIKE ?
                   OR country LIKE ?
                   OR address_2 LIKE ?
                ORDER BY id DESC
                """,
                (
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                ),
            ).fetchall()
        else:
            customers = db.execute(
                """
                SELECT
                    id,
                    customer_number,
                    customer_type,
                    customer_name,
                    customer_tax_number,
                    registration_name,
                    phone_number,
                    address,
                    website,
                    country,
                    address_2
                FROM customers
                ORDER BY id DESC
                """
            ).fetchall()

        return render_template(
            "customers.html",
            page_title="Customers",
            active_menu="customers",
            customers=customers,
            search_query=search_query,
        )

    @app.post("/customers/add")
    def add_customer():
        db = get_db()
        ensure_customers_table()

        customer_type = request.form.get("customer_type", "Company").strip() or "Company"
        if customer_type not in {"Company", "individual"}:
            customer_type = "Company"

        customer_name = request.form.get("customer_name", "").strip()
        customer_tax_number = request.form.get("customer_tax_number", "").strip() or None
        registration_name = request.form.get("registration_name", "").strip() or None
        phone_number = request.form.get("phone_number", "").strip() or None
        address = request.form.get("address", "").strip() or None
        website = request.form.get("website", "").strip() or None
        country = request.form.get("country", "").strip() or None
        address_2 = request.form.get("address_2", "").strip() or None

        if not customer_name:
            return redirect(url_for("customers_page"))

        customer_number = _generate_customer_number(db)
        db.execute(
            """
            INSERT INTO customers (
                customer_number,
                customer_type,
                customer_name,
                customer_tax_number,
                registration_name,
                phone_number,
                address,
                website,
                country,
                address_2
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                customer_number,
                customer_type,
                customer_name,
                customer_tax_number,
                registration_name,
                phone_number,
                address,
                website,
                country,
                address_2,
            ),
        )
        db.commit()
        return redirect(url_for("customers_page"))

    @app.post("/customers/edit")
    def edit_customer():
        db = get_db()
        ensure_customers_table()

        customer_id = request.form.get("customer_id", "").strip()
        customer_type = request.form.get("customer_type", "Company").strip() or "Company"
        if customer_type not in {"Company", "individual"}:
            customer_type = "Company"

        customer_name = request.form.get("customer_name", "").strip()
        customer_tax_number = request.form.get("customer_tax_number", "").strip() or None
        registration_name = request.form.get("registration_name", "").strip() or None
        phone_number = request.form.get("phone_number", "").strip() or None
        address = request.form.get("address", "").strip() or None
        website = request.form.get("website", "").strip() or None
        country = request.form.get("country", "").strip() or None
        address_2 = request.form.get("address_2", "").strip() or None

        if not customer_id or not customer_name:
            return redirect(url_for("customers_page"))

        try:
            parsed_customer_id = int(customer_id)
        except ValueError:
            return redirect(url_for("customers_page"))

        db.execute(
            """
            UPDATE customers
            SET customer_type = ?,
                customer_name = ?,
                customer_tax_number = ?,
                registration_name = ?,
                phone_number = ?,
                address = ?,
                website = ?,
                country = ?,
                address_2 = ?
            WHERE id = ?
            """,
            (
                customer_type,
                customer_name,
                customer_tax_number,
                registration_name,
                phone_number,
                address,
                website,
                country,
                address_2,
                parsed_customer_id,
            ),
        )
        db.commit()
        return redirect(url_for("customers_page"))

    @app.post("/customers/delete")
    def delete_customer():
        db = get_db()
        ensure_customers_table()

        customer_id = request.form.get("customer_id", "").strip()
        try:
            parsed_customer_id = int(customer_id)
        except ValueError:
            return redirect(url_for("customers_page"))

        db.execute("DELETE FROM customers WHERE id = ?", (parsed_customer_id,))
        db.commit()
        return redirect(url_for("customers_page"))

    @app.route("/vendors")
    def vendors_page():
        db = get_db()
        ensure_vendors_table()
        search_query = request.args.get("q", "").strip()

        if search_query:
            like_query = f"%{search_query}%"
            vendors = db.execute(
                """
                SELECT
                    id,
                    vendor_number,
                    vendor_type,
                    vendor_name,
                    vendor_tax_number,
                    registration_name,
                    phone_number,
                    address,
                    website,
                    country,
                    address_2
                FROM vendors
                WHERE vendor_number LIKE ?
                   OR vendor_type LIKE ?
                   OR vendor_name LIKE ?
                   OR vendor_tax_number LIKE ?
                   OR registration_name LIKE ?
                   OR phone_number LIKE ?
                   OR address LIKE ?
                   OR website LIKE ?
                   OR country LIKE ?
                   OR address_2 LIKE ?
                ORDER BY id DESC
                """,
                (
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                    like_query,
                ),
            ).fetchall()
        else:
            vendors = db.execute(
                """
                SELECT
                    id,
                    vendor_number,
                    vendor_type,
                    vendor_name,
                    vendor_tax_number,
                    registration_name,
                    phone_number,
                    address,
                    website,
                    country,
                    address_2
                FROM vendors
                ORDER BY id DESC
                """
            ).fetchall()

        return render_template(
            "vendors.html",
            page_title="Vendors",
            active_menu="Vendors",
            vendors=vendors,
            search_query=search_query,
        )

    @app.post("/vendors/add")
    def add_vendor():
        db = get_db()
        ensure_vendors_table()

        vendor_type = request.form.get("vendor_type", "Company").strip() or "Company"
        if vendor_type not in {"Company", "individual"}:
            vendor_type = "Company"

        vendor_name = request.form.get("vendor_name", "").strip()
        vendor_tax_number = request.form.get("vendor_tax_number", "").strip() or None
        registration_name = request.form.get("registration_name", "").strip() or None
        phone_number = request.form.get("phone_number", "").strip() or None
        address = request.form.get("address", "").strip() or None
        website = request.form.get("website", "").strip() or None
        country = request.form.get("country", "").strip() or None
        address_2 = request.form.get("address_2", "").strip() or None

        if not vendor_name:
            return redirect(url_for("vendors_page"))

        vendor_number = _generate_vendor_number(db)
        db.execute(
            """
            INSERT INTO vendors (
                vendor_number,
                vendor_type,
                vendor_name,
                vendor_tax_number,
                registration_name,
                phone_number,
                address,
                website,
                country,
                address_2
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                vendor_number,
                vendor_type,
                vendor_name,
                vendor_tax_number,
                registration_name,
                phone_number,
                address,
                website,
                country,
                address_2,
            ),
        )
        db.commit()
        return redirect(url_for("vendors_page"))

    @app.post("/vendors/edit")
    def edit_vendor():
        db = get_db()
        ensure_vendors_table()

        vendor_id = request.form.get("vendor_id", "").strip()
        vendor_type = request.form.get("vendor_type", "Company").strip() or "Company"
        if vendor_type not in {"Company", "individual"}:
            vendor_type = "Company"

        vendor_name = request.form.get("vendor_name", "").strip()
        vendor_tax_number = request.form.get("vendor_tax_number", "").strip() or None
        registration_name = request.form.get("registration_name", "").strip() or None
        phone_number = request.form.get("phone_number", "").strip() or None
        address = request.form.get("address", "").strip() or None
        website = request.form.get("website", "").strip() or None
        country = request.form.get("country", "").strip() or None
        address_2 = request.form.get("address_2", "").strip() or None

        if not vendor_id or not vendor_name:
            return redirect(url_for("vendors_page"))

        try:
            parsed_vendor_id = int(vendor_id)
        except ValueError:
            return redirect(url_for("vendors_page"))

        db.execute(
            """
            UPDATE vendors
            SET vendor_type = ?,
                vendor_name = ?,
                vendor_tax_number = ?,
                registration_name = ?,
                phone_number = ?,
                address = ?,
                website = ?,
                country = ?,
                address_2 = ?
            WHERE id = ?
            """,
            (
                vendor_type,
                vendor_name,
                vendor_tax_number,
                registration_name,
                phone_number,
                address,
                website,
                country,
                address_2,
                parsed_vendor_id,
            ),
        )
        db.commit()
        return redirect(url_for("vendors_page"))

    @app.post("/vendors/delete")
    def delete_vendor():
        db = get_db()
        ensure_vendors_table()

        vendor_id = request.form.get("vendor_id", "").strip()
        try:
            parsed_vendor_id = int(vendor_id)
        except ValueError:
            return redirect(url_for("vendors_page"))

        db.execute("DELETE FROM vendors WHERE id = ?", (parsed_vendor_id,))
        db.commit()
        return redirect(url_for("vendors_page"))

    @app.route("/invoices")
    def invoices_page():
        db = get_db()
        ensure_invoices_tables()
        ensure_payment_tables()
        search_query = request.args.get("q", "").strip()

        payment_methods = db.execute(
            """
            SELECT id, name, method_type
            FROM payment_methods
            ORDER BY name ASC
            """
        ).fetchall()

        if search_query:
            like_query = f"%{search_query}%"
            invoices = db.execute(
                """
                SELECT
                    i.id,
                    i.invoice_number,
                    i.invoice_date,
                    i.customer_name,
                    i.total,
                    COALESCE(r.received_amount, 0) AS paid_amount,
                    MAX(i.total - COALESCE(r.received_amount, 0), 0) AS outstanding_amount
                FROM invoices i
                LEFT JOIN (
                    SELECT reference_id, SUM(amount) AS received_amount
                    FROM payment_transactions
                    WHERE reference_type = 'invoice'
                      AND transaction_type = 'invoice_receipt'
                    GROUP BY reference_id
                ) r ON r.reference_id = i.id
                WHERE i.invoice_number LIKE ?
                   OR i.invoice_date LIKE ?
                   OR i.customer_name LIKE ?
                ORDER BY i.id DESC
                """,
                (like_query, like_query, like_query),
            ).fetchall()
        else:
            invoices = db.execute(
                """
                SELECT
                    i.id,
                    i.invoice_number,
                    i.invoice_date,
                    i.customer_name,
                    i.total,
                    COALESCE(r.received_amount, 0) AS paid_amount,
                    MAX(i.total - COALESCE(r.received_amount, 0), 0) AS outstanding_amount
                FROM invoices i
                LEFT JOIN (
                    SELECT reference_id, SUM(amount) AS received_amount
                    FROM payment_transactions
                    WHERE reference_type = 'invoice'
                      AND transaction_type = 'invoice_receipt'
                    GROUP BY reference_id
                ) r ON r.reference_id = i.id
                ORDER BY i.id DESC
                """
            ).fetchall()

        currencies = db.execute(
            """
            SELECT code, name
            FROM payment_currencies
            ORDER BY code ASC
            """
        ).fetchall()
        return render_template(
            "invoices.html",
            page_title="Invoices",
            active_menu="invoices",
            invoices=invoices,
            payment_methods=payment_methods,
            currencies=currencies,
            search_query=search_query,
        )

    @app.post("/invoices/pay")
    def pay_invoice():
        db = get_db()
        ensure_invoices_tables()
        ensure_payment_tables()

        invoice_id = request.form.get("invoice_id", "").strip()
        payment_method_id = request.form.get("payment_method_id", "").strip()
        payment_date = request.form.get("payment_date", "").strip() or date.today().isoformat()
        amount = _to_decimal_or_default(request.form.get("amount", "0"), "0")
        notes = request.form.get("notes", "").strip() or None

        try:
            parsed_invoice_id = int(invoice_id)
            parsed_payment_method_id = int(payment_method_id)
        except ValueError:
            return redirect(url_for("invoices_page"))

        invoice = db.execute("SELECT id, invoice_number, total FROM invoices WHERE id = ?", (parsed_invoice_id,)).fetchone()
        if invoice is None:
            return redirect(url_for("invoices_page"))

        method = db.execute("SELECT id FROM payment_methods WHERE id = ?", (parsed_payment_method_id,)).fetchone()
        if method is None:
            return redirect(url_for("invoices_page"))

        received_raw = db.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM payment_transactions
            WHERE reference_type = 'invoice'
              AND transaction_type = 'invoice_receipt'
              AND reference_id = ?
            """,
            (parsed_invoice_id,),
        ).fetchone()

        outstanding = Decimal(str(invoice["total"] or 0)) - Decimal(str(received_raw["total"] or 0))
        if outstanding <= 0:
            return redirect(url_for("invoices_page"))

        if amount <= 0:
            amount = outstanding
        if amount > outstanding:
            amount = outstanding

        currency_code = _get_default_currency_code(db)
        transaction_notes = notes or f"Invoice payment received for {invoice['invoice_number']}"
        db.execute(
            """
            INSERT INTO payment_transactions (
                transaction_date,
                transaction_type,
                reference_type,
                reference_id,
                amount,
                currency_code,
                method_id,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payment_date,
                "invoice_receipt",
                "invoice",
                parsed_invoice_id,
                float(amount),
                currency_code,
                parsed_payment_method_id,
                transaction_notes,
            ),
        )
        db.commit()
        return redirect(url_for("invoices_page"))

    @app.route("/invoices/new")
    def new_invoice_page():
        db = get_db()
        ensure_invoices_tables()
        customers = db.execute(
            """
            SELECT id, customer_name, customer_tax_number, registration_name,
                   phone_number, address, website, country, address_2
            FROM customers
            ORDER BY id DESC
            """
        ).fetchall()
        items = db.execute(
            """
            SELECT id, name, unit, price, vat_amount
            FROM items
            ORDER BY id DESC
            """
        ).fetchall()

        # Get default currency code
        currency_code = _get_default_currency_code(db)
        invoice = type('InvoiceObj', (), {'currency_code': currency_code})()
        currencies = db.execute(
            """
            SELECT code, name
            FROM payment_currencies
            ORDER BY code ASC
            """
        ).fetchall()
        return render_template(
            "invoice_form.html",
            page_title="Add Invoice",
            active_menu="invoices",
            form_action=url_for("add_invoice"),
            invoice=invoice,
            invoice_items=[],
            customers=customers,
            items_catalog=items,
            currencies=currencies,
            generated_invoice_number=_generate_invoice_number(db),
        )

    @app.post("/invoices/add")
    def add_invoice():
        db = get_db()
        ensure_invoices_tables()

        invoice_number = _ensure_unique_invoice_number(db, request.form.get("invoice_number"))
        invoice_date = request.form.get("invoice_date", "").strip()
        customer_id = request.form.get("customer_id", "").strip()

        customer_name = request.form.get("customer_name", "").strip() or None
        customer_tax_number = request.form.get("customer_tax_number", "").strip() or None
        registration_name = request.form.get("registration_name", "").strip() or None
        phone_number = request.form.get("phone_number", "").strip() or None
        address = request.form.get("address", "").strip() or None
        website = request.form.get("website", "").strip() or None
        country = request.form.get("country", "").strip() or None
        address_2 = request.form.get("address_2", "").strip() or None

        if not invoice_date:
            return redirect(url_for("new_invoice_page"))

        parsed_customer_id = None
        if customer_id:
            try:
                parsed_customer_id = int(customer_id)
            except ValueError:
                parsed_customer_id = None

        currency_code = request.form.get("currency_code", "").strip().upper() or _get_default_currency_code(db)
        db.execute(
            """
            INSERT INTO invoices (
                invoice_number, invoice_date, customer_id,
                customer_name, customer_tax_number, registration_name,
                phone_number, address, website, country, address_2,
                subtotal, vat_total, total, currency_code
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?)
            """,
            (
                invoice_number,
                invoice_date,
                parsed_customer_id,
                customer_name,
                customer_tax_number,
                registration_name,
                phone_number,
                address,
                website,
                country,
                address_2,
                currency_code,
            ),
        )
        invoice_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

        item_ids = request.form.getlist("item_id[]")
        item_names = request.form.getlist("item_name[]")
        quantities = request.form.getlist("quantity[]")
        units = request.form.getlist("unit[]")
        prices = request.form.getlist("price[]")
        vat_amounts = request.form.getlist("vat_amount[]")

        subtotal = Decimal("0")
        vat_total = Decimal("0")

        max_len = max(
            len(item_ids),
            len(item_names),
            len(quantities),
            len(units),
            len(prices),
            len(vat_amounts),
            0,
        )

        for index in range(max_len):
            raw_name = item_names[index].strip() if index < len(item_names) else ""
            raw_quantity = quantities[index] if index < len(quantities) else ""
            raw_unit = units[index].strip() if index < len(units) else ""
            raw_price = prices[index] if index < len(prices) else ""
            raw_vat = vat_amounts[index] if index < len(vat_amounts) else ""
            raw_item_id = item_ids[index].strip() if index < len(item_ids) else ""

            if not raw_name:
                continue

            quantity = _to_decimal_or_default(raw_quantity, "1")
            price = _to_decimal_or_default(raw_price, "0")
            vat_percentage = _to_decimal_or_default(raw_vat, "0")
            line_net = quantity * price
            vat_value = (line_net * vat_percentage) / Decimal("100")
            line_total = line_net + vat_value

            parsed_item_id = None
            if raw_item_id:
                try:
                    parsed_item_id = int(raw_item_id)
                except ValueError:
                    parsed_item_id = None

            db.execute(
                """
                INSERT INTO invoice_items (
                    invoice_id, item_id, item_name, quantity, unit, price, vat_amount, line_total
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    invoice_id,
                    parsed_item_id,
                    raw_name,
                    float(quantity),
                    raw_unit or "1",
                    float(price),
                    float(vat_percentage),
                    float(line_total),
                ),
            )

            subtotal += line_net
            vat_total += vat_value

        total = subtotal + vat_total
        db.execute(
            "UPDATE invoices SET subtotal = ?, vat_total = ?, total = ? WHERE id = ?",
            (float(subtotal), float(vat_total), float(total), invoice_id),
        )
        db.commit()
        return redirect(url_for("view_invoice_page", invoice_id=invoice_id))

    @app.route("/invoices/<int:invoice_id>/edit")
    def edit_invoice_page(invoice_id):
        db = get_db()
        ensure_invoices_tables()

        invoice = db.execute(
            "SELECT * FROM invoices WHERE id = ?",
            (invoice_id,),
        ).fetchone()
        if invoice is None:
            return redirect(url_for("invoices_page"))

        invoice_items = db.execute(
            """
            SELECT id, item_id, item_name, quantity, unit, price, vat_amount, line_total
            FROM invoice_items
            WHERE invoice_id = ?
            ORDER BY id ASC
            """,
            (invoice_id,),
        ).fetchall()

        customers = db.execute(
            """
            SELECT id, customer_name, customer_tax_number, registration_name,
                   phone_number, address, website, country, address_2
            FROM customers
            ORDER BY id DESC
            """
        ).fetchall()
        items = db.execute(
            """
            SELECT id, name, unit, price, vat_amount
            FROM items
            ORDER BY id DESC
            """
        ).fetchall()

        currencies = db.execute(
            """
            SELECT code, name
            FROM payment_currencies
            ORDER BY code ASC
            """
        ).fetchall()
        return render_template(
            "invoice_form.html",
            page_title="Edit Invoice",
            active_menu="invoices",
            form_action=url_for("update_invoice", invoice_id=invoice_id),
            invoice=invoice,
            invoice_items=invoice_items,
            customers=customers,
            items_catalog=items,
            currencies=currencies,
            generated_invoice_number=invoice["invoice_number"],
        )

    @app.post("/invoices/<int:invoice_id>/edit")
    def update_invoice(invoice_id):
        db = get_db()
        ensure_invoices_tables()

        invoice = db.execute("SELECT id FROM invoices WHERE id = ?", (invoice_id,)).fetchone()
        if invoice is None:
            return redirect(url_for("invoices_page"))

        invoice_number = _ensure_unique_invoice_number(
            db,
            request.form.get("invoice_number"),
            exclude_invoice_id=invoice_id,
        )
        invoice_date = request.form.get("invoice_date", "").strip()
        if not invoice_date:
            return redirect(url_for("edit_invoice_page", invoice_id=invoice_id))

        customer_id = request.form.get("customer_id", "").strip()
        parsed_customer_id = None
        if customer_id:
            try:
                parsed_customer_id = int(customer_id)
            except ValueError:
                parsed_customer_id = None

        customer_name = request.form.get("customer_name", "").strip() or None
        customer_tax_number = request.form.get("customer_tax_number", "").strip() or None
        registration_name = request.form.get("registration_name", "").strip() or None
        phone_number = request.form.get("phone_number", "").strip() or None
        address = request.form.get("address", "").strip() or None
        website = request.form.get("website", "").strip() or None
        country = request.form.get("country", "").strip() or None
        address_2 = request.form.get("address_2", "").strip() or None

        currency_code = request.form.get("currency_code", "").strip().upper() or _get_default_currency_code(db)
        db.execute(
            """
            UPDATE invoices
            SET invoice_number = ?, invoice_date = ?, customer_id = ?,
                customer_name = ?, customer_tax_number = ?, registration_name = ?,
                phone_number = ?, address = ?, website = ?, country = ?, address_2 = ?,
                currency_code = ?
            WHERE id = ?
            """,
            (
                invoice_number,
                invoice_date,
                parsed_customer_id,
                customer_name,
                customer_tax_number,
                registration_name,
                phone_number,
                address,
                website,
                country,
                address_2,
                currency_code,
                invoice_id,
            ),
        )

        db.execute("DELETE FROM invoice_items WHERE invoice_id = ?", (invoice_id,))

        item_ids = request.form.getlist("item_id[]")
        item_names = request.form.getlist("item_name[]")
        quantities = request.form.getlist("quantity[]")
        units = request.form.getlist("unit[]")
        prices = request.form.getlist("price[]")
        vat_amounts = request.form.getlist("vat_amount[]")

        subtotal = Decimal("0")
        vat_total = Decimal("0")

        max_len = max(
            len(item_ids),
            len(item_names),
            len(quantities),
            len(units),
            len(prices),
            len(vat_amounts),
            0,
        )

        for index in range(max_len):
            raw_name = item_names[index].strip() if index < len(item_names) else ""
            raw_quantity = quantities[index] if index < len(quantities) else ""
            raw_unit = units[index].strip() if index < len(units) else ""
            raw_price = prices[index] if index < len(prices) else ""
            raw_vat = vat_amounts[index] if index < len(vat_amounts) else ""
            raw_item_id = item_ids[index].strip() if index < len(item_ids) else ""

            if not raw_name:
                continue

            quantity = _to_decimal_or_default(raw_quantity, "1")
            price = _to_decimal_or_default(raw_price, "0")
            vat_percentage = _to_decimal_or_default(raw_vat, "0")
            line_net = quantity * price
            vat_value = (line_net * vat_percentage) / Decimal("100")
            line_total = line_net + vat_value

            parsed_item_id = None
            if raw_item_id:
                try:
                    parsed_item_id = int(raw_item_id)
                except ValueError:
                    parsed_item_id = None

            db.execute(
                """
                INSERT INTO invoice_items (
                    invoice_id, item_id, item_name, quantity, unit, price, vat_amount, line_total
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    invoice_id,
                    parsed_item_id,
                    raw_name,
                    float(quantity),
                    raw_unit or "1",
                    float(price),
                    float(vat_percentage),
                    float(line_total),
                ),
            )

            subtotal += line_net
            vat_total += vat_value

        total = subtotal + vat_total
        db.execute(
            "UPDATE invoices SET subtotal = ?, vat_total = ?, total = ? WHERE id = ?",
            (float(subtotal), float(vat_total), float(total), invoice_id),
        )
        db.commit()
        return redirect(url_for("view_invoice_page", invoice_id=invoice_id))

    @app.post("/invoices/delete")
    def delete_invoice():
        db = get_db()
        ensure_invoices_tables()

        invoice_id = request.form.get("invoice_id", "").strip()
        try:
            parsed_invoice_id = int(invoice_id)
        except ValueError:
            return redirect(url_for("invoices_page"))

        db.execute("DELETE FROM invoice_items WHERE invoice_id = ?", (parsed_invoice_id,))
        db.execute("DELETE FROM invoices WHERE id = ?", (parsed_invoice_id,))
        db.commit()
        return redirect(url_for("invoices_page"))

    @app.route("/invoices/<int:invoice_id>")
    def view_invoice_page(invoice_id):
        db = get_db()
        ensure_invoices_tables()

        invoice = db.execute("SELECT * FROM invoices WHERE id = ?", (invoice_id,)).fetchone()
        if invoice is None:
            return redirect(url_for("invoices_page"))

        invoice_items = db.execute(
            """
            SELECT item_name, quantity, unit, price, vat_amount, line_total
            FROM invoice_items
            WHERE invoice_id = ?
            ORDER BY id ASC
            """,
            (invoice_id,),
        ).fetchall()

        return render_template(
            "invoice_view.html",
            page_title=f"Invoice {invoice['invoice_number']}",
            active_menu="invoices",
            invoice=invoice,
            invoice_items=invoice_items,
        )

    @app.route("/purchases")
    def purchases_page():
        db = get_db()
        ensure_purchase_invoices_tables()
        ensure_payment_tables()
        search_query = request.args.get("q", "").strip()

        payment_methods = db.execute(
            """
            SELECT id, name, method_type
            FROM payment_methods
            ORDER BY name ASC
            """
        ).fetchall()

        if search_query:
            like_query = f"%{search_query}%"
            purchases = db.execute(
                """
                SELECT
                    p.id,
                    p.purchase_number,
                    p.purchase_date,
                    p.vendor_name,
                    p.total,
                    COALESCE(pay.paid_amount, 0) AS paid_amount,
                    MAX(p.total - COALESCE(pay.paid_amount, 0), 0) AS outstanding_amount
                FROM purchase_invoices p
                LEFT JOIN (
                    SELECT reference_id, SUM(amount) AS paid_amount
                    FROM payment_transactions
                    WHERE reference_type = 'purchase'
                      AND transaction_type = 'purchase_payment'
                    GROUP BY reference_id
                ) pay ON pay.reference_id = p.id
                WHERE p.purchase_number LIKE ?
                   OR p.purchase_date LIKE ?
                   OR p.vendor_name LIKE ?
                ORDER BY p.id DESC
                """,
                (like_query, like_query, like_query),
            ).fetchall()
        else:
            purchases = db.execute(
                """
                SELECT
                    p.id,
                    p.purchase_number,
                    p.purchase_date,
                    p.vendor_name,
                    p.total,
                    COALESCE(pay.paid_amount, 0) AS paid_amount,
                    MAX(p.total - COALESCE(pay.paid_amount, 0), 0) AS outstanding_amount
                FROM purchase_invoices p
                LEFT JOIN (
                    SELECT reference_id, SUM(amount) AS paid_amount
                    FROM payment_transactions
                    WHERE reference_type = 'purchase'
                      AND transaction_type = 'purchase_payment'
                    GROUP BY reference_id
                ) pay ON pay.reference_id = p.id
                ORDER BY p.id DESC
                """
            ).fetchall()

        currencies = db.execute(
            """
            SELECT code, name
            FROM payment_currencies
            ORDER BY code ASC
            """
        ).fetchall()
        return render_template(
            "purchases.html",
            page_title="Purcheases",
            active_menu="Purcheases",
            purchases=purchases,
            payment_methods=payment_methods,
            currencies=currencies,
            search_query=search_query,
        )

    @app.post("/purchases/pay")
    def pay_purchase():
        db = get_db()
        ensure_purchase_invoices_tables()
        ensure_payment_tables()

        purchase_id = request.form.get("purchase_id", "").strip()
        payment_method_id = request.form.get("payment_method_id", "").strip()
        payment_date = request.form.get("payment_date", "").strip() or date.today().isoformat()
        amount = _to_decimal_or_default(request.form.get("amount", "0"), "0")
        notes = request.form.get("notes", "").strip() or None

        try:
            parsed_purchase_id = int(purchase_id)
            parsed_payment_method_id = int(payment_method_id)
        except ValueError:
            return redirect(url_for("purchases_page"))

        purchase = db.execute(
            "SELECT id, purchase_number, total FROM purchase_invoices WHERE id = ?",
            (parsed_purchase_id,),
        ).fetchone()
        if purchase is None:
            return redirect(url_for("purchases_page"))

        method = db.execute("SELECT id FROM payment_methods WHERE id = ?", (parsed_payment_method_id,)).fetchone()
        if method is None:
            return redirect(url_for("purchases_page"))

        paid_raw = db.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM payment_transactions
            WHERE reference_type = 'purchase'
              AND transaction_type = 'purchase_payment'
              AND reference_id = ?
            """,
            (parsed_purchase_id,),
        ).fetchone()

        outstanding = Decimal(str(purchase["total"] or 0)) - Decimal(str(paid_raw["total"] or 0))
        if outstanding <= 0:
            return redirect(url_for("purchases_page"))

        if amount <= 0:
            amount = outstanding
        if amount > outstanding:
            amount = outstanding

        currency_code = request.form.get("currency_code") or _get_default_currency_code(db)
        transaction_notes = notes or f"Purchase payment for {purchase['purchase_number']}"
        db.execute(
            """
            INSERT INTO payment_transactions (
                transaction_date,
                transaction_type,
                reference_type,
                reference_id,
                amount,
                currency_code,
                method_id,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payment_date,
                "purchase_payment",
                "purchase",
                parsed_purchase_id,
                float(amount),
                currency_code,
                parsed_payment_method_id,
                transaction_notes,
            ),
        )
        db.commit()
        return redirect(url_for("purchases_page"))

    @app.route("/purchases/new")
    def new_purchase_page():
        db = get_db()
        ensure_purchase_invoices_tables()
        vendors = db.execute(
            """
            SELECT id, vendor_name, vendor_tax_number, registration_name,
                   phone_number, address, website, country, address_2
            FROM vendors
            ORDER BY id DESC
            """
        ).fetchall()
        items = db.execute(
            """
            SELECT id, name, unit, price, vat_amount
            FROM items
            ORDER BY id DESC
            """
        ).fetchall()

        currencies = db.execute(
            """
            SELECT code, name
            FROM payment_currencies
            ORDER BY code ASC
            """
        ).fetchall()
        return render_template(
            "purchase_form.html",
            page_title="Add Purchase Invoice",
            active_menu="Purcheases",
            form_action=url_for("add_purchase"),
            purchase=None,
            purchase_items=[],
            vendors=vendors,
            items_catalog=items,
            currencies=currencies,
            generated_purchase_number=_generate_purchase_number(db),
        )

    @app.post("/purchases/add")
    def add_purchase():
        db = get_db()
        ensure_purchase_invoices_tables()

        purchase_number = _ensure_unique_purchase_number(db, request.form.get("purchase_number"))
        purchase_date = request.form.get("purchase_date", "").strip()
        vendor_id = request.form.get("vendor_id", "").strip()

        vendor_name = request.form.get("vendor_name", "").strip() or None
        vendor_tax_number = request.form.get("vendor_tax_number", "").strip() or None
        registration_name = request.form.get("registration_name", "").strip() or None
        phone_number = request.form.get("phone_number", "").strip() or None
        address = request.form.get("address", "").strip() or None
        website = request.form.get("website", "").strip() or None
        country = request.form.get("country", "").strip() or None
        address_2 = request.form.get("address_2", "").strip() or None

        if not purchase_date:
            return redirect(url_for("new_purchase_page"))

        parsed_vendor_id = None
        if vendor_id:
            try:
                parsed_vendor_id = int(vendor_id)
            except ValueError:
                parsed_vendor_id = None

        db.execute(
            """
            INSERT INTO purchase_invoices (
                purchase_number, purchase_date, vendor_id,
                vendor_name, vendor_tax_number, registration_name,
                phone_number, address, website, country, address_2,
                subtotal, vat_total, total
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0)
            """,
            (
                purchase_number,
                purchase_date,
                parsed_vendor_id,
                vendor_name,
                vendor_tax_number,
                registration_name,
                phone_number,
                address,
                website,
                country,
                address_2,
            ),
        )
        purchase_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

        item_ids = request.form.getlist("item_id[]")
        item_names = request.form.getlist("item_name[]")
        quantities = request.form.getlist("quantity[]")
        units = request.form.getlist("unit[]")
        prices = request.form.getlist("price[]")
        vat_amounts = request.form.getlist("vat_amount[]")

        subtotal = Decimal("0")
        vat_total = Decimal("0")

        max_len = max(
            len(item_ids),
            len(item_names),
            len(quantities),
            len(units),
            len(prices),
            len(vat_amounts),
            0,
        )

        for index in range(max_len):
            raw_name = item_names[index].strip() if index < len(item_names) else ""
            raw_quantity = quantities[index] if index < len(quantities) else ""
            raw_unit = units[index].strip() if index < len(units) else ""
            raw_price = prices[index] if index < len(prices) else ""
            raw_vat = vat_amounts[index] if index < len(vat_amounts) else ""
            raw_item_id = item_ids[index].strip() if index < len(item_ids) else ""

            if not raw_name:
                continue

            quantity = _to_decimal_or_default(raw_quantity, "1")
            price = _to_decimal_or_default(raw_price, "0")
            vat_percentage = _to_decimal_or_default(raw_vat, "0")
            line_net = quantity * price
            vat_value = (line_net * vat_percentage) / Decimal("100")
            line_total = line_net + vat_value

            parsed_item_id = None
            if raw_item_id:
                try:
                    parsed_item_id = int(raw_item_id)
                except ValueError:
                    parsed_item_id = None

            db.execute(
                """
                INSERT INTO purchase_invoice_items (
                    purchase_invoice_id, item_id, item_name, quantity, unit, price, vat_amount, line_total
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    purchase_id,
                    parsed_item_id,
                    raw_name,
                    float(quantity),
                    raw_unit or "1",
                    float(price),
                    float(vat_percentage),
                    float(line_total),
                ),
            )

            subtotal += line_net
            vat_total += vat_value

        total = subtotal + vat_total
        db.execute(
            "UPDATE purchase_invoices SET subtotal = ?, vat_total = ?, total = ? WHERE id = ?",
            (float(subtotal), float(vat_total), float(total), purchase_id),
        )
        db.commit()
        return redirect(url_for("view_purchase_page", purchase_id=purchase_id))

    @app.route("/purchases/<int:purchase_id>/edit")
    def edit_purchase_page(purchase_id):
        db = get_db()
        ensure_purchase_invoices_tables()

        purchase = db.execute(
            "SELECT * FROM purchase_invoices WHERE id = ?",
            (purchase_id,),
        ).fetchone()
        if purchase is None:
            return redirect(url_for("purchases_page"))

        purchase_items = db.execute(
            """
            SELECT id, item_id, item_name, quantity, unit, price, vat_amount, line_total
            FROM purchase_invoice_items
            WHERE purchase_invoice_id = ?
            ORDER BY id ASC
            """,
            (purchase_id,),
        ).fetchall()

        vendors = db.execute(
            """
            SELECT id, vendor_name, vendor_tax_number, registration_name,
                   phone_number, address, website, country, address_2
            FROM vendors
            ORDER BY id DESC
            """
        ).fetchall()
        items = db.execute(
            """
            SELECT id, name, unit, price, vat_amount
            FROM items
            ORDER BY id DESC
            """
        ).fetchall()

        currencies = db.execute(
            """
            SELECT code, name
            FROM payment_currencies
            ORDER BY code ASC
            """
        ).fetchall()
        return render_template(
            "purchase_form.html",
            page_title="Edit Purchase Invoice",
            active_menu="Purcheases",
            form_action=url_for("update_purchase", purchase_id=purchase_id),
            purchase=purchase,
            purchase_items=purchase_items,
            vendors=vendors,
            items_catalog=items,
            currencies=currencies,
            generated_purchase_number=purchase["purchase_number"],
        )

    @app.post("/purchases/<int:purchase_id>/edit")
    def update_purchase(purchase_id):
        db = get_db()
        ensure_purchase_invoices_tables()

        purchase = db.execute("SELECT id FROM purchase_invoices WHERE id = ?", (purchase_id,)).fetchone()
        if purchase is None:
            return redirect(url_for("purchases_page"))

        purchase_number = _ensure_unique_purchase_number(
            db,
            request.form.get("purchase_number"),
            exclude_purchase_id=purchase_id,
        )
        purchase_date = request.form.get("purchase_date", "").strip()
        if not purchase_date:
            return redirect(url_for("edit_purchase_page", purchase_id=purchase_id))

        vendor_id = request.form.get("vendor_id", "").strip()
        parsed_vendor_id = None
        if vendor_id:
            try:
                parsed_vendor_id = int(vendor_id)
            except ValueError:
                parsed_vendor_id = None

        vendor_name = request.form.get("vendor_name", "").strip() or None
        vendor_tax_number = request.form.get("vendor_tax_number", "").strip() or None
        registration_name = request.form.get("registration_name", "").strip() or None
        phone_number = request.form.get("phone_number", "").strip() or None
        address = request.form.get("address", "").strip() or None
        website = request.form.get("website", "").strip() or None
        country = request.form.get("country", "").strip() or None
        address_2 = request.form.get("address_2", "").strip() or None

        db.execute(
            """
            UPDATE purchase_invoices
            SET purchase_number = ?, purchase_date = ?, vendor_id = ?,
                vendor_name = ?, vendor_tax_number = ?, registration_name = ?,
                phone_number = ?, address = ?, website = ?, country = ?, address_2 = ?
            WHERE id = ?
            """,
            (
                purchase_number,
                purchase_date,
                parsed_vendor_id,
                vendor_name,
                vendor_tax_number,
                registration_name,
                phone_number,
                address,
                website,
                country,
                address_2,
                purchase_id,
            ),
        )

        db.execute("DELETE FROM purchase_invoice_items WHERE purchase_invoice_id = ?", (purchase_id,))

        item_ids = request.form.getlist("item_id[]")
        item_names = request.form.getlist("item_name[]")
        quantities = request.form.getlist("quantity[]")
        units = request.form.getlist("unit[]")
        prices = request.form.getlist("price[]")
        vat_amounts = request.form.getlist("vat_amount[]")

        subtotal = Decimal("0")
        vat_total = Decimal("0")

        max_len = max(
            len(item_ids),
            len(item_names),
            len(quantities),
            len(units),
            len(prices),
            len(vat_amounts),
            0,
        )

        for index in range(max_len):
            raw_name = item_names[index].strip() if index < len(item_names) else ""
            raw_quantity = quantities[index] if index < len(quantities) else ""
            raw_unit = units[index].strip() if index < len(units) else ""
            raw_price = prices[index] if index < len(prices) else ""
            raw_vat = vat_amounts[index] if index < len(vat_amounts) else ""
            raw_item_id = item_ids[index].strip() if index < len(item_ids) else ""

            if not raw_name:
                continue

            quantity = _to_decimal_or_default(raw_quantity, "1")
            price = _to_decimal_or_default(raw_price, "0")
            vat_percentage = _to_decimal_or_default(raw_vat, "0")
            line_net = quantity * price
            vat_value = (line_net * vat_percentage) / Decimal("100")
            line_total = line_net + vat_value

            parsed_item_id = None
            if raw_item_id:
                try:
                    parsed_item_id = int(raw_item_id)
                except ValueError:
                    parsed_item_id = None

            db.execute(
                """
                INSERT INTO purchase_invoice_items (
                    purchase_invoice_id, item_id, item_name, quantity, unit, price, vat_amount, line_total
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    purchase_id,
                    parsed_item_id,
                    raw_name,
                    float(quantity),
                    raw_unit or "1",
                    float(price),
                    float(vat_percentage),
                    float(line_total),
                ),
            )

            subtotal += line_net
            vat_total += vat_value

        total = subtotal + vat_total
        db.execute(
            "UPDATE purchase_invoices SET subtotal = ?, vat_total = ?, total = ? WHERE id = ?",
            (float(subtotal), float(vat_total), float(total), purchase_id),
        )
        db.commit()
        return redirect(url_for("view_purchase_page", purchase_id=purchase_id))

    @app.post("/purchases/delete")
    def delete_purchase():
        db = get_db()
        ensure_purchase_invoices_tables()

        purchase_id = request.form.get("purchase_id", "").strip()
        try:
            parsed_purchase_id = int(purchase_id)
        except ValueError:
            return redirect(url_for("purchases_page"))

        db.execute("DELETE FROM purchase_invoice_items WHERE purchase_invoice_id = ?", (parsed_purchase_id,))
        db.execute("DELETE FROM purchase_invoices WHERE id = ?", (parsed_purchase_id,))
        db.commit()
        return redirect(url_for("purchases_page"))

    @app.route("/purchases/<int:purchase_id>")
    def view_purchase_page(purchase_id):
        db = get_db()
        ensure_purchase_invoices_tables()

        purchase = db.execute("SELECT * FROM purchase_invoices WHERE id = ?", (purchase_id,)).fetchone()
        if purchase is None:
            return redirect(url_for("purchases_page"))

        purchase_items = db.execute(
            """
            SELECT item_name, quantity, unit, price, vat_amount, line_total
            FROM purchase_invoice_items
            WHERE purchase_invoice_id = ?
            ORDER BY id ASC
            """,
            (purchase_id,),
        ).fetchall()

        return render_template(
            "purchase_view.html",
            page_title=f"Purchase {purchase['purchase_number']}",
            active_menu="Purcheases",
            purchase=purchase,
            purchase_items=purchase_items,
        )

    @app.route("/assets")
    def assets_page():
        db = get_db()
        ensure_purchase_invoices_tables()
        ensure_invoices_tables()

        purchase_rows = db.execute(
            """
            SELECT
                pii.item_id,
                pii.item_name,
                pii.quantity,
                pii.unit,
                pii.price,
                pi.purchase_date
            FROM purchase_invoice_items pii
            JOIN purchase_invoices pi ON pi.id = pii.purchase_invoice_id
            ORDER BY pi.purchase_date ASC, pii.id ASC
            """
        ).fetchall()

        invoice_rows = db.execute(
            """
            SELECT
                ii.item_id,
                ii.item_name,
                ii.quantity,
                ii.unit,
                ii.price,
                i.invoice_date
            FROM invoice_items ii
            JOIN invoices i ON i.id = ii.invoice_id
            ORDER BY i.invoice_date ASC, ii.id ASC
            """
        ).fetchall()

        assets = {}

        for row in purchase_rows:
            key = _asset_key(row["item_id"], row["item_name"])
            if key not in assets:
                assets[key] = {
                    "item_name": row["item_name"] or "Unnamed Item",
                    "unit": row["unit"] or "1",
                    "purchased_qty": Decimal("0"),
                    "sold_qty": Decimal("0"),
                    "purchase_value": Decimal("0"),
                    "last_purchase_date": None,
                    "last_sale_date": None,
                }

            quantity = _to_decimal_or_default(str(row["quantity"]), "0")
            price = _to_decimal_or_default(str(row["price"]), "0")

            assets[key]["purchased_qty"] += quantity
            assets[key]["purchase_value"] += quantity * price
            assets[key]["unit"] = row["unit"] or assets[key]["unit"]
            assets[key]["item_name"] = row["item_name"] or assets[key]["item_name"]
            assets[key]["last_purchase_date"] = row["purchase_date"]

        for row in invoice_rows:
            key = _asset_key(row["item_id"], row["item_name"])
            if key not in assets:
                assets[key] = {
                    "item_name": row["item_name"] or "Unnamed Item",
                    "unit": row["unit"] or "1",
                    "purchased_qty": Decimal("0"),
                    "sold_qty": Decimal("0"),
                    "purchase_value": Decimal("0"),
                    "last_purchase_date": None,
                    "last_sale_date": None,
                }

            quantity = _to_decimal_or_default(str(row["quantity"]), "0")
            assets[key]["sold_qty"] += quantity
            assets[key]["unit"] = row["unit"] or assets[key]["unit"]
            assets[key]["item_name"] = row["item_name"] or assets[key]["item_name"]
            assets[key]["last_sale_date"] = row["invoice_date"]

        rows = []
        total_stock_value = Decimal("0")
        total_available_qty = Decimal("0")

        search_query = request.args.get("q", "").strip().lower()

        for data in assets.values():
            purchased_qty = data["purchased_qty"]
            sold_qty = data["sold_qty"]
            available_qty = purchased_qty - sold_qty
            average_cost = (
                (data["purchase_value"] / purchased_qty) if purchased_qty > 0 else Decimal("0")
            )
            stock_value = available_qty * average_cost

            row_payload = {
                "item_name": data["item_name"],
                "unit": data["unit"],
                "purchased_qty": float(purchased_qty),
                "sold_qty": float(sold_qty),
                "available_qty": float(available_qty),
                "average_cost": float(average_cost),
                "stock_value": float(stock_value),
                "last_purchase_date": data["last_purchase_date"],
                "last_sale_date": data["last_sale_date"],
            }

            if search_query and search_query not in row_payload["item_name"].lower():
                continue

            rows.append(row_payload)
            total_stock_value += stock_value
            total_available_qty += available_qty

        rows.sort(key=lambda entry: entry["item_name"].lower())

        return render_template(
            "assets.html",
            page_title="Assets",
            active_menu="Assets",
            assets=rows,
            search_query=request.args.get("q", "").strip(),
            total_stock_value=float(total_stock_value),
            total_available_qty=float(total_available_qty),
        )

    @app.route("/expenses")
    def expenses_page():
        db = get_db()
        ensure_expenses_table()
        ensure_payment_tables()

        payment_methods = db.execute(
            """
            SELECT id, name, method_type
            FROM payment_methods
            ORDER BY name ASC
            """
        ).fetchall()

        search_query = request.args.get("q", "").strip()
        if search_query:
            like_query = f"%{search_query}%"
            expenses = db.execute(
                """
                SELECT
                    e.id,
                    e.expense_number,
                    e.expense_date,
                    e.title,
                    e.category,
                    e.payment_method_id,
                    e.amount,
                    e.notes,
                    m.name AS payment_method_name,
                    m.method_type AS payment_method_type
                FROM expenses e
                LEFT JOIN payment_methods m ON m.id = e.payment_method_id
                WHERE expense_number LIKE ?
                   OR expense_date LIKE ?
                   OR title LIKE ?
                   OR category LIKE ?
                   OR m.name LIKE ?
                   OR notes LIKE ?
                ORDER BY e.id DESC
                """,
                (like_query, like_query, like_query, like_query, like_query, like_query),
            ).fetchall()
        else:
            expenses = db.execute(
                """
                SELECT
                    e.id,
                    e.expense_number,
                    e.expense_date,
                    e.title,
                    e.category,
                    e.payment_method_id,
                    e.amount,
                    e.notes,
                    m.name AS payment_method_name,
                    m.method_type AS payment_method_type
                FROM expenses e
                LEFT JOIN payment_methods m ON m.id = e.payment_method_id
                ORDER BY e.id DESC
                """
            ).fetchall()

        currencies = db.execute(
            """
            SELECT code, name
            FROM payment_currencies
            ORDER BY code ASC
            """
        ).fetchall()
        return render_template(
            "expenses.html",
            page_title="Expenses",
            active_menu="Expenses",
            expenses=expenses,
            payment_methods=payment_methods,
            currencies=currencies,
            search_query=search_query,
        )

    @app.post("/expenses/add")
    def add_expense():
        db = get_db()
        ensure_expenses_table()
        ensure_payment_tables()

        expense_date = request.form.get("expense_date", "").strip()
        title = request.form.get("title", "").strip()
        category = request.form.get("category", "").strip() or None
        notes = request.form.get("notes", "").strip() or None
        payment_method_id = request.form.get("payment_method_id", "").strip()
        amount = _to_decimal_or_default(request.form.get("amount", "0"), "0")

        if not expense_date or not title or not payment_method_id:
            return redirect(url_for("expenses_page"))

        try:
            parsed_payment_method_id = int(payment_method_id)
        except ValueError:
            return redirect(url_for("expenses_page"))

        payment_method_exists = db.execute(
            "SELECT id FROM payment_methods WHERE id = ?",
            (parsed_payment_method_id,),
        ).fetchone()
        if payment_method_exists is None:
            return redirect(url_for("expenses_page"))

        expense_number = _generate_expense_number(db)
        insert_result = db.execute(
            """
            INSERT INTO expenses (expense_number, expense_date, title, category, payment_method_id, amount, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                expense_number,
                expense_date,
                title,
                category,
                parsed_payment_method_id,
                float(amount),
                notes,
            ),
        )

        expense_id = insert_result.lastrowid
        currency_code = request.form.get("currency_code", "").strip().upper() or _get_default_currency_code(db)
        transaction_notes = notes or f"Expense payment for {title}"
        db.execute(
            """
            INSERT INTO payment_transactions (
                transaction_date,
                transaction_type,
                reference_type,
                reference_id,
                amount,
                currency_code,
                method_id,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                expense_date,
                "expense_payment",
                "expense",
                expense_id,
                float(amount),
                currency_code,
                parsed_payment_method_id,
                transaction_notes,
            ),
        )
        db.commit()
        return redirect(url_for("expenses_page"))

    @app.post("/expenses/edit")
    def edit_expense():
        db = get_db()
        ensure_expenses_table()
        ensure_payment_tables()

        expense_id = request.form.get("expense_id", "").strip()
        expense_date = request.form.get("expense_date", "").strip()
        title = request.form.get("title", "").strip()
        category = request.form.get("category", "").strip() or None
        notes = request.form.get("notes", "").strip() or None
        payment_method_id = request.form.get("payment_method_id", "").strip()
        amount = _to_decimal_or_default(request.form.get("amount", "0"), "0")

        if not expense_id or not expense_date or not title or not payment_method_id:
            return redirect(url_for("expenses_page"))

        try:
            parsed_expense_id = int(expense_id)
        except ValueError:
            return redirect(url_for("expenses_page"))

        try:
            parsed_payment_method_id = int(payment_method_id)
        except ValueError:
            return redirect(url_for("expenses_page"))

        payment_method_exists = db.execute(
            "SELECT id FROM payment_methods WHERE id = ?",
            (parsed_payment_method_id,),
        ).fetchone()
        if payment_method_exists is None:
            return redirect(url_for("expenses_page"))

        db.execute(
            """
            UPDATE expenses
            SET expense_date = ?, title = ?, category = ?, payment_method_id = ?, amount = ?, notes = ?
            WHERE id = ?
            """,
            (
                expense_date,
                title,
                category,
                parsed_payment_method_id,
                float(amount),
                notes,
                parsed_expense_id,
            ),
        )

        transaction_notes = notes or f"Expense payment for {title}"
        updated_transaction = db.execute(
            """
            UPDATE payment_transactions
            SET transaction_date = ?, amount = ?, method_id = ?, notes = ?
            WHERE reference_type = 'expense'
              AND reference_id = ?
              AND transaction_type = 'expense_payment'
            """,
            (
                expense_date,
                float(amount),
                parsed_payment_method_id,
                transaction_notes,
                parsed_expense_id,
            ),
        )

        if updated_transaction.rowcount == 0:
            currency_code = request.form.get("currency_code") or _get_default_currency_code(db)
            db.execute(
                """
                INSERT INTO payment_transactions (
                    transaction_date,
                    transaction_type,
                    reference_type,
                    reference_id,
                    amount,
                    currency_code,
                    method_id,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    expense_date,
                    "expense_payment",
                    "expense",
                    parsed_expense_id,
                    float(amount),
                    currency_code,
                    parsed_payment_method_id,
                    transaction_notes,
                ),
            )
        db.commit()
        return redirect(url_for("expenses_page"))

    @app.post("/expenses/delete")
    def delete_expense():
        db = get_db()
        ensure_expenses_table()
        ensure_payment_tables()

        expense_id = request.form.get("expense_id", "").strip()
        try:
            parsed_expense_id = int(expense_id)
        except ValueError:
            return redirect(url_for("expenses_page"))

        db.execute(
            """
            DELETE FROM payment_transactions
            WHERE reference_type = 'expense'
              AND reference_id = ?
              AND transaction_type = 'expense_payment'
            """,
            (parsed_expense_id,),
        )
        db.execute("DELETE FROM expenses WHERE id = ?", (parsed_expense_id,))
        db.commit()
        return redirect(url_for("expenses_page"))

    @app.route("/payments")
    def payments_page():
        db = get_db()
        ensure_payment_tables()

        methods = db.execute(
            """
            SELECT id, name, method_type, account_identifier, details
            FROM payment_methods
            ORDER BY id DESC
            """
        ).fetchall()

        currencies = db.execute(
            """
            SELECT id, code, name, symbol, is_crypto
            FROM payment_currencies
            ORDER BY code ASC
            """
        ).fetchall()

        transactions = db.execute(
            """
            SELECT
                t.id,
                t.transaction_date,
                t.transaction_type,
                t.reference_type,
                t.reference_id,
                t.amount,
                t.currency_code,
                t.method_id,
                t.notes,
                m.name AS method_name
            FROM payment_transactions t
            LEFT JOIN payment_methods m ON m.id = t.method_id
            ORDER BY t.id DESC
            """
        ).fetchall()

        invoices = db.execute(
            """
            SELECT id, invoice_number, invoice_date, total
            FROM invoices
            ORDER BY id DESC
            """
        ).fetchall()

        purchases = db.execute(
            """
            SELECT id, purchase_number, purchase_date, total
            FROM purchase_invoices
            ORDER BY id DESC
            """
        ).fetchall()

        return render_template(
            "payments.html",
            page_title="Payment",
            active_menu="Payment",
            methods=methods,
            currencies=currencies,
            transactions=transactions,
            invoices=invoices,
            purchases=purchases,
        )

    @app.post("/payments/methods/add")
    def add_payment_method():
        db = get_db()
        ensure_payment_tables()

        name = request.form.get("name", "").strip()
        method_type = request.form.get("method_type", "").strip()
        account_identifier = request.form.get("account_identifier", "").strip() or None
        details = request.form.get("details", "").strip() or None

        if not name or not method_type:
            return redirect(url_for("payments_page"))

        db.execute(
            """
            INSERT INTO payment_methods (name, method_type, account_identifier, details)
            VALUES (?, ?, ?, ?)
            """,
            (name, method_type, account_identifier, details),
        )
        db.commit()
        return redirect(url_for("payments_page"))

    @app.post("/payments/methods/edit")
    def edit_payment_method():
        db = get_db()
        ensure_payment_tables()

        method_id = request.form.get("method_id", "").strip()
        name = request.form.get("name", "").strip()
        method_type = request.form.get("method_type", "").strip()
        account_identifier = request.form.get("account_identifier", "").strip() or None
        details = request.form.get("details", "").strip() or None

        if not method_id or not name or not method_type:
            return redirect(url_for("payments_page"))

        try:
            parsed_method_id = int(method_id)
        except ValueError:
            return redirect(url_for("payments_page"))

        db.execute(
            """
            UPDATE payment_methods
            SET name = ?, method_type = ?, account_identifier = ?, details = ?
            WHERE id = ?
            """,
            (name, method_type, account_identifier, details, parsed_method_id),
        )
        db.commit()
        return redirect(url_for("payments_page"))

    @app.post("/payments/methods/delete")
    def delete_payment_method():
        db = get_db()
        ensure_payment_tables()

        method_id = request.form.get("method_id", "").strip()
        try:
            parsed_method_id = int(method_id)
        except ValueError:
            return redirect(url_for("payments_page"))

        db.execute("DELETE FROM payment_methods WHERE id = ?", (parsed_method_id,))
        db.commit()
        return redirect(url_for("payments_page"))

    @app.post("/payments/currencies/add")
    def add_payment_currency():
        db = get_db()
        ensure_payment_tables()

        code = request.form.get("code", "").strip().upper()
        name = request.form.get("name", "").strip()
        symbol = request.form.get("symbol", "").strip() or None
        is_crypto = 1 if request.form.get("is_crypto") == "on" else 0

        if not code or not name:
            return redirect(url_for("payments_page"))

        db.execute(
            """
            INSERT OR IGNORE INTO payment_currencies (code, name, symbol, is_crypto)
            VALUES (?, ?, ?, ?)
            """,
            (code, name, symbol, is_crypto),
        )
        db.commit()
        return redirect(url_for("payments_page"))

    @app.post("/payments/currencies/edit")
    def edit_payment_currency():
        db = get_db()
        ensure_payment_tables()

        currency_id = request.form.get("currency_id", "").strip()
        code = request.form.get("code", "").strip().upper()
        name = request.form.get("name", "").strip()
        symbol = request.form.get("symbol", "").strip() or None
        is_crypto = 1 if request.form.get("is_crypto") == "on" else 0

        if not currency_id or not code or not name:
            return redirect(url_for("payments_page"))

        try:
            parsed_currency_id = int(currency_id)
        except ValueError:
            return redirect(url_for("payments_page"))

        db.execute(
            """
            UPDATE payment_currencies
            SET code = ?, name = ?, symbol = ?, is_crypto = ?
            WHERE id = ?
            """,
            (code, name, symbol, is_crypto, parsed_currency_id),
        )
        db.commit()
        return redirect(url_for("payments_page"))

    @app.post("/payments/currencies/delete")
    def delete_payment_currency():
        db = get_db()
        ensure_payment_tables()

        currency_id = request.form.get("currency_id", "").strip()
        try:
            parsed_currency_id = int(currency_id)
        except ValueError:
            return redirect(url_for("payments_page"))

        db.execute("DELETE FROM payment_currencies WHERE id = ?", (parsed_currency_id,))
        db.commit()
        return redirect(url_for("payments_page"))

    @app.post("/payments/transactions/add")
    def add_payment_transaction():
        db = get_db()
        ensure_payment_tables()

        transaction_date = request.form.get("transaction_date", "").strip()
        transaction_type = request.form.get("transaction_type", "").strip()
        reference_type = request.form.get("reference_type", "").strip() or None
        reference_id = request.form.get("reference_id", "").strip()
        amount = _to_decimal_or_default(request.form.get("amount", "0"), "0")
        currency_code = request.form.get("currency_code", "").strip().upper()
        method_id = request.form.get("method_id", "").strip()
        notes = request.form.get("notes", "").strip() or None

        if not transaction_date or not transaction_type or not currency_code:
            return redirect(url_for("payments_page"))

        parsed_reference_id = None
        if reference_id:
            try:
                parsed_reference_id = int(reference_id)
            except ValueError:
                parsed_reference_id = None

        parsed_method_id = None
        if method_id:
            try:
                parsed_method_id = int(method_id)
            except ValueError:
                parsed_method_id = None

        db.execute(
            """
            INSERT INTO payment_transactions (
                transaction_date,
                transaction_type,
                reference_type,
                reference_id,
                amount,
                currency_code,
                method_id,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transaction_date,
                transaction_type,
                reference_type,
                parsed_reference_id,
                float(amount),
                currency_code,
                parsed_method_id,
                notes,
            ),
        )
        db.commit()
        return redirect(url_for("payments_page"))

    @app.post("/payments/transactions/delete")
    def delete_payment_transaction():
        db = get_db()
        ensure_payment_tables()

        transaction_id = request.form.get("transaction_id", "").strip()
        try:
            parsed_transaction_id = int(transaction_id)
        except ValueError:
            return redirect(url_for("payments_page"))

        db.execute("DELETE FROM payment_transactions WHERE id = ?", (parsed_transaction_id,))
        db.commit()
        return redirect(url_for("payments_page"))

    @app.route("/report")
    def report_page():
        db = get_db()
        ensure_invoices_tables()
        ensure_purchase_invoices_tables()
        ensure_expenses_table()
        ensure_payment_tables()

        sold_total_raw = db.execute("SELECT COALESCE(SUM(total), 0) AS total FROM invoices").fetchone()
        bought_total_raw = db.execute(
            "SELECT COALESCE(SUM(total), 0) AS total FROM purchase_invoices"
        ).fetchone()
        expenses_total_raw = db.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total FROM expenses"
        ).fetchone()

        sold_total = Decimal(str(sold_total_raw["total"] or 0))
        bought_total = Decimal(str(bought_total_raw["total"] or 0))
        expenses_total = Decimal(str(expenses_total_raw["total"] or 0))

        net_result = sold_total - bought_total - expenses_total
        benefit = net_result if net_result > 0 else Decimal("0")
        loss = -net_result if net_result < 0 else Decimal("0")

        invoice_outstanding_raw = db.execute(
            """
            SELECT COALESCE(SUM(i.total), 0) - COALESCE(SUM(r.received_amount), 0) AS outstanding
            FROM invoices i
            LEFT JOIN (
                SELECT reference_id, SUM(amount) AS received_amount
                FROM payment_transactions
                WHERE reference_type = 'invoice'
                  AND transaction_type = 'invoice_receipt'
                GROUP BY reference_id
            ) r ON r.reference_id = i.id
            """
        ).fetchone()

        purchase_outstanding_raw = db.execute(
            """
            SELECT COALESCE(SUM(p.total), 0) - COALESCE(SUM(pay.paid_amount), 0) AS outstanding
            FROM purchase_invoices p
            LEFT JOIN (
                SELECT reference_id, SUM(amount) AS paid_amount
                FROM payment_transactions
                WHERE reference_type = 'purchase'
                  AND transaction_type = 'purchase_payment'
                GROUP BY reference_id
            ) pay ON pay.reference_id = p.id
            """
        ).fetchone()

        should_receive = Decimal(str(invoice_outstanding_raw["outstanding"] or 0))
        i_owe = Decimal(str(purchase_outstanding_raw["outstanding"] or 0))
        debt = i_owe

        invoice_vat_rows = db.execute(
            """
            SELECT
                i.id,
                i.invoice_number,
                i.invoice_date,
                i.customer_name,
                COALESCE(i.total, 0) AS total,
                COALESCE(i.vat_total, 0) AS vat_total,
                COALESCE(r.received_amount, 0) AS received_amount
            FROM invoices i
            LEFT JOIN (
                SELECT reference_id, SUM(amount) AS received_amount
                FROM payment_transactions
                WHERE reference_type = 'invoice'
                  AND transaction_type = 'invoice_receipt'
                GROUP BY reference_id
            ) r ON r.reference_id = i.id
            """
        ).fetchall()

        purchase_vat_rows = db.execute(
            """
            SELECT
                p.id,
                p.purchase_number,
                p.purchase_date,
                p.vendor_name,
                COALESCE(p.total, 0) AS total,
                COALESCE(p.vat_total, 0) AS vat_total,
                COALESCE(pay.paid_amount, 0) AS paid_amount
            FROM purchase_invoices p
            LEFT JOIN (
                SELECT reference_id, SUM(amount) AS paid_amount
                FROM payment_transactions
                WHERE reference_type = 'purchase'
                  AND transaction_type = 'purchase_payment'
                GROUP BY reference_id
            ) pay ON pay.reference_id = p.id
            """
        ).fetchall()

        received_vat = Decimal("0")
        vat_entries = []
        quarterly_map = {}

        def ensure_quarter_bucket(quarter_key):
            if quarter_key not in quarterly_map:
                quarterly_map[quarter_key] = {
                    "quarter": quarter_key,
                    "output_vat": Decimal("0"),
                    "input_vat": Decimal("0"),
                }

        def quarter_from_date(date_value):
            normalized = (date_value or "").strip()
            if len(normalized) < 7:
                return "Unknown"
            try:
                year, month = normalized.split("-")[:2]
                month_number = int(month)
                quarter_number = ((month_number - 1) // 3) + 1
                return f"{year}-Q{quarter_number}"
            except (ValueError, IndexError):
                return "Unknown"

        for row in invoice_vat_rows:
            total_amount = Decimal(str(row["total"] or 0))
            vat_amount = Decimal(str(row["vat_total"] or 0))
            received_amount = Decimal(str(row["received_amount"] or 0))
            if total_amount <= 0 or vat_amount <= 0:
                continue
            received_ratio = (
                min(received_amount / total_amount, Decimal("1"))
                if received_amount > 0
                else Decimal("0")
            )
            recognized_vat = vat_amount * received_ratio
            received_vat += recognized_vat

            quarter_key = quarter_from_date(row["invoice_date"])
            ensure_quarter_bucket(quarter_key)
            quarterly_map[quarter_key]["output_vat"] += recognized_vat

            vat_entries.append(
                {
                    "entry_type": "Sales VAT",
                    "date": row["invoice_date"] or "-",
                    "quarter": quarter_key,
                    "reference_number": row["invoice_number"] or f"INV-{row['id']}",
                    "party_name": row["customer_name"] or "-",
                    "total_amount": float(total_amount),
                    "vat_amount": float(vat_amount),
                    "settled_amount": float(received_amount),
                    "recognized_vat": float(recognized_vat),
                }
            )

        paid_vat = Decimal("0")
        for row in purchase_vat_rows:
            total_amount = Decimal(str(row["total"] or 0))
            vat_amount = Decimal(str(row["vat_total"] or 0))
            paid_amount = Decimal(str(row["paid_amount"] or 0))
            if total_amount <= 0 or vat_amount <= 0:
                continue
            paid_ratio = (
                min(paid_amount / total_amount, Decimal("1"))
                if paid_amount > 0
                else Decimal("0")
            )
            recognized_vat = vat_amount * paid_ratio
            paid_vat += recognized_vat

            quarter_key = quarter_from_date(row["purchase_date"])
            ensure_quarter_bucket(quarter_key)
            quarterly_map[quarter_key]["input_vat"] += recognized_vat

            vat_entries.append(
                {
                    "entry_type": "Purchase VAT",
                    "date": row["purchase_date"] or "-",
                    "quarter": quarter_key,
                    "reference_number": row["purchase_number"] or f"PUR-{row['id']}",
                    "party_name": row["vendor_name"] or "-",
                    "total_amount": float(total_amount),
                    "vat_amount": float(vat_amount),
                    "settled_amount": float(paid_amount),
                    "recognized_vat": float(recognized_vat),
                }
            )

        vat_entries.sort(
            key=lambda item: (item["date"] if item["date"] != "-" else "0000-00-00"), reverse=True
        )

        vat_quarterly = []
        for quarter_key in sorted(quarterly_map.keys()):
            output_vat = quarterly_map[quarter_key]["output_vat"]
            input_vat = quarterly_map[quarter_key]["input_vat"]
            net_vat = output_vat - input_vat
            vat_quarterly.append(
                {
                    "quarter": quarter_key,
                    "output_vat": float(output_vat),
                    "input_vat": float(input_vat),
                    "net_vat": float(net_vat),
                }
            )

        monthly_invoices = db.execute(
            """
            SELECT strftime('%Y-%m', invoice_date) AS period, COALESCE(SUM(total), 0) AS total
            FROM invoices
            WHERE invoice_date IS NOT NULL AND invoice_date != ''
            GROUP BY strftime('%Y-%m', invoice_date)
            ORDER BY period ASC
            """
        ).fetchall()

        monthly_purchases = db.execute(
            """
            SELECT strftime('%Y-%m', purchase_date) AS period, COALESCE(SUM(total), 0) AS total
            FROM purchase_invoices
            WHERE purchase_date IS NOT NULL AND purchase_date != ''
            GROUP BY strftime('%Y-%m', purchase_date)
            ORDER BY period ASC
            """
        ).fetchall()

        monthly_expenses = db.execute(
            """
            SELECT strftime('%Y-%m', expense_date) AS period, COALESCE(SUM(amount), 0) AS total
            FROM expenses
            WHERE expense_date IS NOT NULL AND expense_date != ''
            GROUP BY strftime('%Y-%m', expense_date)
            ORDER BY period ASC
            """
        ).fetchall()

        monthly_map = {}
        for row in monthly_invoices:
            period = row["period"]
            if not period:
                continue
            monthly_map.setdefault(
                period, {"sold": Decimal("0"), "bought": Decimal("0"), "expenses": Decimal("0")}
            )
            monthly_map[period]["sold"] = Decimal(str(row["total"] or 0))

        for row in monthly_purchases:
            period = row["period"]
            if not period:
                continue
            monthly_map.setdefault(
                period, {"sold": Decimal("0"), "bought": Decimal("0"), "expenses": Decimal("0")}
            )
            monthly_map[period]["bought"] = Decimal(str(row["total"] or 0))

        for row in monthly_expenses:
            period = row["period"]
            if not period:
                continue
            monthly_map.setdefault(
                period, {"sold": Decimal("0"), "bought": Decimal("0"), "expenses": Decimal("0")}
            )
            monthly_map[period]["expenses"] = Decimal(str(row["total"] or 0))

        monthly_labels = sorted(monthly_map.keys())
        monthly_sold = [float(monthly_map[p]["sold"]) for p in monthly_labels]
        monthly_bought = [float(monthly_map[p]["bought"]) for p in monthly_labels]
        monthly_expenses_values = [float(monthly_map[p]["expenses"]) for p in monthly_labels]
        monthly_turnover = [
            float(monthly_map[p]["sold"] - monthly_map[p]["bought"] - monthly_map[p]["expenses"])
            for p in monthly_labels
        ]

        yearly_map = {}
        for period, sold, bought, expenses in zip(
            monthly_labels, monthly_sold, monthly_bought, monthly_expenses_values
        ):
            year = period.split("-")[0]
            yearly_map.setdefault(year, {"sold": 0.0, "bought": 0.0, "expenses": 0.0})
            yearly_map[year]["sold"] += sold
            yearly_map[year]["bought"] += bought
            yearly_map[year]["expenses"] += expenses

        yearly_labels = sorted(yearly_map.keys())
        yearly_sold = [yearly_map[y]["sold"] for y in yearly_labels]
        yearly_bought = [yearly_map[y]["bought"] for y in yearly_labels]
        yearly_expenses_values = [yearly_map[y]["expenses"] for y in yearly_labels]
        yearly_turnover = [
            yearly_map[y]["sold"] - yearly_map[y]["bought"] - yearly_map[y]["expenses"]
            for y in yearly_labels
        ]

        return render_template(
            "report.html",
            page_title="Report",
            active_menu="Report",
            sold_total=float(sold_total),
            bought_total=float(bought_total),
            expenses_total=float(expenses_total),
            benefit=float(benefit),
            loss=float(loss),
            debt=float(debt),
            should_receive=float(should_receive),
            i_owe=float(i_owe),
            received_vat=float(received_vat),
            paid_vat=float(paid_vat),
            vat_quarterly=vat_quarterly,
            vat_entries=vat_entries,
            monthly_labels=json.dumps(monthly_labels),
            monthly_sold=json.dumps(monthly_sold),
            monthly_bought=json.dumps(monthly_bought),
            monthly_expenses=json.dumps(monthly_expenses_values),
            monthly_turnover=json.dumps(monthly_turnover),
            yearly_labels=json.dumps(yearly_labels),
            yearly_sold=json.dumps(yearly_sold),
            yearly_bought=json.dumps(yearly_bought),
            yearly_expenses=json.dumps(yearly_expenses_values),
            yearly_turnover=json.dumps(yearly_turnover),
        )

    return app
