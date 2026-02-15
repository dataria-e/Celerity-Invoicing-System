# Celerity Invoicing System

Celerity is a complete finance and invoicing web application built with Flask and SQLite. It is designed for small and medium businesses that need billing, expense tracking, purchasing, payment tracking, VAT visibility, and simple user administration in one dashboard.

## Version

Current release: **1.0.0**

This is the first public release and includes the full feature set listed below.

---

## Quick Start (60 Seconds)

### macOS / Linux

```bash
git clone https://github.com/dataria-e/Celerity-Invoicing-System.git && cd "Invoicing System" && python3 -m venv .venv && source .venv/bin/activate && python -m pip install --upgrade pip && pip install -r requirements.txt && python app.py
```

### Windows (PowerShell)

```powershell
git clone https://github.com/dataria-e/Celerity-Invoicing-System.git; Set-Location "Invoicing System"; py -m venv .venv; .\.venv\Scripts\Activate.ps1; python -m pip install --upgrade pip; pip install -r requirements.txt; python app.py
```

Open: `http://127.0.0.1:5000`  
First login: `admin / admin123`

---

## Core Features

### 1) Authentication and Access Control

- Login required for all business pages.
- User session handling powered by Flask-Login.
- Admin can manage users from the Users page.
- User records support active/inactive status.
- Password reset support from user management tools.

### 2) Dashboard

- Top-level financial snapshot cards.
- Income, cost, and net/benefit visibility.
- Period comparison with trend indicators.
- Quick access to major modules.

### 3) Item Catalog

- Create, edit, and delete items.
- Item code/number generation.
- Unit, price, and VAT amount per item.
- Item reuse in invoice and purchase line items.

### 4) Customer Management

- Full customer CRUD.
- Company/individual type support.
- Tax number, registration name, contact details, website, country, and address fields.
- Customer reuse in sales invoices.

### 5) Vendor Management

- Full vendor CRUD.
- Company/individual type support.
- Tax and contact profile fields.
- Vendor reuse in purchase invoices.

### 6) Sales Invoices

- Create, edit, view, and delete sales invoices.
- Multi-line invoice items with quantity, unit, price, VAT, and line totals.
- Auto subtotal, VAT total, and grand total calculations.
- Invoice numbering with uniqueness checks.
- Invoice payment status tracking.

### 7) Purchase Invoices

- Create, edit, view, and delete purchase invoices.
- Multi-line purchase items with VAT calculations.
- Auto subtotal, VAT total, and grand total calculations.
- Purchase numbering with uniqueness checks.
- Purchase payment status tracking.

### 8) Assets

- Asset visibility derived from item and stock movement logic.
- Value-oriented overview for purchased/sold item impact.

### 9) Expense Management

- Expense CRUD with date, category, amount, VAT, and notes.
- Expense numbering with unique identifiers.
- Optional link to payment methods for settlement tracking.

### 10) Payments Module

- Manage payment methods (cash, bank, card, crypto, etc.).
- Manage currencies including fiat/crypto flags.
- Track manual payment transactions.
- Payment records connected to invoice/purchase/expense flows.

### 11) Reporting

- Sales report views.
- Purchase/cost report views.
- Expense analysis views.
- Profit/loss style summary metrics.
- Debt/receivable/payable visibility.
- Monthly/yearly turnover and trend datasets.
- VAT paid/received and VAT-focused report sections.

### 12) Settings, Backup, and Restore

- Database backup download from UI.
- Database restore upload from UI.
- Automatic safety backup created before restore.

### 13) UI/UX

- Sidebar-driven navigation.
- Tabler/Bootstrap style interface.
- HTML confirmation modals for delete/critical actions.

---

## Tech Stack

- Python 3.10+
- Flask
- Flask-Login
- SQLite (built into Python)
- Jinja2 templates
- Bootstrap/Tabler-style frontend

---

## Project Structure

```text
.
├─ app.py
├─ requirements.txt
├─ invoicing_app/
│  ├─ __init__.py
│  ├─ db.py
│  ├─ schema.sql
│  ├─ templates/
│  └─ static/
└─ instance/
```

---

## System Requirements

- Python **3.10 or newer** (recommended: 3.11+)
- `pip`
- 200 MB free disk (minimum practical baseline)
- Modern browser (Chrome, Edge, Firefox, Safari)
- Git (recommended for cloning and updates)

Python packages are listed in [requirements.txt](requirements.txt).

---

## Installation — macOS

### A) Step-by-step

1. Install prerequisites:

   ```bash
   xcode-select --install
   brew install python
   ```

2. Clone and enter project:

   ```bash
   git clone https://github.com/dataria-e/Celerity-Invoicing-System.git
   cd "Invoicing System"
   ```

3. Create virtual environment:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

4. Install dependencies:

   ```bash
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

5. Start application:

   ```bash
   python app.py
   ```

6. Open browser:

   ```text
   http://127.0.0.1:5000
   ```

### B) One-line macOS install + run

```bash
git clone https://github.com/dataria-e/Celerity-Invoicing-System.git && cd "Invoicing System" && python3 -m venv .venv && source .venv/bin/activate && python -m pip install --upgrade pip && pip install -r requirements.txt && python app.py
```

---

## Installation — Linux

### A) Ubuntu / Debian (step-by-step)

1. Install prerequisites:

   ```bash
   sudo apt update
   sudo apt install -y python3 python3-venv python3-pip git
   ```

2. Clone and enter project:

   ```bash
   git clone https://github.com/dataria-e/Celerity-Invoicing-System.git
   cd "Invoicing System"
   ```

3. Create and activate virtual environment:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

4. Install dependencies:

   ```bash
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

5. Start application:

   ```bash
   python app.py
   ```

6. Open browser:

   ```text
   http://127.0.0.1:5000
   ```

### B) Fedora / RHEL (step-by-step)

```bash
sudo dnf install -y python3 python3-pip git
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python app.py
```

### C) One-line Linux install + run

```bash
git clone https://github.com/dataria-e/Celerity-Invoicing-System.git && cd "Invoicing System" && python3 -m venv .venv && source .venv/bin/activate && python -m pip install --upgrade pip && pip install -r requirements.txt && python app.py
```

---

## Installation — Windows

### A) PowerShell (step-by-step)

1. Install Python 3.10+ from python.org and make sure `py` command is available.
2. Clone and enter project:

   ```powershell
   git clone https://github.com/dataria-e/Celerity-Invoicing-System.git
   Set-Location "Invoicing System"
   ```

3. Create virtual environment:

   ```powershell
   py -m venv .venv
   ```

4. Activate virtual environment:

   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```

5. Install dependencies:

   ```powershell
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

6. Start application:

   ```powershell
   python app.py
   ```

7. Open browser:

   ```text
   http://127.0.0.1:5000
   ```

### B) Command Prompt (CMD)

```bat
git clone https://github.com/dataria-e/Celerity-Invoicing-System.git
cd "Invoicing System"
py -m venv .venv
.venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
python app.py
```

### C) One-line Windows PowerShell install + run

```powershell
git clone https://github.com/dataria-e/Celerity-Invoicing-System.git; Set-Location "Invoicing System"; py -m venv .venv; .\.venv\Scripts\Activate.ps1; python -m pip install --upgrade pip; pip install -r requirements.txt; python app.py
```

---

## First Login

On first run, a default admin account is generated:

- Username: `admin`
- Password: `admin123`

For security, change this password immediately after logging in.

---

## Typical Workflow

1. Configure payment methods/currencies.
2. Add items.
3. Add customers and vendors.
4. Create sales invoices and purchase invoices.
5. Record expenses and payment transactions.
6. Review dashboard and report pages.
7. Download routine backups from Settings.

---

## Data and Backup Notes

- Database file location: `instance/invoicing.sqlite`
- Backup and restore are available from Settings.
- Restore automatically creates a pre-restore safety backup.

---

## Dependency Validation

Direct third-party imports used in code:

- `Flask`
- `Flask-Login`
- `click`
- `Werkzeug`

All required packages are included in [requirements.txt](requirements.txt).

---

## Production Deployment Note

`app.py` currently starts Flask development server (`debug=True`) for local development.
For production use:

- Set a strong secret key through environment/config.
- Use a production WSGI server (for example Gunicorn on Linux).
- Place behind a reverse proxy (for example Nginx).

---

## Troubleshooting

- If venv activation is blocked in PowerShell, run:

  ```powershell
  Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
  ```

- If `python3` is not found on Linux/macOS, use the available command (`python`) and verify with `python --version`.
- If port 5000 is busy, stop the conflicting process or run Flask on another port.

---

## License

This project is distributed under a **custom proprietary license**.

- Free normal/internal use is allowed.
- Modification, copying, redistribution, resale, and commercial/money-making use are restricted unless you have prior written permission from Dataria.
- All rights are reserved by Dataria (dataria.eu).

See the full terms in [LICENSE](LICENSE).
