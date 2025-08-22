# Freight Billing Checklist Tracker

A Streamlit web application for tracking freight billing amounts by carrier and client for invoice preparation.

## Features
- Upload carrier reconciliation files (Excel/CSV)
- Track billable amounts and costs per carrier/client
- Generate billing checklists for invoice preparation
- Cross-reference carrier breakdowns
- Handle files up to 50MB
- Replace/update existing data

## Installation

1. Clone this repository
2. Install requirements: `pip install -r requirements.txt`
3. Run the app: `streamlit run freight_tracker.py`

## Usage
1. Upload carrier reconciliation files with client, cost, and billable amount data
2. Use the billing checklist to prepare client invoices
3. Cross-reference carrier breakdowns against reconciliations
4. Export reports for accounting

## Required Columns
- Client/Customer name
- Cost (what carrier charged you)
- Billable Amount (what to charge client)
