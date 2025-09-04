import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import io
import os
from pathlib import Path
import hashlib

class FreightBillingChecker:
    def __init__(self, data_folder="billing_data"):
        """Initialize the billing checker with Excel file storage"""
        self.data_folder = Path(data_folder)
        self.data_folder.mkdir(exist_ok=True)
        
        # Excel file paths
        self.shipment_data_file = self.data_folder / "shipment_data.xlsx"
        self.billing_checklist_file = self.data_folder / "billing_checklist.xlsx"
        self.upload_log_file = self.data_folder / "upload_log.xlsx"
        
        self.init_excel_files()
    
    def init_excel_files(self):
        """Create Excel files if they don't exist"""
        
        # Initialize shipment data file
        if not self.shipment_data_file.exists():
            shipment_df = pd.DataFrame(columns=[
                'carrier', 'client', 'tracking_number', 'service_type', 
                'cost', 'billable_amount', 'weight', 'zone', 
                'ship_date', 'delivery_date', 'invoice_status', 
                'invoice number', 'invoice date', 'cycle_period', 
                'upload_timestamp', 'file_hash'
            ])
            shipment_df.to_excel(self.shipment_data_file, index=False)
        
        # Initialize billing checklist file
        if not self.billing_checklist_file.exists():
            checklist_df = pd.DataFrame(columns=[
                'client', 'carrier', 'cycle_period', 'shipment_count', 
                'total_cost', 'total_billable', 'profit', 'profit_margin',
                'invoice_status', 'invoice number', 'invoice date', 'notes'
            ])
            checklist_df.to_excel(self.billing_checklist_file, index=False)
        
        # Initialize upload log file
        if not self.upload_log_file.exists():
            log_df = pd.DataFrame(columns=[
                'filename', 'file_hash', 'upload_date', 'records_imported',
                'carrier', 'cycle_period','status', 'deleted_date'
            ])
            log_df.to_excel(self.upload_log_file, index=False)
    
    def get_file_hash(self, file_content):
        """Generate hash for uploaded file to prevent duplicates"""
        return hashlib.md5(file_content).hexdigest()
    
    def load_shipment_data(self):
        """Load shipment data from Excel"""
        try:
            return pd.read_excel(self.shipment_data_file)
        except:
            return pd.DataFrame()
    
    def load_billing_checklist(self):
        """Load billing checklist from Excel"""
        try:
            return pd.read_excel(self.billing_checklist_file)
        except:
            return pd.DataFrame()
    
    def load_upload_log(self):
        """Load upload log from Excel"""
        try:
            return pd.read_excel(self.upload_log_file)
        except:
            return pd.DataFrame()
    
    def save_shipment_data(self, df):
        """Save shipment data to Excel"""
        df.to_excel(self.shipment_data_file, index=False)
    
    def save_billing_checklist(self, df):
        """Save billing checklist to Excel"""
        df.to_excel(self.billing_checklist_file, index=False)
    
    def save_upload_log(self, df):
        """Save upload log to Excel"""
        df.to_excel(self.upload_log_file, index=False)
    
    def check_existing_data(self, carrier_name, cycle_period):
        """Check if data already exists for this carrier/cycle combination"""
        existing_shipments = self.load_shipment_data()
        if existing_shipments.empty:
            return False, 0
        
        existing_data = existing_shipments[
            (existing_shipments['carrier'] == carrier_name) & 
            (existing_shipments['cycle_period'] == cycle_period)
        ]
        
        return not existing_data.empty, len(existing_data)
    
    def remove_existing_data(self, carrier_name, cycle_period):
        """Remove existing data for carrier/cycle before adding new data"""
        # Remove from shipment data
        shipment_data = self.load_shipment_data()
        if not shipment_data.empty:
            updated_shipments = shipment_data[
                ~((shipment_data['carrier'] == carrier_name) & 
                  (shipment_data['cycle_period'] == cycle_period))
            ]
            self.save_shipment_data(updated_shipments)
        
        # Remove from billing checklist
        checklist = self.load_billing_checklist()
        if not checklist.empty:
            updated_checklist = checklist[
                ~((checklist['carrier'] == carrier_name) & 
                  (checklist['cycle_period'] == cycle_period))
            ]
            self.save_billing_checklist(updated_checklist)
        
        return True
    
    def process_carrier_file(self, file, carrier_name, cycle_period, column_mapping=None, replace_existing=False):
        """
        Process uploaded carrier reconciliation file with improved column handling
        """
        try:
            # Check for existing data
            has_existing, existing_count = self.check_existing_data(carrier_name, cycle_period)
        
            if has_existing and not replace_existing:
                return False, f"Data already exists for {carrier_name} - {cycle_period} ({existing_count:,} records). Use 'Replace Existing Data' option to update."
        
            # Check file size
            file_size_mb = len(file.getvalue()) / (1024 * 1024)
        
            # Remove existing data if replacing
            if replace_existing and has_existing:
                self.remove_existing_data(carrier_name, cycle_period)

            # Read file with better error handling
            if file.name.endswith('.xlsx'):
                df = pd.read_excel(file, engine='openpyxl')
            elif file.name.endswith('.csv'):
                if file_size_mb > 10:
                    chunk_list = []
                    chunk_size = 10000
                    for chunk in pd.read_csv(file, chunksize=chunk_size):
                        chunk_list.append(chunk)
                    df = pd.concat(chunk_list, ignore_index=True)
                else:
                    df = pd.read_csv(file)
            else:
                return False, "Unsupported file format. Please use Excel or CSV."

            print(f"Raw file shape: {df.shape}")

            # Clean up DataFrame - remove empty columns and rows
            df = df.dropna(axis=1, how='all')  # Remove completely empty columns
            df = df.dropna(axis=0, how='all')  # Remove completely empty rows
        
            # Remove columns that are mostly empty (>95% null)
            null_threshold = 0.95
            null_percentages = df.isnull().mean()
            cols_to_keep = null_percentages[null_percentages < null_threshold].index
            df = df[cols_to_keep]
        
            print(f"After cleaning: {df.shape}")
        
            # Standardize columns with manual mapping if provided
            if column_mapping:
                df = df.rename(columns=column_mapping)
        
            # Auto-detect columns (your existing logic but more robust)
            standard_columns = {
                'client': [
                    'client', 'customer', 'customer_name', 'account', 'consignee', 
                    'shipper', 'company', 'client_name', 'customer name', 'account name'
                ],
                'tracking_number': [
                    'tracking', 'tracking_number', 'tracking_id', 'awb', 'pro', 
                    'tracking number', 'tracking id', 'shipment id', 'reference'
                ],
                'service_type': [
                    'service', 'service_type', 'service_level', 'service type',
                    'service level', 'shipping service', 'delivery service'
                ],
                'cost': [
                    'cost', 'freight_cost', 'shipping_cost', 'carrier_charge', 
                    'total_cost', 'total cost', 'freight cost', 'shipping cost',
                    'carrier cost', 'transport cost', 'delivery cost'
                ],
                'billable_amount': [
                    'billable', 'billable_amount', 'revenue', 'charge_amount', 
                    'bill_amount', 'invoice_amount', 'billable amount', 'bill amount',
                '   invoice amount', 'charge amount', 'total billable', 'total_billable'
                ],
                'weight': [
                    'weight', 'package_weight', 'total_weight', 'package weight',
                    'total weight', 'shipment weight', 'gross weight'
                ],
                'zone': [
                    'zone', 'delivery_zone', 'shipping_zone', 'delivery zone',
                    'shipping zone', 'service zone'
                ],
                'ship_date': [
                    'date', 'ship_date', 'pickup_date', 'service_date', 'ship date',
                    'pickup date', 'service date', 'shipment date', 'send date'
                ],
                'delivery_date': [
                    'delivery_date', 'delivered_date', 'delivery', 'delivery date',
                    'delivered date', 'arrival date', 'completion date'
                ]
            }

            # Auto-detect columns with better matching
            column_map = {}
            df_columns_lower = {col: col.lower().strip().replace(' ', '').replace('_', '') for col in df.columns}
        
            for standard, variants in standard_columns.items():
                for original_col, clean_col in df_columns_lower.items():
                    clean_variants = [v.lower().replace(' ', '').replace('_', '') for v in variants]
                    if clean_col in clean_variants:
                        column_map[original_col] = standard
                        break

            # Apply column mapping
            df = df.rename(columns=column_map)

            print(f'"Columns after auto-detection: {list(df.columns)}")')

            # Verify required columns exist
            required_cols = ['client', 'cost', 'billable_amount']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                available_cols = list(df.columns)
                return False, f"Missing required columns: {missing_cols}. Available columns: {available_cols}"

            # CREATE STANDARDIZED DATAFRAME
            # Define our exact standard structure (16 columns)
            STANDARD_COLUMNS = [
                'carrier', 'client', 'tracking_number', 'service_type', 
                'cost', 'billable_amount', 'weight', 'zone', 
                'ship_date', 'delivery_date', 'invoice_status', 
                'invoice_number', 'invoice_date', 'cycle_period', 
                'upload_timestamp', 'file_hash'
            ]
        
            # Build standardized DataFrame
            standardized_data = []

            for index, row in df.iterrows():
                standard_row = {
                    'carrier': carrier_name,
                    'client': str(row.get('client', '')).strip(),
                    'tracking_number': str(row.get('tracking_number', '')).strip(),
                    'service_type': str(row.get('service_type', '')).strip(),
                    'cost': pd.to_numeric(row.get('cost', 0), errors='coerce'),
                    'billable_amount': pd.to_numeric(row.get('billable_amount', 0), errors='coerce'),
                    'weight': pd.to_numeric(row.get('weight'), errors='coerce'),
                    'zone': str(row.get('zone', '')).strip(),
                    'ship_date': pd.to_datetime(row.get('ship_date'), errors='coerce'),
                    'delivery_date': pd.to_datetime(row.get('delivery_date'), errors='coerce'),
                    'invoice_status': 'Ready to Bill',
                    'invoice_number': '',
                    'invoice_date': pd.to_datetime(row.get('invoice_date'), errors='coerce'),
                    'cycle_period': cycle_period,
                    'upload_timestamp': datetime.now(),
                    'file_hash': self.get_file_hash(file.getvalue())
                }
                standardized_data.append(standard_row)

            # Create standardized DataFrame with exact column structure
            standardized_df = pd.DataFrame(standardized_data, columns=STANDARD_COLUMNS)
        
            # Remove rows with missing critical data
            initial_count = len(standardized_df)
            standardized_df = standardized_df.dropna(subset=['cost', 'billable_amount'])
            standardized_df = standardized_df[
                (standardized_df['cost'] != 0) | (standardized_df['billable_amount'] != 0)
            ]
            final_count = len(standardized_df)

            if final_count == 0:
                return False, "No valid records found with both costs and billable amount data."

            print(f"Standardised DataFrame: {standardized_df.shape}")

            # Handle existing data with structure enforcement
            existing_shipments = self.load_shipment_data()
        
            if not existing_shipments.empty:
                print(f"Existing data before standardization: {existing_shipments.shape}")
            
                # Force existing data into standard structure
                existing_standardized = pd.DataFrame(columns=STANDARD_COLUMNS)
            
            for col in STANDARD_COLUMNS:
                if col in existing_shipments.columns:
                    existing_standardized[col] = existing_shipments[col]
                else:
                    # Add missing columns with appropriate defaults
                    if col in ['ship_date', 'delivery_date', 'invoice_date']:
                        existing_standardized[col] = pd.NaT
                    elif col in ['cost', 'billable_amount', 'weight']:
                        existing_standardized[col] = 0
                    else:
                        existing_standardized[col] = ''
            
                print(f"Existing data after standardization: {existing_standardized.shape}")
            
                # Safe concatenation with identical structures
                combined_shipments = pd.concat([existing_standardized, standardized_df], ignore_index=True)
            else:
                combined_shipments = standardized_df
        
            print(f"Final combined shape: {combined_shipments.shape}")
        
            # Save the data
            self.save_shipment_data(combined_shipments)
        
            # Update upload log
            upload_log = self.load_upload_log()
        
            new_log_entry = pd.DataFrame([{
                'filename': file.name,
                'file_hash': self.get_file_hash(file.getvalue()),
                'upload_date': datetime.now(),
                'records_imported': final_count,
                'carrier': carrier_name,
                'cycle_period': cycle_period,
                'status': 'Active',
                'deleted_date': None
            }])
        
            # Ensure upload log structure consistency
            if not upload_log.empty:
                if 'status' not in upload_log.columns:
                    upload_log['status'] = 'Active'
                if 'deleted_date' not in upload_log.columns:
                    upload_log['deleted_date'] = None
        
            combined_log = pd.concat([upload_log, new_log_entry], ignore_index=True)
            self.save_upload_log(combined_log)
        
            # Update billing checklist
            self.update_billing_checklist(standardized_df)
        
            message = f"Successfully {'replaced' if replace_existing and has_existing else 'imported'} {final_count:,} shipments for {carrier_name} ({file_size_mb:.1f} MB)"
            if replace_existing and has_existing:
                message += f" (replaced {existing_count:,} existing records)"
            if initial_count != final_count:
                message += f" (removed {initial_count - final_count:,} records with missing data)"
        
            return True, message
        
        except Exception as e:
            import traceback
            traceback.print_exc()
            return False, f"Error processing file: {str(e)}"
        
    def update_billing_checklist(self, new_shipments):
        """Update billing checklist with new shipment data"""
        # Group by client, carrier, and cycle period
        summary = new_shipments.groupby(['client', 'carrier', 'cycle_period']).agg({
            'tracking_number': 'count',
            'cost': 'sum',
            'billable_amount': 'sum'
        }).reset_index()
        
        summary.rename(columns={'tracking_number': 'shipment_count'}, inplace=True)
        summary['total_cost'] = summary['cost']
        summary['total_billable'] = summary['billable_amount']
        summary['profit'] = summary['total_billable'] - summary['total_cost']
        summary['profit_margin'] = (summary['profit'] / summary['total_billable'] * 100).round(2)
        summary['invoice_status'] = 'Ready to Bill'
        summary['invoice number'] = ''
        summary['invoice_date'] = None
        summary['notes'] = ''
        
        # Load existing checklist
        existing_checklist = self.load_billing_checklist()
        
        for _, row in summary.iterrows():
            client = row['client']
            carrier = row['carrier']
            cycle = row['cycle_period']
            
            # Check if entry exists
            mask = ((existing_checklist['client'] == client) & 
                   (existing_checklist['carrier'] == carrier) & 
                   (existing_checklist['cycle_period'] == cycle))
            
            if existing_checklist[mask].empty:
                # Add new entry
                new_entry = pd.DataFrame([row])
                existing_checklist = pd.concat([existing_checklist, new_entry], ignore_index=True)
            else:
                # Update existing entry (add to totals)
                for col in ['shipment_count', 'total_cost', 'total_billable']:
                    existing_checklist.loc[mask, col] = (
                        existing_checklist.loc[mask, col].iloc[0] + row[col]
                    )
                # Recalculate derived fields
                existing_checklist.loc[mask, 'profit'] = (
                    existing_checklist.loc[mask, 'total_billable'] - 
                    existing_checklist.loc[mask, 'total_cost']
                )
                existing_checklist.loc[mask, 'profit_margin'] = (
                    existing_checklist.loc[mask, 'profit'] / 
                    existing_checklist.loc[mask, 'total_billable'] * 100
                ).round(2)
        
        self.save_billing_checklist(existing_checklist)
    
    def get_billing_checklist(self, cycle_period=None, client=None, carrier=None):
        """Get billing checklist for invoice preparation"""
        checklist = self.load_billing_checklist()
        
        if checklist.empty:
            return pd.DataFrame()
        
        # Apply filters
        if cycle_period:
            checklist = checklist[checklist['cycle_period'] == cycle_period]
        if client:
            checklist = checklist[checklist['client'] == client]
        if carrier:
            checklist = checklist[checklist['carrier'] == carrier]
        
        return checklist.sort_values(['cycle_period', 'client', 'carrier'], ascending=[False, True, True])
    
    def get_client_summary(self, cycle_period=None):
        """Get summary by client (combining all carriers)"""
        checklist = self.load_billing_checklist()
        
        if checklist.empty:
            return pd.DataFrame()
        
        if cycle_period:
            checklist = checklist[checklist['cycle_period'] == cycle_period]
        
        # Group by client and cycle
        client_summary = checklist.groupby(['client', 'cycle_period']).agg({
            'shipment_count': 'sum',
            'total_cost': 'sum',
            'total_billable': 'sum',
            'invoice_status': lambda x: 'Billed' if all(x == 'Billed') else 'Ready to Bill'
        }).reset_index()
        
        client_summary['profit'] = client_summary['total_billable'] - client_summary['total_cost']
        client_summary['profit_margin'] = (
            client_summary['profit'] / client_summary['total_billable'] * 100
        ).round(2)
        
        return client_summary.sort_values(['cycle_period', 'total_billable'], ascending=[False, False])
    
    def get_carrier_breakdown(self, client, cycle_period):
        """Get carrier breakdown for specific client/cycle"""
        checklist = self.load_billing_checklist()
        
        if checklist.empty:
            return pd.DataFrame()
        
        breakdown = checklist[
            (checklist['client'] == client) & 
            (checklist['cycle_period'] == cycle_period)
        ]
        
        return breakdown.sort_values('total_billable', ascending=False)
    
    def get_shipment_details(self, client=None, carrier=None, cycle_period=None):
        """Get detailed shipment data for line items"""
        shipment_data = self.load_shipment_data()
        
        if shipment_data.empty:
            return pd.DataFrame()
        
        # Apply filters
        if client:
            shipment_data = shipment_data[shipment_data['client'] == client]
        if carrier:
            shipment_data = shipment_data[shipment_data['carrier'] == carrier]
        if cycle_period:
            shipment_data = shipment_data[shipment_data['cycle_period'] == cycle_period]
        
        return shipment_data.sort_values(['client', 'carrier', 'ship_date'])
    
    def mark_client_billed(self, client, cycle_period, invoice_number, invoice_date=None, notes=""):
        """Mark entire client as billed (all carriers for that cycle)"""
        if invoice_date is None:
            invoice_date = datetime.now().date()
        
        # Update billing checklist
        checklist = self.load_billing_checklist()
        mask = (checklist['client'] == client) & (checklist['cycle_period'] == cycle_period)
        
        if not checklist[mask].empty:
            checklist.loc[mask, 'invoice_status'] = 'Billed'
            checklist.loc[mask, 'invoice_number'] = invoice_number
            checklist.loc[mask, 'invoice_date'] = invoice_date
            checklist.loc[mask, 'notes'] = notes
            
            self.save_billing_checklist(checklist)
        
        # Update shipment data
        shipment_data = self.load_shipment_data()
        mask = (shipment_data['client'] == client) & (shipment_data['cycle_period'] == cycle_period)
        
        if not shipment_data[mask].empty:
            shipment_data.loc[mask, 'invoice_status'] = 'Billed'
            shipment_data.loc[mask, 'invoice_number'] = invoice_number
            shipment_data.loc[mask, 'invoice_date'] = invoice_date
            
            self.save_shipment_data(shipment_data)
        
        return True
    
    def export_billing_data(self, cycle_period=None, client=None):
        """Export billing data for invoice preparation"""
        client_summary = self.get_client_summary(cycle_period)
        detailed_checklist = self.get_billing_checklist(cycle_period, client)
        shipment_details = self.get_shipment_details(client, cycle_period=cycle_period)
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Client summary (main invoicing reference)
            client_summary.to_excel(writer, sheet_name='Client_Invoice_Totals', index=False)
            
            # Detailed breakdown by carrier
            detailed_checklist.to_excel(writer, sheet_name='Carrier_Breakdown', index=False)
            
            # Shipment line items
            if not shipment_details.empty:
                # Select relevant columns for invoicing
                invoice_details = shipment_details[[
                    'client', 'carrier', 'tracking_number', 'service_type', 
                    'ship_date', 'cost', 'billable_amount', 'cycle_period'
                ]].copy()
                invoice_details.to_excel(writer, sheet_name='Shipment_Line_Items', index=False)
            
            # Summary totals
            if not client_summary.empty:
                totals = pd.DataFrame([
                    ['Total Clients', len(client_summary)],
                    ['Total Shipments', client_summary['shipment_count'].sum()],
                    ['Total Cost', client_summary['total_cost'].sum()],
                    ['Total Billable', client_summary['total_billable'].sum()],
                    ['Total Profit', client_summary['profit'].sum()],
                    ['Average Margin %', client_summary['profit_margin'].mean()]
                ], columns=['Metric', 'Value'])
                totals.to_excel(writer, sheet_name='Summary_Totals', index=False)
        
        return output.getvalue()
    # Add these methods to your FreightBillingChecker class:

    def delete_carrier_data(self, carrier_name, cycle_period):
        """Delete all data for a specific carrier/cycle combination"""
        try:
            # Remove from shipment data
            shipment_data = self.load_shipment_data()
            if not shipment_data.empty:
                updated_shipments = shipment_data[
                    ~((shipment_data['carrier'] == carrier_name) & 
                      (shipment_data['cycle_period'] == cycle_period))
                ]
                self.save_shipment_data(updated_shipments)
            
            # Remove from billing checklist
            checklist = self.load_billing_checklist()
            if not checklist.empty:
                updated_checklist = checklist[
                    ~((checklist['carrier'] == carrier_name) & 
                      (checklist['cycle_period'] == cycle_period))
                ]
                self.save_billing_checklist(updated_checklist)
            
            # Update upload log (mark as deleted but keep for audit trail)
            upload_log = self.load_upload_log()
            if not upload_log.empty:
                mask = ((upload_log['carrier'] == carrier_name) & 
                       (upload_log['cycle_period'] == cycle_period))
                # Add status column if it doesn't exist
                if 'status' not in upload_log.columns:
                    upload_log['status'] = 'Active'
                if 'deleted_date' not in upload_log.columns:
                    upload_log['deleted_date'] = None
                    
                upload_log.loc[mask, 'status'] = 'Deleted'
                upload_log.loc[mask, 'deleted_date'] = datetime.now()
                self.save_upload_log(upload_log)
            
            return True, f"Successfully deleted data for {carrier_name} - {cycle_period}"
            
        except Exception as e:
            return False, f"Error deleting data: {str(e)}"

    def delete_client_cycle(self, client_name, cycle_period):
        """Delete all data for a client in a specific cycle (all carriers)"""
        try:
            # Remove from shipment data
            shipment_data = self.load_shipment_data()
            if not shipment_data.empty:
                updated_shipments = shipment_data[
                    ~((shipment_data['client'] == client_name) & 
                      (shipment_data['cycle_period'] == cycle_period))
                ]
                self.save_shipment_data(updated_shipments)
            
            # Remove from billing checklist
            checklist = self.load_billing_checklist()
            if not checklist.empty:
                updated_checklist = checklist[
                    ~((checklist['client'] == client_name) & 
                      (checklist['cycle_period'] == cycle_period))
                ]
                self.save_billing_checklist(updated_checklist)
            
            return True, f"Successfully deleted all data for {client_name} - {cycle_period}"
            
        except Exception as e:
            return False, f"Error deleting client data: {str(e)}"

    def get_data_summary(self):
        """Get summary of all data for management view"""
        shipment_data = self.load_shipment_data()
        
        if shipment_data.empty:
            return pd.DataFrame()
        
        # Group by carrier, client, and cycle
        summary = shipment_data.groupby(['carrier', 'client', 'cycle_period']).agg({
            'tracking_number': 'count',
            'cost': 'sum',
            'billable_amount': 'sum',
            'upload_timestamp': 'min'  # First upload timestamp
        }).reset_index()
        
        summary.rename(columns={'tracking_number': 'shipment_count'}, inplace=True)
        summary['profit'] = summary['billable_amount'] - summary['cost']
        summary['upload_date'] = pd.to_datetime(summary['upload_timestamp']).dt.date
        
        return summary.sort_values(['cycle_period', 'client', 'carrier'], ascending=[False, True, True])

    def clear_all_data(self, confirmation_code):
        """Clear all data with confirmation code"""
        if confirmation_code != "DELETE_ALL_BILLING_DATA":
            return False, "Invalid confirmation code"
        
        try:
            # Clear all files by recreating them
            self.init_excel_files()
            return True, "All billing data has been cleared"
        except Exception as e:
            return False, f"Error clearing data: {str(e)}"

    def export_data_backup(self):
        """Export complete backup of all data"""
        try:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # All data
                shipment_data = self.load_shipment_data()
                billing_checklist = self.load_billing_checklist()
                upload_log = self.load_upload_log()
                
                if not shipment_data.empty:
                    shipment_data.to_excel(writer, sheet_name='All_Shipments', index=False)
                if not billing_checklist.empty:
                    billing_checklist.to_excel(writer, sheet_name='Billing_Checklist', index=False)
                if not upload_log.empty:
                    upload_log.to_excel(writer, sheet_name='Upload_History', index=False)
            
            return output.getvalue()
        except Exception as e:
            return None

# Streamlit Web Interface
def main():
    st.set_page_config(
        page_title="Freight Billing Checklist",
        page_icon="📋",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Configure for large files
    st.markdown("""
    <style>
    .uploadedFile {
        max-height: 200px;
        overflow-y: auto;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.title("📋 Freight Billing Checklist Tracker")
    st.markdown("*Track billable amounts by carrier and client for invoice preparation*")
    st.markdown("---")
    
    # Initialize tracker
    if 'tracker' not in st.session_state:
        st.session_state.tracker = FreightBillingChecker()
    
    tracker = st.session_state.tracker
    
    # Sidebar for navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a page:",
        ["📊 Billing Dashboard", "📤 Upload Carrier Data", "📋 Client Billing Checklist", "🚚 Carrier Breakdown", "📈 Reports", "🗂️ Data Management","⚙️ Settings"]
    )
    
    if page == "📊 Billing Dashboard":
        show_billing_dashboard(tracker)
    elif page == "📤 Upload Carrier Data":
        show_upload_page(tracker)
    elif page == "📋 Client Billing Checklist":
        show_client_checklist(tracker)
    elif page == "🚚 Carrier Breakdown":
        show_carrier_breakdown(tracker)
    elif page == "📈 Reports":
        show_reports(tracker)
    elif page == "🗂️ Data Management":
        show_data_management(tracker)
    elif page == "⚙️ Settings":
        show_settings(tracker)

def show_billing_dashboard(tracker):
    """Show billing dashboard"""
    st.header("📊 Billing Dashboard")
    
    # Get data
    client_summary = tracker.get_client_summary()
    
    if client_summary.empty:
        st.info("📋 No billing data available. Upload carrier reconciliation files to start tracking.")
        return
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_billable = client_summary['total_billable'].sum()
        st.metric("💰 Total to Bill", f"${total_billable:,.2f}")
    
    with col2:
        total_cost = client_summary['total_cost'].sum()
        st.metric("💸 Total Cost", f"${total_cost:,.2f}")
    
    with col3:
        total_profit = client_summary['profit'].sum()
        st.metric("📈 Total Profit", f"${total_profit:,.2f}")
    
    with col4:
        ready_to_bill = len(client_summary[client_summary['invoice_status'] == 'Ready to Bill'])
        st.metric("📋 Ready to Bill", ready_to_bill)
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Billing status pie chart
        status_counts = client_summary['invoice_status'].value_counts()
        fig = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="📋 Billing Status"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top clients by billable amount
        top_clients = client_summary.groupby('client')['total_billable'].sum().sort_values(ascending=False).head(10)
        fig = px.bar(
            x=top_clients.index,
            y=top_clients.values,
            title="💰 Top Clients by Billable Amount"
        )
        fig.update_layout(xaxis_title="Client", yaxis_title="Billable Amount ($)")
        st.plotly_chart(fig, use_container_width=True)
    
    # Recent billing checklist
    st.subheader("📋 Recent Client Billing Summary")
    
    # Show latest cycle data
    if not client_summary.empty:
        latest_cycle = client_summary['cycle_period'].max()
        latest_data = client_summary[client_summary['cycle_period'] == latest_cycle]
        
        st.write(f"**Latest Cycle: {latest_cycle}**")
        
        # Format for display
        display_data = latest_data.copy()
        for col in ['total_cost', 'total_billable', 'profit']:
            display_data[col] = display_data[col].apply(lambda x: f"${x:,.2f}")
        display_data['profit_margin'] = display_data['profit_margin'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(display_data, use_container_width=True, hide_index=True)

def show_upload_page(tracker):
    """Show carrier data upload page"""
    st.header("📤 Upload Carrier Reconciliation Data")
    st.markdown("*Upload carrier files containing cost and billable amounts*")
    
    # File size warning
    st.info("💡 **Large File Support:** Files up to 50MB supported. Files over 10MB may take 1-2 minutes to process.")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📁 Select Carrier File")
        uploaded_file = st.file_uploader(
            "Choose carrier reconciliation file",
            type=['xlsx', 'csv'],
            help="Upload files with client, cost, and billable amount data. Max size: 50MB"
        )
        
        if uploaded_file:
            # Show file info
            file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
            if file_size_mb > 30:
                st.warning(f"⚠️ Large file detected: {file_size_mb:.1f} MB. Processing may take 2-3 minutes.")
            elif file_size_mb > 10:
                st.info(f"📊 File size: {file_size_mb:.1f} MB. Processing may take 1-2 minutes.")
            else:
                st.success(f"✅ File selected: {uploaded_file.name} ({file_size_mb:.1f} MB)")
            
            # Show file preview for smaller files
            if file_size_mb < 20:  # Only preview smaller files
                try:
                    if uploaded_file.name.endswith('.xlsx'):
                        preview_df = pd.read_excel(uploaded_file, nrows=5)
                    else:
                        preview_df = pd.read_csv(uploaded_file, nrows=5)
                    
                    st.subheader("👀 File Preview")
                    st.dataframe(preview_df, use_container_width=True)
                    
                    st.subheader("📋 Detected Columns")
                    st.write(f"**Columns found:** {', '.join(preview_df.columns)}")
                    
                except Exception as e:
                    st.error(f"Error reading file: {e}")
            else:
                st.info("📋 File preview disabled for large files to improve performance.")
    
    with col2:
        st.subheader("📝 Upload Details")
        carrier_name = st.text_input(
            "🚚 Carrier Name",
            placeholder="e.g., FedEx, UPS, DHL"
        )
        
        cycle_period = st.text_input(
            "📅 Billing Cycle",
            placeholder="e.g., 2024-08, Week 32"
        )
        
        # Check for existing data
        if carrier_name and cycle_period:
            has_existing, existing_count = tracker.check_existing_data(carrier_name, cycle_period)
            if has_existing:
                st.warning(f"⚠️ Existing data found: {existing_count:,} records for {carrier_name} - {cycle_period}")
                replace_existing = st.checkbox(
                    "🔄 Replace existing data", 
                    help="Check this to replace the existing data with the new file"
                )
            else:
                replace_existing = False
                st.info("✅ No existing data for this carrier/cycle combination")
        else:
            replace_existing = False
        
        st.markdown("**📊 Required Columns:**")
        st.markdown("""
        - **Client/Customer** name *(required)*
        - **Cost** *(what carrier charged you)*
        - **Billable Amount** *(what to charge client)*
        
        **Optional Columns:**
        - Tracking number
        - Service type  
        - Weight, Zone
        - Ship/delivery dates
        """)
        
        if st.button("🚀 Process File", type="primary"):
            if uploaded_file and carrier_name and cycle_period:
                # Show processing message based on file size
                file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
                if file_size_mb > 20:
                    processing_msg = f"⏳ Processing large file ({file_size_mb:.1f} MB)... This may take 2-3 minutes."
                elif file_size_mb > 10:
                    processing_msg = f"⏳ Processing file ({file_size_mb:.1f} MB)... This may take 1-2 minutes."
                else:
                    processing_msg = "⏳ Processing file..."
                
                if replace_existing:
                    processing_msg = processing_msg.replace("Processing", "Replacing existing data and processing")
                
                with st.spinner(processing_msg):
                    # Add progress bar for large files
                    if file_size_mb > 10:
                        progress_bar = st.progress(0)
                        progress_bar.progress(25)  # File reading
                    
                    success, message = tracker.process_carrier_file(
                        uploaded_file, carrier_name, cycle_period, replace_existing=replace_existing
                    )
                    
                    if file_size_mb > 10:
                        progress_bar.progress(100)  # Complete
                
                if success:
                    if replace_existing:
                        st.success(f"🔄 {message}")
                    else:
                        st.success(f"✅ {message}")
                    st.balloons()
                    st.rerun()
                else:
                    st.error(f"❌ {message}")
            else:
                st.error("❌ Please provide all required information")
    
    # Upload history
    st.markdown("---")
    st.subheader("📝 Recent Uploads")
    
    upload_history = tracker.load_upload_log()
    if not upload_history.empty:
        recent_uploads = upload_history.sort_values('upload_date', ascending=False).head(10)
        
        # Format upload date
        display_uploads = recent_uploads.copy()
        display_uploads['upload_date'] = pd.to_datetime(display_uploads['upload_date']).dt.strftime('%Y-%m-%d %H:%M')
        
        st.dataframe(
            display_uploads[['filename', 'carrier', 'cycle_period', 'records_imported', 'upload_date']], 
            use_container_width=True, 
            hide_index=True
        )
    else:
        st.info("📋 No upload history yet")

def show_client_checklist(tracker):
    """Show client billing checklist"""
    st.header("📋 Client Billing Checklist")
    st.markdown("*Cross-reference these totals when building client invoices*")
    
    # Get client summary
    client_summary = tracker.get_client_summary()
    
    if client_summary.empty:
        st.info("📋 No billing data available")
        return
    
    # Filter section
    col1, col2 = st.columns(2)
    
    with col1:
        cycles = ['All'] + list(client_summary['cycle_period'].unique())
        selected_cycle = st.selectbox("📅 Billing Cycle", cycles)
    
    with col2:
        status_filter = st.selectbox(
            "📊 Status",
            ['All', 'Ready to Bill', 'Billed']
        )
    
    # Apply filters
    filtered_summary = client_summary.copy()
    if selected_cycle != 'All':
        filtered_summary = filtered_summary[filtered_summary['cycle_period'] == selected_cycle]
    if status_filter != 'All':
        filtered_summary = filtered_summary[filtered_summary['invoice_status'] == status_filter]
    
    # Display client checklist
    if not filtered_summary.empty:
        st.subheader("💰 Client Invoice Totals")
        
        # Format for display
        display_data = filtered_summary.copy()
        for col in ['total_cost', 'total_billable', 'profit']:
            display_data[col] = display_data[col].apply(lambda x: f"${x:,.2f}")
        display_data['profit_margin'] = display_data['profit_margin'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(display_data, use_container_width=True, hide_index=True)
        
        # Summary totals
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📦 Total Shipments", filtered_summary['shipment_count'].sum())
        with col2:
            st.metric("💰 Total to Bill", f"${filtered_summary['total_billable'].sum():,.2f}")
        with col3:
            st.metric("📈 Average Margin", f"{filtered_summary['profit_margin'].mean():.1f}%")
    
    # Mark as billed section
    st.markdown("---")
    st.subheader("✅ Mark Client as Billed")
    
    with st.form("mark_billed"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            ready_clients = client_summary[client_summary['invoice_status'] == 'Ready to Bill']
            if not ready_clients.empty:
                client_options = ready_clients['client'].unique()
                bill_client = st.selectbox("👤 Client", client_options)
            else:
                st.info("No clients ready to bill")
                bill_client = None
        
        with col2:
            if bill_client:
                cycle_options = ready_clients[ready_clients['client'] == bill_client]['cycle_period'].unique()
                bill_cycle = st.selectbox("📅 Cycle", cycle_options)
                
                # Show totals for this client/cycle
                client_total = ready_clients[
                    (ready_clients['client'] == bill_client) & 
                    (ready_clients['cycle_period'] == bill_cycle)
                ]
                if not client_total.empty:
                    total_amount = client_total['total_billable'].iloc[0]
                    st.info(f"💰 Total: ${total_amount:,.2f}")
            else:
                bill_cycle = None
        
        with col3:
            invoice_number = st.text_input("📄 Invoice Number")
        
        notes = st.text_area("📝 Notes", placeholder="Optional notes...")
        
        if st.form_submit_button("✅ Mark as Billed"):
            if bill_client and bill_cycle and invoice_number:
                tracker.mark_client_billed(bill_client, bill_cycle, invoice_number, notes=notes)
                st.success("✅ Client marked as billed!")
                st.rerun()
            else:
                st.error("❌ Please fill in all required fields")

def show_carrier_breakdown(tracker):
    """Show carrier breakdown for cross-referencing"""
    st.header("🚚 Carrier Breakdown by Client")
    st.markdown("*Detailed breakdown by carrier for cross-referencing your reconciliations*")
    
    # Get detailed checklist
    detailed_checklist = tracker.get_billing_checklist()
    
    if detailed_checklist.empty:
        st.info("📋 No data available")
        return
    
    # Filter section
    col1, col2, col3 = st.columns(3)
    
    with col1:
        cycles = ['All'] + list(detailed_checklist['cycle_period'].unique())
        selected_cycle = st.selectbox("📅 Cycle", cycles)
    
    with col2:
        clients = ['All'] + list(detailed_checklist['client'].unique())
        selected_client = st.selectbox("👤 Client", clients)
    
    with col3:
        carriers = ['All'] + list(detailed_checklist['carrier'].unique())
        selected_carrier = st.selectbox("🚚 Carrier", carriers)
    
    # Apply filters
    filtered_data = detailed_checklist.copy()
    if selected_cycle != 'All':
        filtered_data = filtered_data[filtered_data['cycle_period'] == selected_cycle]
    if selected_client != 'All':
        filtered_data = filtered_data[filtered_data['client'] == selected_client]
    if selected_carrier != 'All':
        filtered_data = filtered_data[filtered_data['carrier'] == selected_carrier]
    
    # Display breakdown
    if not filtered_data.empty:
        st.subheader("📊 Carrier-Client Breakdown")
        
        # Format for display
        display_data = filtered_data.copy()
        for col in ['total_cost', 'total_billable', 'profit']:
            display_data[col] = display_data[col].apply(lambda x: f"${x:,.2f}")
        display_data['profit_margin'] = display_data['profit_margin'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(display_data, use_container_width=True, hide_index=True)
        
        # Add pie chart for shipments by carrier
        st.markdown("---")
        
        # Create two columns for charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Pie chart showing shipments by carrier
            carrier_shipments = filtered_data.groupby('carrier')['shipment_count'].sum().reset_index()
            if len(carrier_shipments) > 1:
                fig_pie = px.pie(
                    carrier_shipments,
                    values='shipment_count',
                    names='carrier',
                    title="📦 Shipments by Carrier",
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig_pie.update_traces(
                    textposition='inside', 
                    textinfo='percent+label',
                    hovertemplate='<b>%{label}</b><br>Shipments: %{value:,}<br>Percentage: %{percent}<extra></extra>'
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("📊 Pie chart requires multiple carriers to display")
        
        with col2:
            # Bar chart showing billable amounts by carrier
            carrier_billable = filtered_data.groupby('carrier')['total_billable'].sum().reset_index()
            if not carrier_billable.empty:
                fig_bar = px.bar(
                    carrier_billable,
                    x='carrier',
                    y='total_billable',
                    title="💰 Billable Amount by Carrier",
                    color='total_billable',
                    color_continuous_scale='Blues'
                )
                fig_bar.update_layout(
                    xaxis_title="Carrier",
                    yaxis_title="Billable Amount ($)",
                    showlegend=False
                )
                fig_bar.update_traces(
                    hovertemplate='<b>%{x}</b><br>Billable: $%{y:,.2f}<extra></extra>'
                )
                st.plotly_chart(fig_bar, use_container_width=True)
        
        # Summary by carrier
        if selected_client != 'All' and selected_cycle != 'All':
            st.markdown("---")
            st.subheader(f"📋 Summary for {selected_client} - {selected_cycle}")
            
            carrier_totals = filtered_data.groupby('carrier').agg({
                'shipment_count': 'sum',
                'total_cost': 'sum',
                'total_billable': 'sum'
            }).reset_index()
            carrier_totals['profit'] = carrier_totals['total_billable'] - carrier_totals['total_cost']
            
            # Format totals
            display_totals = carrier_totals.copy()
            for col in ['total_cost', 'total_billable', 'profit']:
                display_totals[col] = display_totals[col].apply(lambda x: f"${x:,.2f}")
            
            st.dataframe(display_totals, use_container_width=True, hide_index=True)
    
    # Shipment details section
    if selected_client != 'All' and selected_cycle != 'All':
        st.markdown("---")
        st.subheader("📦 Shipment Details")
        
        if selected_carrier != 'All':
            shipment_details = tracker.get_shipment_details(
                client=selected_client, 
                carrier=selected_carrier, 
                cycle_period=selected_cycle
            )
        else:
            shipment_details = tracker.get_shipment_details(
                client=selected_client, 
                cycle_period=selected_cycle
            )
        
        if not shipment_details.empty:
            # Select relevant columns
            detail_cols = [
                'carrier', 'tracking_number', 'service_type', 'ship_date',
                'cost', 'billable_amount'
            ]
            display_details = shipment_details[detail_cols].copy()
            
            # Format currency columns
            display_details['cost'] = display_details['cost'].apply(lambda x: f"${x:,.2f}")
            display_details['billable_amount'] = display_details['billable_amount'].apply(lambda x: f"${x:,.2f}")
            
            st.dataframe(display_details, use_container_width=True, hide_index=True)

def show_reports(tracker):
    """Show reports page"""
    st.header("📈 Billing Reports")
    
    report_type = st.selectbox(
        "📊 Select Report",
        ["Billing Export", "Cycle Summary", "Carrier Performance"]
    )
    
    if report_type == "Billing Export":
        st.subheader("📤 Export Billing Data")
        
        client_summary = tracker.get_client_summary()
        if not client_summary.empty:
            cycles = ['All'] + list(client_summary['cycle_period'].unique())
            export_cycle = st.selectbox("📅 Export Cycle", cycles)
            
            if st.button("📥 Generate Excel Export"):
                cycle_filter = None if export_cycle == 'All' else export_cycle
                excel_data = tracker.export_billing_data(cycle_period=cycle_filter)
                
                filename = f"billing_checklist_{export_cycle if export_cycle != 'All' else 'all'}_{datetime.now().strftime('%Y%m%d')}.xlsx"
                
                st.download_button(
                    "📁 Download Billing Report",
                    excel_data,
                    filename,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.success("✅ Report generated!")
    
    elif report_type == "Cycle Summary":
        st.subheader("📅 Billing Cycle Summary")
        
        client_summary = tracker.get_client_summary()
        if not client_summary.empty:
            # Summary by cycle
            cycle_summary = client_summary.groupby('cycle_period').agg({
                'shipment_count': 'sum',
                'total_cost': 'sum',
                'total_billable': 'sum',
                'profit': 'sum'
            }).reset_index()
            cycle_summary['profit_margin'] = (
                cycle_summary['profit'] / cycle_summary['total_billable'] * 100
            ).round(2)
            
            # Format for display
            display_summary = cycle_summary.copy()
            for col in ['total_cost', 'total_billable', 'profit']:
                display_summary[col] = display_summary[col].apply(lambda x: f"${x:,.2f}")
            display_summary['profit_margin'] = display_summary['profit_margin'].apply(lambda x: f"{x:.1f}%")
            
            st.dataframe(display_summary, use_container_width=True, hide_index=True)
    
    elif report_type == "Carrier Performance":
        st.subheader("🚚 Carrier Performance Analysis")
        
        detailed_checklist = tracker.get_billing_checklist()
        if not detailed_checklist.empty:
            # Performance by carrier
            carrier_performance = detailed_checklist.groupby('carrier').agg({
                'shipment_count': 'sum',
                'total_cost': 'sum',
                'total_billable': 'sum',
                'profit': 'sum'
            }).reset_index()
            carrier_performance['profit_margin'] = (
                carrier_performance['profit'] / carrier_performance['total_billable'] * 100
            ).round(2)
            
            st.dataframe(carrier_performance, use_container_width=True, hide_index=True)
            
            # Chart
            fig = px.bar(
                carrier_performance, 
                x='carrier', 
                y='total_billable',
                title="Billable Amount by Carrier"
            )
            st.plotly_chart(fig, use_container_width=True)

def show_settings(tracker):
    """Show settings page"""
    st.header("⚙️ Settings")
    
    st.subheader("📁 Data Files")
    st.write(f"**Data Location:** {tracker.data_folder}")
    
    # File status
    files = [
        ("📦 Shipment Data", tracker.shipment_data_file),
        ("📋 Billing Checklist", tracker.billing_checklist_file),
        ("📝 Upload Log", tracker.upload_log_file)
    ]
    
    for name, path in files:
        col1, col2 = st.columns([1, 3])
        with col1:
            if path.exists():
                st.success(f"✅ {name}")
                # Show record count
                try:
                    if 'shipment' in str(path):
                        df = tracker.load_shipment_data()
                    elif 'checklist' in str(path):
                        df = tracker.load_billing_checklist()
                    else:
                        df = tracker.load_upload_log()
                    st.caption(f"📊 {len(df)} records")
                except:
                    st.caption("📊 Error reading")
            else:
                st.error(f"❌ {name}")
        with col2:
            st.code(str(path))
    
    # Help section
    st.markdown("---")
    st.subheader("❓ Column Detection Help")
    
    with st.expander("🔧 Supported Column Names"):
        st.markdown("""
        **The system automatically detects these column variations:**
        
        - **Client:** `client`, `customer`, `customer_name`, `account`, `consignee`, `shipper`, `company`, `client_name`, `customer name`, `account name`
        - **Cost:** `cost`, `freight_cost`, `shipping_cost`, `carrier_charge`, `total_cost`, `total cost`, `freight cost`, `shipping cost`, `carrier cost`, `transport cost`, `delivery cost`
        - **Billable Amount:** `billable`, `billable_amount`, `revenue`, `charge_amount`, `bill_amount`, `invoice_amount`, `billable amount`, `bill amount`, `invoice amount`, `charge amount`, `total billable`, `total_billable`
        - **Tracking:** `tracking`, `tracking_number`, `tracking_id`, `awb`, `pro`, `tracking number`, `tracking id`, `shipment id`, `reference`
        - **Service:** `service`, `service_type`, `service_level`, `service type`, `service level`, `shipping service`, `delivery service`
        - **Weight:** `weight`, `package_weight`, `total_weight`, `package weight`, `total weight`, `shipment weight`, `gross weight`
        - **Zone:** `zone`, `delivery_zone`, `shipping_zone`, `delivery zone`, `shipping zone`, `service zone`
        - **Dates:** `date`, `ship_date`, `pickup_date`, `service_date`, `ship date`, `pickup date`, `service date`, `shipment date`, `send date`, `delivery_date`, `delivered_date`, `delivery`, `delivery date`, `delivered date`, `arrival date`, `completion date`
        """)

def show_data_management(tracker):
    """Show data management page for deleting/managing uploaded files"""
    st.header("🗂️ Data Management")
    st.markdown("*Manage uploaded carrier files and billing data*")
    
    # Get data summary
    data_summary = tracker.get_data_summary()
    
    if data_summary.empty:
        st.info("📋 No data to manage")
        return
    
    # Tabs for different management options
    tab1, tab2, tab3, tab4 = st.tabs(["🗑️ Delete Data", "📊 Data Overview", "💾 Backup", "⚠️ Reset All"])
    
    with tab1:
        st.subheader("🗑️ Delete Uploaded Data")
        st.warning("⚠️ **Warning:** Deleting data cannot be undone. Consider backing up first.")
        
        # Option 1: Delete by carrier/cycle
        st.markdown("### Delete by Carrier & Cycle")
        
        with st.form("delete_carrier_cycle"):
            col1, col2 = st.columns(2)
            
            with col1:
                carriers = list(data_summary['carrier'].unique())
                delete_carrier = st.selectbox("🚚 Select Carrier", carriers)
            
            with col2:
                if delete_carrier:
                    cycles = data_summary[data_summary['carrier'] == delete_carrier]['cycle_period'].unique()
                    delete_cycle = st.selectbox("📅 Select Cycle", cycles)
                else:
                    delete_cycle = None
            
            if delete_carrier and delete_cycle:
                # Show what will be deleted
                preview_data = data_summary[
                    (data_summary['carrier'] == delete_carrier) & 
                    (data_summary['cycle_period'] == delete_cycle)
                ]
                
                if not preview_data.empty:
                    st.markdown("**📋 Data to be deleted:**")
                    display_preview = preview_data.copy()
                    display_preview['cost'] = display_preview['cost'].apply(lambda x: f"${x:,.2f}")
                    display_preview['billable_amount'] = display_preview['billable_amount'].apply(lambda x: f"${x:,.2f}")
                    display_preview['profit'] = display_preview['profit'].apply(lambda x: f"${x:,.2f}")
                    
                    st.dataframe(display_preview[['client', 'shipment_count', 'cost', 'billable_amount', 'profit', 'upload_date']], 
                               use_container_width=True, hide_index=True)
                    
                    # Confirmation
                    confirm_delete = st.checkbox(f"✅ I confirm deletion of {delete_carrier} - {delete_cycle} data")
                    
                    if st.form_submit_button("🗑️ Delete Data", type="secondary"):
                        if confirm_delete:
                            success, message = tracker.delete_carrier_data(delete_carrier, delete_cycle)
                            if success:
                                st.success(f"✅ {message}")
                                st.rerun()
                            else:
                                st.error(f"❌ {message}")
                        else:
                            st.error("❌ Please confirm deletion")
        
        # Option 2: Delete by client/cycle (all carriers)
        st.markdown("---")
        st.markdown("### Delete by Client & Cycle (All Carriers)")
        
        with st.form("delete_client_cycle"):
            col1, col2 = st.columns(2)
            
            with col1:
                clients = list(data_summary['client'].unique())
                delete_client = st.selectbox("👤 Select Client", clients)
            
            with col2:
                if delete_client:
                    cycles = data_summary[data_summary['client'] == delete_client]['cycle_period'].unique()
                    delete_cycle_client = st.selectbox("📅 Select Cycle", cycles, key="client_cycle")
                else:
                    delete_cycle_client = None
            
            if delete_client and delete_cycle_client:
                # Show what will be deleted
                preview_data = data_summary[
                    (data_summary['client'] == delete_client) & 
                    (data_summary['cycle_period'] == delete_cycle_client)
                ]
                
                if not preview_data.empty:
                    st.markdown("**📋 Data to be deleted (all carriers):**")
                    display_preview = preview_data.copy()
                    display_preview['cost'] = display_preview['cost'].apply(lambda x: f"${x:,.2f}")
                    display_preview['billable_amount'] = display_preview['billable_amount'].apply(lambda x: f"${x:,.2f}")
                    display_preview['profit'] = display_preview['profit'].apply(lambda x: f"${x:,.2f}")
                    
                    st.dataframe(display_preview[['carrier', 'shipment_count', 'cost', 'billable_amount', 'profit', 'upload_date']], 
                               use_container_width=True, hide_index=True)
                    
                    total_shipments = preview_data['shipment_count'].sum()
                    total_billable = preview_data['billable_amount'].sum()
                    st.info(f"📦 Total: {total_shipments:,} shipments, ${total_billable:,.2f} billable")
                    
                    # Confirmation
                    confirm_delete_client = st.checkbox(f"✅ I confirm deletion of ALL {delete_client} data for {delete_cycle_client}")
                    
                    if st.form_submit_button("🗑️ Delete Client Data", type="secondary"):
                        if confirm_delete_client:
                            success, message = tracker.delete_client_cycle(delete_client, delete_cycle_client)
                            if success:
                                st.success(f"✅ {message}")
                                st.rerun()
                            else:
                                st.error(f"❌ {message}")
                        else:
                            st.error("❌ Please confirm deletion")
    
    with tab2:
        st.subheader("📊 Data Overview")
        
        # Summary stats
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_shipments = data_summary['shipment_count'].sum()
            st.metric("📦 Total Shipments", f"{total_shipments:,}")
        
        with col2:
            unique_clients = data_summary['client'].nunique()
            st.metric("👥 Unique Clients", unique_clients)
        
        with col3:
            unique_carriers = data_summary['carrier'].nunique()
            st.metric("🚚 Carriers", unique_carriers)
        
        with col4:
            unique_cycles = data_summary['cycle_period'].nunique()
            st.metric("📅 Billing Cycles", unique_cycles)
        
        # Data table with formatted values
        st.subheader("📋 All Uploaded Data")
        display_summary = data_summary.copy()
        display_summary['cost'] = display_summary['cost'].apply(lambda x: f"${x:,.2f}")
        display_summary['billable_amount'] = display_summary['billable_amount'].apply(lambda x: f"${x:,.2f}")
        display_summary['profit'] = display_summary['profit'].apply(lambda x: f"${x:,.2f}")
        
        st.dataframe(display_summary, use_container_width=True, hide_index=True)
        
        # Upload history
        st.markdown("---")
        st.subheader("📝 Upload History")
        upload_log = tracker.load_upload_log()
        
        if not upload_log.empty:
            # Add status column if it doesn't exist (for older versions)
            if 'status' not in upload_log.columns:
                upload_log['status'] = 'Active'
            
            # Format display
            display_log = upload_log.copy()
            display_log['upload_date'] = pd.to_datetime(display_log['upload_date']).dt.strftime('%Y-%m-%d %H:%M')
            
            st.dataframe(
                display_log[['filename', 'carrier', 'cycle_period', 'records_imported', 'upload_date', 'status']], 
                use_container_width=True, 
                hide_index=True
            )
    
    with tab3:
        st.subheader("💾 Data Backup")
        st.markdown("*Export complete backup of all billing data*")
        
        if st.button("📥 Generate Backup"):
            backup_data = tracker.export_data_backup()
            if backup_data:
                filename = f"freight_billing_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                
                st.download_button(
                    "📁 Download Complete Backup",
                    backup_data,
                    filename,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.success("✅ Backup generated successfully!")
            else:
                st.error("❌ Error generating backup")
        
        # File sizes
        st.markdown("---")
        st.subheader("💾 Storage Information")
        
        files_info = []
        files = [
            ("Shipment Data", tracker.shipment_data_file),
            ("Billing Checklist", tracker.billing_checklist_file),
            ("Upload Log", tracker.upload_log_file)
        ]
        
        for name, path in files:
            if path.exists():
                size_mb = path.stat().st_size / (1024 * 1024)
                files_info.append([name, f"{size_mb:.2f} MB", "✅ Exists"])
            else:
                files_info.append([name, "0 MB", "❌ Missing"])
        
        files_df = pd.DataFrame(files_info, columns=["File", "Size", "Status"])
        st.dataframe(files_df, use_container_width=True, hide_index=True)
    
    with tab4:
        st.subheader("⚠️ Reset All Data")
        st.error("🚨 **DANGER ZONE:** This will permanently delete ALL billing data")
        
        st.markdown("""
        **This action will:**
        - Delete all shipment records
        - Clear the billing checklist
        - Remove upload history
        - Reset all files to empty state
        
        **⚠️ This cannot be undone!**
        """)
        
        with st.form("reset_all_form"):
            st.markdown("**To confirm, type:** `DELETE_ALL_BILLING_DATA`")
            confirmation_code = st.text_input("Confirmation Code", type="password")
            
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.form_submit_button("🗑️ RESET ALL", type="secondary"):
                    if confirmation_code == "DELETE_ALL_BILLING_DATA":
                        success, message = tracker.clear_all_data(confirmation_code)
                        if success:
                            st.success("✅ All data has been cleared")
                            st.rerun()
                        else:
                            st.error(f"❌ {message}")
                    else:
                        st.error("❌ Incorrect confirmation code")
            
            with col2:
                st.info("💡 **Tip:** Generate a backup before resetting if you want to preserve data")

if __name__ == "__main__":
    import time
    main()
