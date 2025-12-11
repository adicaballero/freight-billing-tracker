import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import io
import os
import json
import re
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
        self.config_file = self.data_folder / "config.json"
        
        self.init_excel_files()
        self.load_config()
    
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
                'carrier', 'cycle_period','status', 'deleted_date', 'source_path'
            ])
            log_df.to_excel(self.upload_log_file, index=False)
    
    def load_config(self):
        """Load configuration from JSON file"""
        default_config = {
            'input_folder': '',
            'filename_pattern': 'auto',  # auto, manual
            'processed_files': []  # List of processed file paths
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
                # Ensure all keys exist
                for key, value in default_config.items():
                    if key not in self.config:
                        self.config[key] = value
            except:
                self.config = default_config
        else:
            self.config = default_config
    
    def save_config(self):
        """Save configuration to JSON file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
    
    def set_input_folder(self, folder_path):
        """Set the input folder path"""
        self.config['input_folder'] = folder_path
        self.save_config()
    
    def get_input_folder(self):
        """Get the configured input folder path"""
        return self.config.get('input_folder', '')
    
    def parse_filename(self, filename):
        """
        Parse carrier name and cycle period from filename.
        Supports multiple formats:
        - CarrierName_YYYY-MM.xlsx (e.g., FedEx_2024-08.xlsx)
        - CarrierName_MonthYYYY.xlsx (e.g., UPS_November2024.xlsx)
        - CarrierName_YYYY-MM-WeekN.xlsx (e.g., DHL_2024-08-Week1.xlsx)
        - CarrierName_CyclePeriod.xlsx (generic)
        
        Returns: (carrier_name, cycle_period) or (None, None) if parsing fails
        """
        # Remove extension
        name_without_ext = Path(filename).stem
        
        # Try to split by underscore
        parts = name_without_ext.split('_', 1)
        
        if len(parts) == 2:
            carrier_name = parts[0].strip()
            cycle_part = parts[1].strip()
            
            # Clean up carrier name (replace common separators)
            carrier_name = carrier_name.replace('-', ' ').replace('_', ' ').title()
            
            # Try to normalize cycle period
            cycle_period = self.normalize_cycle_period(cycle_part)
            
            return carrier_name, cycle_period
        
        # Try splitting by hyphen if underscore didn't work
        parts = name_without_ext.split('-', 1)
        if len(parts) == 2:
            carrier_name = parts[0].strip().title()
            cycle_period = self.normalize_cycle_period(parts[1].strip())
            return carrier_name, cycle_period
        
        return None, None
    
    def normalize_cycle_period(self, cycle_str):
        """
        Normalize cycle period string to consistent format.
        Tries to convert various formats to YYYY-MM or keeps as-is.
        """
        cycle_str = cycle_str.strip()
        
        # Already in YYYY-MM format
        if re.match(r'^\d{4}-\d{2}$', cycle_str):
            return cycle_str
        
        # YYYY-MM-WeekN format - keep as is
        if re.match(r'^\d{4}-\d{2}-Week\d+$', cycle_str, re.IGNORECASE):
            return cycle_str
        
        # MonthYYYY format (e.g., November2024)
        month_match = re.match(r'^([A-Za-z]+)(\d{4})$', cycle_str)
        if month_match:
            month_name = month_match.group(1)
            year = month_match.group(2)
            try:
                month_num = datetime.strptime(month_name, '%B').month
                return f"{year}-{month_num:02d}"
            except:
                pass
        
        # MonthYY format (e.g., Nov24)
        month_match = re.match(r'^([A-Za-z]{3})(\d{2})$', cycle_str)
        if month_match:
            month_name = month_match.group(1)
            year = month_match.group(2)
            try:
                month_num = datetime.strptime(month_name, '%b').month
                return f"20{year}-{month_num:02d}"
            except:
                pass
        
        # Return as-is if no pattern matches
        return cycle_str
    
    def scan_input_folder(self):
        """
        Scan the configured input folder for carrier files.
        Returns list of file info dicts with parsing results.
        """
        input_folder = self.get_input_folder()
        
        if not input_folder:
            return [], "No input folder configured"
        
        folder_path = Path(input_folder)
        
        if not folder_path.exists():
            return [], f"Folder does not exist: {input_folder}"
        
        if not folder_path.is_dir():
            return [], f"Path is not a folder: {input_folder}"
        
        # Get list of processed files
        processed_files = set(self.config.get('processed_files', []))
        upload_log = self.load_upload_log()
        if not upload_log.empty and 'source_path' in upload_log.columns:
            processed_files.update(upload_log['source_path'].dropna().tolist())
        
        files_info = []
        
        # Scan for Excel and CSV files
        for ext in ['*.xlsx', '*.csv', '*.xls']:
            for file_path in folder_path.glob(ext):
                # Skip temporary files
                if file_path.name.startswith('~$'):
                    continue
                
                # Parse filename
                carrier, cycle = self.parse_filename(file_path.name)
                
                # Check if already processed
                is_processed = str(file_path) in processed_files
                
                # Get file info
                file_stat = file_path.stat()
                file_size_mb = file_stat.st_size / (1024 * 1024)
                modified_date = datetime.fromtimestamp(file_stat.st_mtime)
                
                files_info.append({
                    'path': str(file_path),
                    'filename': file_path.name,
                    'carrier': carrier,
                    'cycle_period': cycle,
                    'size_mb': round(file_size_mb, 2),
                    'modified_date': modified_date,
                    'is_processed': is_processed,
                    'parse_success': carrier is not None and cycle is not None
                })
        
        # Sort by modified date (newest first)
        files_info.sort(key=lambda x: x['modified_date'], reverse=True)
        
        return files_info, None
    
    def process_file_from_path(self, file_path, carrier_name, cycle_period, replace_existing=False):
        """
        Process a carrier file from a file path (instead of uploaded file).
        """
        try:
            file_path = Path(file_path)
            
            if not file_path.exists():
                return False, f"File not found: {file_path}"
            
            # Check for existing data
            has_existing, existing_count = self.check_existing_data(carrier_name, cycle_period)
            
            if has_existing and not replace_existing:
                return False, f"Data already exists for {carrier_name} - {cycle_period} ({existing_count:,} records). Use 'Replace Existing Data' option to update."
            
            # Read file
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            
            # Remove existing data if replacing
            if replace_existing and has_existing:
                self.remove_existing_data(carrier_name, cycle_period)
            
            # Read file content for hash
            with open(file_path, 'rb') as f:
                file_content = f.read()
            file_hash = self.get_file_hash(file_content)
            
            # Read into DataFrame
            if str(file_path).endswith('.xlsx') or str(file_path).endswith('.xls'):
                df = pd.read_excel(file_path, engine='openpyxl')
            elif str(file_path).endswith('.csv'):
                if file_size_mb > 10:
                    chunk_list = []
                    chunk_size = 10000
                    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                        chunk_list.append(chunk)
                    df = pd.concat(chunk_list, ignore_index=True)
                else:
                    df = pd.read_csv(file_path)
            else:
                return False, "Unsupported file format. Please use Excel or CSV."
            
            # Clean up DataFrame
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            
            # Remove columns that are mostly empty (>95% null)
            null_threshold = 0.95
            null_percentages = df.isnull().mean()
            cols_to_keep = null_percentages[null_percentages < null_threshold].index
            df = df[cols_to_keep]
            
            # Auto-detect columns
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
                    'invoice amount', 'charge amount', 'total billable', 'total_billable'
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
            
            column_map = {}
            df_columns_lower = {col: col.lower().strip().replace(' ', '').replace('_', '') for col in df.columns}
            
            for standard, variants in standard_columns.items():
                for original_col, clean_col in df_columns_lower.items():
                    clean_variants = [v.lower().replace(' ', '').replace('_', '') for v in variants]
                    if clean_col in clean_variants:
                        column_map[original_col] = standard
                        break
            
            df = df.rename(columns=column_map)
            
            # Verify required columns
            required_cols = ['client', 'cost', 'billable_amount']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                available_cols = list(df.columns)
                return False, f"Missing required columns: {missing_cols}. Available columns: {available_cols}"
            
            # Create standardized DataFrame
            STANDARD_COLUMNS = [
                'carrier', 'client', 'tracking_number', 'service_type', 
                'cost', 'billable_amount', 'weight', 'zone', 
                'ship_date', 'delivery_date', 'invoice_status', 
                'invoice_number', 'invoice_date', 'cycle_period', 
                'upload_timestamp', 'file_hash'
            ]
            
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
                    'file_hash': file_hash
                }
                standardized_data.append(standard_row)
            
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
            
            # Handle existing data
            existing_shipments = self.load_shipment_data()
            
            if not existing_shipments.empty:
                existing_standardized = pd.DataFrame(columns=STANDARD_COLUMNS)
                
                for col in STANDARD_COLUMNS:
                    if col in existing_shipments.columns:
                        existing_standardized[col] = existing_shipments[col]
                    else:
                        if col in ['ship_date', 'delivery_date', 'invoice_date']:
                            existing_standardized[col] = pd.NaT
                        elif col in ['cost', 'billable_amount', 'weight']:
                            existing_standardized[col] = 0
                        else:
                            existing_standardized[col] = ''
                
                combined_shipments = pd.concat([existing_standardized, standardized_df], ignore_index=True)
            else:
                combined_shipments = standardized_df
            
            self.save_shipment_data(combined_shipments)
            
            # Update upload log with source path
            upload_log = self.load_upload_log()
            
            new_log_entry = pd.DataFrame([{
                'filename': file_path.name,
                'file_hash': file_hash,
                'upload_date': datetime.now(),
                'records_imported': final_count,
                'carrier': carrier_name,
                'cycle_period': cycle_period,
                'status': 'Active',
                'deleted_date': None,
                'source_path': str(file_path)
            }])
            
            if not upload_log.empty:
                if 'status' not in upload_log.columns:
                    upload_log['status'] = 'Active'
                if 'deleted_date' not in upload_log.columns:
                    upload_log['deleted_date'] = None
                if 'source_path' not in upload_log.columns:
                    upload_log['source_path'] = None
            
            combined_log = pd.concat([upload_log, new_log_entry], ignore_index=True)
            self.save_upload_log(combined_log)
            
            # Update billing checklist
            self.update_billing_checklist(standardized_df)
            
            # Mark file as processed
            if str(file_path) not in self.config.get('processed_files', []):
                if 'processed_files' not in self.config:
                    self.config['processed_files'] = []
                self.config['processed_files'].append(str(file_path))
                self.save_config()
            
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
    
    def mark_file_as_processed(self, file_path):
        """Mark a file as processed without actually processing it"""
        if str(file_path) not in self.config.get('processed_files', []):
            if 'processed_files' not in self.config:
                self.config['processed_files'] = []
            self.config['processed_files'].append(str(file_path))
            self.save_config()
    
    def unmark_file_as_processed(self, file_path):
        """Remove a file from the processed list"""
        if 'processed_files' in self.config:
            try:
                self.config['processed_files'].remove(str(file_path))
                self.save_config()
            except ValueError:
                pass
    
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
                'deleted_date': None,
                'source_path': None  # Manual upload, no source path
            }])
        
            # Ensure upload log structure consistency
            if not upload_log.empty:
                if 'status' not in upload_log.columns:
                    upload_log['status'] = 'Active'
                if 'deleted_date' not in upload_log.columns:
                    upload_log['deleted_date'] = None
                if 'source_path' not in upload_log.columns:
                    upload_log['source_path'] = None
        
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
        """Delete all data for a specific client/cycle combination (all carriers)"""
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
            return False, f"Error deleting data: {str(e)}"

    def get_data_summary(self):
        """Get summary of all uploaded data grouped by carrier/cycle"""
        shipment_data = self.load_shipment_data()
        
        if shipment_data.empty:
            return pd.DataFrame()
        
        summary = shipment_data.groupby(['carrier', 'cycle_period', 'client']).agg({
            'tracking_number': 'count',
            'cost': 'sum',
            'billable_amount': 'sum',
            'upload_timestamp': 'max'
        }).reset_index()
        
        summary.rename(columns={
            'tracking_number': 'shipment_count',
            'upload_timestamp': 'upload_date'
        }, inplace=True)
        
        summary['profit'] = summary['billable_amount'] - summary['cost']
        
        return summary.sort_values(['cycle_period', 'carrier', 'client'], ascending=[False, True, True])

    def clear_all_data(self, confirmation_code):
        """Clear all data after confirmation"""
        if confirmation_code != "DELETE_ALL_BILLING_DATA":
            return False, "Invalid confirmation code"
        
        try:
            # Reset all Excel files
            self.shipment_data_file.unlink(missing_ok=True)
            self.billing_checklist_file.unlink(missing_ok=True)
            self.upload_log_file.unlink(missing_ok=True)
            
            # Clear processed files list
            self.config['processed_files'] = []
            self.save_config()
            
            # Reinitialize
            self.init_excel_files()
            
            return True, "All data cleared successfully"
        except Exception as e:
            return False, f"Error clearing data: {str(e)}"

    def export_data_backup(self):
        """Export complete backup of all data"""
        try:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Shipment data
                shipment_data = self.load_shipment_data()
                if not shipment_data.empty:
                    shipment_data.to_excel(writer, sheet_name='Shipment_Data', index=False)
                
                # Billing checklist
                checklist = self.load_billing_checklist()
                if not checklist.empty:
                    checklist.to_excel(writer, sheet_name='Billing_Checklist', index=False)
                
                # Upload log
                upload_log = self.load_upload_log()
                if not upload_log.empty:
                    upload_log.to_excel(writer, sheet_name='Upload_Log', index=False)
            
            return output.getvalue()
        except Exception as e:
            print(f"Error creating backup: {e}")
            return None


# ============================================================================
# STREAMLIT UI
# ============================================================================

def main():
    st.set_page_config(
        page_title="Freight Billing Tracker",
        page_icon="📋",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS
    st.markdown("""
    <style>
    .main-header {font-size: 2.5rem; font-weight: bold; color: #1E88E5;}
    .metric-card {background-color: #f8f9fa; padding: 1rem; border-radius: 0.5rem;}
    .status-ready {color: #28a745; font-weight: bold;}
    .status-billed {color: #6c757d; font-weight: bold;}
    .file-new {background-color: #d4edda; padding: 0.5rem; border-radius: 0.25rem; margin: 0.25rem 0;}
    .file-processed {background-color: #f8f9fa; padding: 0.5rem; border-radius: 0.25rem; margin: 0.25rem 0;}
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
        ["📊 Billing Dashboard", "📂 Scan Folder", "📤 Upload Carrier Data", "📋 Client Billing Checklist", "🚚 Carrier Breakdown", "📈 Reports", "🗂️ Data Management", "⚙️ Settings"]
    )
    
    if page == "📊 Billing Dashboard":
        show_billing_dashboard(tracker)
    elif page == "📂 Scan Folder":
        show_scan_folder_page(tracker)
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


def show_scan_folder_page(tracker):
    """Show the folder scanning page for semi-automatic file processing"""
    st.header("📂 Scan Folder for Carrier Files")
    st.markdown("*Automatically detect and process carrier files from a designated folder*")
    
    # Show current folder configuration
    current_folder = tracker.get_input_folder()
    
    if not current_folder:
        st.warning("⚠️ No input folder configured. Please set one in Settings or below.")
        
        with st.expander("🔧 Quick Setup - Configure Input Folder", expanded=True):
            new_folder = st.text_input(
                "📁 Input Folder Path",
                placeholder=r"e.g., C:\BillingFiles or \\server\share\billing",
                help="Enter the full path to the folder containing carrier files"
            )
            
            if st.button("💾 Save Folder Path"):
                if new_folder:
                    tracker.set_input_folder(new_folder)
                    st.success(f"✅ Input folder set to: {new_folder}")
                    st.rerun()
                else:
                    st.error("Please enter a folder path")
        return
    
    # Show folder info
    st.info(f"📁 **Scanning folder:** `{current_folder}`")
    
    # Scan button
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        scan_button = st.button("🔄 Scan Folder", type="primary")
    with col2:
        show_processed = st.checkbox("Show processed files", value=False)
    
    # Scan folder
    files_info, error = tracker.scan_input_folder()
    
    if error:
        st.error(f"❌ {error}")
        return
    
    if not files_info:
        st.info("📋 No Excel or CSV files found in the folder.")
        return
    
    # Filter files
    if not show_processed:
        display_files = [f for f in files_info if not f['is_processed']]
    else:
        display_files = files_info
    
    # Summary
    new_files = len([f for f in files_info if not f['is_processed']])
    processed_files = len([f for f in files_info if f['is_processed']])
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📄 Total Files", len(files_info))
    with col2:
        st.metric("🆕 New Files", new_files)
    with col3:
        st.metric("✅ Processed", processed_files)
    
    st.markdown("---")
    
    if not display_files:
        st.success("✅ All files have been processed!")
        return
    
    # File naming convention help
    with st.expander("💡 Filename Convention Help"):
        st.markdown("""
        **Recommended filename format:** `CarrierName_CyclePeriod.xlsx`
        
        **Examples:**
        - `FedEx_2024-11.xlsx` → Carrier: FedEx, Cycle: 2024-11
        - `UPS_November2024.csv` → Carrier: UPS, Cycle: 2024-11
        - `DHL_2024-11-Week1.xlsx` → Carrier: DHL, Cycle: 2024-11-Week1
        
        Files that don't follow this convention can still be processed - you'll just need to manually enter the carrier and cycle.
        """)
    
    st.subheader("📋 Files Found")
    
    # Process files section
    if 'selected_files' not in st.session_state:
        st.session_state.selected_files = {}
    
    # Select all / none buttons
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("☑️ Select All New"):
            for f in display_files:
                if not f['is_processed']:
                    st.session_state.selected_files[f['path']] = True
            st.rerun()
    with col2:
        if st.button("⬜ Clear Selection"):
            st.session_state.selected_files = {}
            st.rerun()
    
    # Display files
    files_to_process = []
    
    for idx, file_info in enumerate(display_files):
        file_key = f"file_{idx}"
        
        with st.container():
            # File card
            if file_info['is_processed']:
                st.markdown(f"<div class='file-processed'>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='file-new'>", unsafe_allow_html=True)
            
            col1, col2, col3, col4, col5 = st.columns([0.5, 3, 2, 2, 1.5])
            
            with col1:
                if not file_info['is_processed']:
                    selected = st.checkbox(
                        "Select",
                        key=f"select_{file_key}",
                        value=st.session_state.selected_files.get(file_info['path'], False),
                        label_visibility="collapsed"
                    )
                    st.session_state.selected_files[file_info['path']] = selected
                else:
                    st.write("✅")
            
            with col2:
                st.write(f"**{file_info['filename']}**")
                st.caption(f"Size: {file_info['size_mb']} MB | Modified: {file_info['modified_date'].strftime('%Y-%m-%d %H:%M')}")
            
            with col3:
                if file_info['parse_success']:
                    carrier_value = st.text_input(
                        "Carrier",
                        value=file_info['carrier'],
                        key=f"carrier_{file_key}",
                        disabled=file_info['is_processed'],
                        label_visibility="collapsed"
                    )
                else:
                    carrier_value = st.text_input(
                        "Carrier (required)",
                        placeholder="Enter carrier name",
                        key=f"carrier_{file_key}",
                        disabled=file_info['is_processed'],
                        label_visibility="collapsed"
                    )
                file_info['carrier_input'] = carrier_value
            
            with col4:
                if file_info['parse_success']:
                    cycle_value = st.text_input(
                        "Cycle",
                        value=file_info['cycle_period'],
                        key=f"cycle_{file_key}",
                        disabled=file_info['is_processed'],
                        label_visibility="collapsed"
                    )
                else:
                    cycle_value = st.text_input(
                        "Cycle (required)",
                        placeholder="e.g., 2024-11",
                        key=f"cycle_{file_key}",
                        disabled=file_info['is_processed'],
                        label_visibility="collapsed"
                    )
                file_info['cycle_input'] = cycle_value
            
            with col5:
                if file_info['is_processed']:
                    st.write("✅ Processed")
                elif not file_info['parse_success']:
                    st.write("⚠️ Manual entry")
                else:
                    st.write("🆕 Ready")
            
            # Track for processing
            if st.session_state.selected_files.get(file_info['path'], False):
                files_to_process.append(file_info)
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        st.markdown("---")
    
    # Process selected files
    if files_to_process:
        st.subheader(f"🚀 Process {len(files_to_process)} Selected File(s)")
        
        # Validate all have carrier/cycle
        valid_files = []
        invalid_files = []
        
        for f in files_to_process:
            carrier = f.get('carrier_input', f.get('carrier', ''))
            cycle = f.get('cycle_input', f.get('cycle_period', ''))
            
            if carrier and cycle:
                valid_files.append({
                    'path': f['path'],
                    'filename': f['filename'],
                    'carrier': carrier,
                    'cycle': cycle
                })
            else:
                invalid_files.append(f['filename'])
        
        if invalid_files:
            st.warning(f"⚠️ The following files need carrier and cycle info: {', '.join(invalid_files)}")
        
        if valid_files:
            # Replace existing option
            replace_existing = st.checkbox(
                "🔄 Replace existing data if found",
                help="If data already exists for a carrier/cycle combination, replace it with the new file"
            )
            
            # Preview what will be processed
            with st.expander("📋 Preview Processing", expanded=True):
                preview_df = pd.DataFrame(valid_files)
                st.dataframe(preview_df[['filename', 'carrier', 'cycle']], use_container_width=True, hide_index=True)
            
            if st.button("🚀 Process Selected Files", type="primary"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                results = []
                
                for i, file_info in enumerate(valid_files):
                    status_text.text(f"Processing {file_info['filename']}...")
                    
                    success, message = tracker.process_file_from_path(
                        file_info['path'],
                        file_info['carrier'],
                        file_info['cycle'],
                        replace_existing=replace_existing
                    )
                    
                    results.append({
                        'filename': file_info['filename'],
                        'success': success,
                        'message': message
                    })
                    
                    progress_bar.progress((i + 1) / len(valid_files))
                
                status_text.empty()
                
                # Show results
                st.subheader("📊 Processing Results")
                
                success_count = sum(1 for r in results if r['success'])
                fail_count = len(results) - success_count
                
                if success_count > 0:
                    st.success(f"✅ Successfully processed {success_count} file(s)")
                if fail_count > 0:
                    st.error(f"❌ Failed to process {fail_count} file(s)")
                
                for result in results:
                    if result['success']:
                        st.write(f"✅ **{result['filename']}**: {result['message']}")
                    else:
                        st.write(f"❌ **{result['filename']}**: {result['message']}")
                
                # Clear selection
                st.session_state.selected_files = {}
                
                if st.button("🔄 Refresh"):
                    st.rerun()


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
    """Show carrier data upload page (manual upload)"""
    st.header("📤 Upload Carrier Reconciliation Data")
    st.markdown("*Manual upload - for automated processing, use the Scan Folder page*")
    
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
            if file_size_mb < 20:
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
                file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
                if file_size_mb > 20:
                    processing_msg = f"⏳ Processing large file ({file_size_mb:.1f} MB)... This may take 2-3 minutes."
                elif file_size_mb > 10:
                    processing_msg = f"⏳ Processing file ({file_size_mb:.1f} MB)... This may take 1-2 minutes."
                else:
                    processing_msg = "⏳ Processing file..."
                
                with st.spinner(processing_msg):
                    uploaded_file.seek(0)
                    success, message = tracker.process_carrier_file(
                        uploaded_file, 
                        carrier_name, 
                        cycle_period,
                        replace_existing=replace_existing
                    )
                    
                    if success:
                        st.success(f"✅ {message}")
                        st.balloons()
                    else:
                        st.error(f"❌ {message}")
            else:
                st.error("Please fill in all required fields")


def show_client_checklist(tracker):
    """Show client billing checklist"""
    st.header("📋 Client Billing Checklist")
    
    client_summary = tracker.get_client_summary()
    
    if client_summary.empty:
        st.info("📋 No billing data available")
        return
    
    # Filters
    col1, col2 = st.columns(2)
    
    with col1:
        cycles = ['All'] + list(client_summary['cycle_period'].unique())
        selected_cycle = st.selectbox("📅 Filter by Cycle", cycles)
    
    with col2:
        clients = ['All'] + list(client_summary['client'].unique())
        selected_client = st.selectbox("👤 Filter by Client", clients)
    
    # Apply filters
    filtered_data = client_summary.copy()
    if selected_cycle != 'All':
        filtered_data = filtered_data[filtered_data['cycle_period'] == selected_cycle]
    if selected_client != 'All':
        filtered_data = filtered_data[filtered_data['client'] == selected_client]
    
    # Display data
    if not filtered_data.empty:
        # Format for display
        display_data = filtered_data.copy()
        for col in ['total_cost', 'total_billable', 'profit']:
            display_data[col] = display_data[col].apply(lambda x: f"${x:,.2f}")
        display_data['profit_margin'] = display_data['profit_margin'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(display_data, use_container_width=True, hide_index=True)
        
        # Mark as billed section
        st.markdown("---")
        st.subheader("✅ Mark Client as Billed")
        
        with st.form("mark_billed_form"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                bill_client = st.selectbox("👤 Client", filtered_data['client'].unique())
            
            with col2:
                bill_cycle = st.selectbox("📅 Cycle", filtered_data['cycle_period'].unique())
            
            with col3:
                invoice_number = st.text_input("📄 Invoice Number")
            
            if st.form_submit_button("✅ Mark as Billed"):
                if invoice_number:
                    tracker.mark_client_billed(bill_client, bill_cycle, invoice_number)
                    st.success(f"✅ Marked {bill_client} - {bill_cycle} as billed")
                    st.rerun()
                else:
                    st.error("Please enter an invoice number")


def show_carrier_breakdown(tracker):
    """Show carrier breakdown page"""
    st.header("🚚 Carrier Breakdown")
    
    checklist = tracker.get_billing_checklist()
    
    if checklist.empty:
        st.info("📋 No billing data available")
        return
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        clients = ['All'] + list(checklist['client'].unique())
        selected_client = st.selectbox("👤 Client", clients)
    
    with col2:
        if selected_client != 'All':
            cycles = ['All'] + list(checklist[checklist['client'] == selected_client]['cycle_period'].unique())
        else:
            cycles = ['All'] + list(checklist['cycle_period'].unique())
        selected_cycle = st.selectbox("📅 Cycle", cycles)
    
    with col3:
        carriers = ['All'] + list(checklist['carrier'].unique())
        selected_carrier = st.selectbox("🚚 Carrier", carriers)
    
    # Apply filters
    filtered_data = checklist.copy()
    if selected_client != 'All':
        filtered_data = filtered_data[filtered_data['client'] == selected_client]
    if selected_cycle != 'All':
        filtered_data = filtered_data[filtered_data['cycle_period'] == selected_cycle]
    if selected_carrier != 'All':
        filtered_data = filtered_data[filtered_data['carrier'] == selected_carrier]
    
    if filtered_data.empty:
        st.info("No data matching filters")
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📦 Total Shipments", f"{filtered_data['shipment_count'].sum():,}")
    with col2:
        st.metric("💸 Total Cost", f"${filtered_data['total_cost'].sum():,.2f}")
    with col3:
        st.metric("💰 Total Billable", f"${filtered_data['total_billable'].sum():,.2f}")
    with col4:
        st.metric("📈 Total Profit", f"${filtered_data['profit'].sum():,.2f}")
    
    # Data table
    st.subheader("📋 Carrier Details")
    display_data = filtered_data.copy()
    for col in ['total_cost', 'total_billable', 'profit']:
        if col in display_data.columns:
            display_data[col] = display_data[col].apply(lambda x: f"${x:,.2f}")
    if 'profit_margin' in display_data.columns:
        display_data['profit_margin'] = display_data['profit_margin'].apply(lambda x: f"{x:.1f}%")
    
    st.dataframe(display_data, use_container_width=True, hide_index=True)


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
            cycle_summary = client_summary.groupby('cycle_period').agg({
                'shipment_count': 'sum',
                'total_cost': 'sum',
                'total_billable': 'sum',
                'profit': 'sum'
            }).reset_index()
            cycle_summary['profit_margin'] = (
                cycle_summary['profit'] / cycle_summary['total_billable'] * 100
            ).round(2)
            
            display_summary = cycle_summary.copy()
            for col in ['total_cost', 'total_billable', 'profit']:
                display_summary[col] = display_summary[col].apply(lambda x: f"${x:,.2f}")
            display_summary['profit_margin'] = display_summary['profit_margin'].apply(lambda x: f"{x:.1f}%")
            
            st.dataframe(display_summary, use_container_width=True, hide_index=True)
    
    elif report_type == "Carrier Performance":
        st.subheader("🚚 Carrier Performance Analysis")
        
        detailed_checklist = tracker.get_billing_checklist()
        if not detailed_checklist.empty:
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
            
            fig = px.bar(
                carrier_performance, 
                x='carrier', 
                y='total_billable',
                title="Billable Amount by Carrier"
            )
            st.plotly_chart(fig, use_container_width=True)


def show_data_management(tracker):
    """Show data management page"""
    st.header("🗂️ Data Management")
    
    data_summary = tracker.get_data_summary()
    
    if data_summary.empty:
        st.info("📋 No data to manage")
        return
    
    tab1, tab2, tab3, tab4 = st.tabs(["🗑️ Delete Data", "📊 Data Overview", "💾 Backup", "⚠️ Reset All"])
    
    with tab1:
        st.subheader("🗑️ Delete Uploaded Data")
        st.warning("⚠️ Deleting data cannot be undone. Consider backing up first.")
        
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
            
            confirm_delete = st.checkbox("✅ I confirm deletion")
            
            if st.form_submit_button("🗑️ Delete Data"):
                if confirm_delete and delete_carrier and delete_cycle:
                    success, message = tracker.delete_carrier_data(delete_carrier, delete_cycle)
                    if success:
                        st.success(f"✅ {message}")
                        st.rerun()
                    else:
                        st.error(f"❌ {message}")
                else:
                    st.error("Please confirm deletion")
    
    with tab2:
        st.subheader("📊 Data Overview")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📦 Total Shipments", f"{data_summary['shipment_count'].sum():,}")
        with col2:
            st.metric("👥 Unique Clients", data_summary['client'].nunique())
        with col3:
            st.metric("🚚 Carriers", data_summary['carrier'].nunique())
        with col4:
            st.metric("📅 Billing Cycles", data_summary['cycle_period'].nunique())
        
        st.dataframe(data_summary, use_container_width=True, hide_index=True)
    
    with tab3:
        st.subheader("💾 Data Backup")
        
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
                st.success("✅ Backup generated!")
    
    with tab4:
        st.subheader("⚠️ Reset All Data")
        st.error("🚨 **DANGER ZONE:** This will permanently delete ALL billing data")
        
        with st.form("reset_all_form"):
            st.markdown("**To confirm, type:** `DELETE_ALL_BILLING_DATA`")
            confirmation_code = st.text_input("Confirmation Code", type="password")
            
            if st.form_submit_button("🗑️ RESET ALL"):
                if confirmation_code == "DELETE_ALL_BILLING_DATA":
                    success, message = tracker.clear_all_data(confirmation_code)
                    if success:
                        st.success("✅ All data has been cleared")
                        st.rerun()
                    else:
                        st.error(f"❌ {message}")
                else:
                    st.error("❌ Incorrect confirmation code")


def show_settings(tracker):
    """Show settings page"""
    st.header("⚙️ Settings")
    
    # Input Folder Configuration
    st.subheader("📂 Input Folder Configuration")
    
    current_folder = tracker.get_input_folder()
    
    st.write(f"**Current Input Folder:** `{current_folder if current_folder else 'Not configured'}`")
    
    with st.form("folder_settings"):
        new_folder = st.text_input(
            "📁 Input Folder Path",
            value=current_folder,
            placeholder=r"e.g., C:\BillingFiles or \\server\share\billing",
            help="Enter the full path to the folder containing carrier files"
        )
        
        st.markdown("""
        **Filename Convention:**
        Files should be named as `CarrierName_CyclePeriod.xlsx` for automatic detection.
        
        Examples:
        - `FedEx_2024-11.xlsx`
        - `UPS_November2024.csv`
        - `DHL_2024-11-Week1.xlsx`
        """)
        
        if st.form_submit_button("💾 Save Settings"):
            tracker.set_input_folder(new_folder)
            st.success(f"✅ Input folder updated to: {new_folder}")
    
    # Clear processed files list
    st.markdown("---")
    st.subheader("🔄 Processed Files")
    
    processed_count = len(tracker.config.get('processed_files', []))
    st.write(f"**Files marked as processed:** {processed_count}")
    
    if processed_count > 0:
        with st.expander("View processed files"):
            for f in tracker.config.get('processed_files', []):
                st.write(f"- `{f}`")
        
        if st.button("🗑️ Clear Processed Files List"):
            tracker.config['processed_files'] = []
            tracker.save_config()
            st.success("✅ Processed files list cleared. Files will show as 'new' on next scan.")
            st.rerun()
    
    # Data Files Info
    st.markdown("---")
    st.subheader("📁 Data Files")
    st.write(f"**Data Location:** `{tracker.data_folder}`")
    
    files = [
        ("📦 Shipment Data", tracker.shipment_data_file),
        ("📋 Billing Checklist", tracker.billing_checklist_file),
        ("📝 Upload Log", tracker.upload_log_file),
        ("⚙️ Config", tracker.config_file)
    ]
    
    for name, path in files:
        col1, col2 = st.columns([1, 3])
        with col1:
            if path.exists():
                st.success(f"✅ {name}")
            else:
                st.error(f"❌ {name}")
        with col2:
            st.code(str(path))
    
    # Column Detection Help
    st.markdown("---")
    st.subheader("❓ Column Detection Help")
    
    with st.expander("🔧 Supported Column Names"):
        st.markdown("""
        **The system automatically detects these column variations:**
        
        - **Client:** `client`, `customer`, `customer_name`, `account`, `consignee`, `shipper`, `company`
        - **Cost:** `cost`, `freight_cost`, `shipping_cost`, `carrier_charge`, `total_cost`
        - **Billable Amount:** `billable`, `billable_amount`, `revenue`, `charge_amount`, `bill_amount`
        - **Tracking:** `tracking`, `tracking_number`, `tracking_id`, `awb`, `pro`
        - **Service:** `service`, `service_type`, `service_level`
        - **Weight:** `weight`, `package_weight`, `total_weight`
        - **Zone:** `zone`, `delivery_zone`, `shipping_zone`
        - **Dates:** `ship_date`, `pickup_date`, `delivery_date`
        """)


if __name__ == "__main__":
    main()
