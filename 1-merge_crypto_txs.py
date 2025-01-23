# CryptoCompare API key - https://min-api.cryptocompare.com
# Optional: Script will use daily historical prices if no API key is provided
# With API key: Script can fetch minute-level historical prices (last 7 days only)
CRYPTOCOMPARE_API_KEY = 'YOUR-API-KEY-HERE'

import pandas as pd
import glob
from datetime import datetime, timedelta
import requests
import time
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
from typing import Dict, List
import logging
import openpyxl
import openpyxl.utils
import openpyxl.styles

LUNA_TRANSITION_DATE = datetime(2022, 5, 28)  # LUNA -> LUNC transition date

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='crypto_transactions_merge.log'
)

def load_price_cache():
    """Load historical price cache from JSON file"""
    cache_file = Path('price_cache.json')
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading price cache: {e}")
    return {}

def save_price_cache(cache):
    """Save historical price cache to JSON file"""
    try:
        with open('price_cache.json', 'w') as f:
            json.dump(cache, f)
    except Exception as e:
        print(f"Error saving price cache: {e}")

# Initialize price cache
price_cache = load_price_cache()

def safe_concat(dfs: list, columns: list, **kwargs) -> pd.DataFrame:
    """Safely concatenate DataFrames ensuring all columns exist"""
    if not dfs:
        return pd.DataFrame(columns=columns)
    
    # Ensure all DataFrames have all columns
    normalized_dfs = []
    for df in dfs:
        if isinstance(df, dict):
            df = pd.DataFrame([df])
        elif isinstance(df, pd.Series):
            df = pd.DataFrame([df.to_dict()])
        elif isinstance(df, list):
            df = pd.DataFrame(df)
            
        if isinstance(df, pd.DataFrame) and not df.empty:
            # Create a new DataFrame with the desired columns
            new_df = pd.DataFrame(columns=columns)
            
            # Copy over existing columns
            for col in columns:
                if col in df.columns:
                    new_df[col] = df[col]
                else:
                    # Initialize missing columns with empty strings for object dtype
                    # and 0 for numeric columns
                    if col in ['Amount', 'Subtotal', 'Fee', 'Total USD', 'Spot Price']:
                        new_df[col] = 0
                    else:
                        new_df[col] = ''
            
            normalized_dfs.append(new_df)
    
    if not normalized_dfs:
        return pd.DataFrame(columns=columns)
        
    return pd.concat(normalized_dfs, **kwargs)

def load_coinbase_transactions():
    """Load and process Coinbase transaction history"""
    try:
        # Find Coinbase transaction files
        excel_files = glob.glob("*.xlsx")
        transaction_patterns = ['transactions', 'txs', 'transaction', 'tx', 'trans', 'action', 'activity', 'log', 'report', 'history']
        coinbase_files = [
            f for f in excel_files 
            if 'coinbase' in f.lower() 
            and any(pattern in f.lower() for pattern in transaction_patterns)
            and 'pro' not in f.lower()  # Exclude Coinbase Pro files
        ]
        
        if not coinbase_files:
            print("Warning: No Coinbase transaction files found. Skipping Coinbase transactions.")
            return pd.DataFrame()
        
        print(f"  **  Loading Coinbase transactions...\n")
        print(f" {coinbase_files[0]}")
        df = pd.read_excel(coinbase_files[0])
        
        # Clean and convert both columns to numeric, handling currency symbols
        subtotal = pd.to_numeric(
            df['Subtotal'].replace(r'[\$,]', '', regex=True),
            errors='coerce'
        )
        total = pd.to_numeric(
            df['Total (inclusive of fees and/or spread)'].replace(r'[\$,]', '', regex=True),
            errors='coerce'
        )
        
        # Create a mask for where we should use the Total instead of Subtotal
        use_total_mask = (
            subtotal.notna() & 
            total.notna() & 
            (abs(total) < abs(subtotal))
        )
        
        # Where the mask is True, use Total instead of Subtotal
        df.loc[use_total_mask, 'Subtotal'] = df.loc[use_total_mask, 'Total (inclusive of fees and/or spread)']
        
        # Now continue with the normal processing
        df['Source'] = 'Coinbase'
        df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.tz_localize(None)
        df = df.rename(columns={
            'Transaction Type': 'Type',
            'Quantity Transacted': 'Amount',
            'Subtotal': 'Subtotal',
            'Fees and/or Spread': 'Fee',
            'Price at Transaction': 'Spot Price'
        })
        
        # Clean monetary values using Decimal for precision
        df['Amount'] = df['Amount'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else Decimal('0'))
        df['Subtotal'] = df['Subtotal'].replace(r'[\$,]', '', regex=True).apply(lambda x: Decimal(str(x)) if pd.notnull(x) else Decimal('0'))
        df['Fee'] = df['Fee'].replace(r'[\$,]', '', regex=True).apply(lambda x: Decimal(str(x)) if pd.notnull(x) else Decimal('0'))
        df['Spot Price'] = df['Spot Price'].replace(r'[\$,]', '', regex=True).apply(lambda x: Decimal(str(x)) if pd.notnull(x) else Decimal('0'))
        
        # Calculate Total USD before processing Convert transactions
        df['Total USD'] = df.apply(
            lambda row: row['Subtotal'] + (abs(row['Fee']) * (1 if row['Subtotal'] >= 0 else -1)),
            axis=1
        )
        
        # Handle Convert transactions by creating two rows for each
        new_rows = []
        
        for idx, row in df.iterrows():
            if row['Type'] == 'Convert':
                # Parse the Notes field
                # Example: "Converted 21.40733 ALGO to 0.00022474 BTC"
                parts = row['Notes'].split(' ')
                sell_amount = Decimal(str(parts[1]))
                sell_asset = parts[2]
                buy_amount = Decimal(str(parts[4]))
                buy_asset = parts[5]
                
                # Get USD values directly from input file
                total_with_fees = abs(Decimal(str(row['Total (inclusive of fees and/or spread)']).replace('$', '').replace(',', '')))
                subtotal = abs(Decimal(str(row['Subtotal']).replace('$', '').replace(',', '')))
                fee = total_with_fees - subtotal  # Calculate fee as difference
                
                # Update the original row to be the sell transaction
                df.at[idx, 'Type'] = 'Sell'
                df.at[idx, 'Asset'] = sell_asset
                df.at[idx, 'Amount'] = sell_amount
                df.at[idx, 'Fee'] = fee
                df.at[idx, 'Total USD'] = total_with_fees  # Use the total with fees value
                df.at[idx, 'Spot Price'] = total_with_fees / sell_amount
                
                # Create the buy transaction as a new row
                buy_row = row.copy()
                buy_row['Type'] = 'Buy'
                buy_row['Asset'] = buy_asset
                buy_row['Amount'] = buy_amount
                buy_row['Fee'] = Decimal('0')
                buy_row['Total USD'] = total_with_fees  # Use same total with fees value
                buy_row['Spot Price'] = total_with_fees / buy_amount
                new_rows.append(buy_row)
        
        # Add the new buy rows to the DataFrame
        if new_rows:
            df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        
        # Rename "Inflation Reward" to "Staking Income"
        df.loc[df['Type'] == 'Inflation Reward', 'Type'] = 'Staking Income'
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['Amount', 'Total USD', 'Fee', 'Spot Price']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else Decimal('0'))
        
        return df[['ID', 'Timestamp', 'Source', 'Type', 'Asset', 'Amount', 'Subtotal', 'Fee', 'Total USD', 'Spot Price', 'Notes']]
    except Exception as e:
        print(f"Error loading Coinbase transactions: {str(e)}")
        return pd.DataFrame()

def load_coinbase_pro_transactions():
    """Load and process Coinbase Pro transaction history"""
    try:
        dfs = []
        excel_files = glob.glob("*.xlsx")
        
        # Match any combination of fills/fill and coinbasepro/coinbase-pro
        fill_patterns = ['fill', 'fills']
        pro_patterns = ['coinbasepro', 'coinbase-pro']
        pro_files = [
            f for f in excel_files 
            if any(pro in f.lower() for pro in pro_patterns)
            and any(fill in f.lower() for fill in fill_patterns)
        ]
        
        if not pro_files:
            print("Warning: No Coinbase Pro files found. Skipping Coinbase Pro transactions.")
            return pd.DataFrame()
        
        print(f"  **  Loading Coinbase Pro transactions...\n")
        for file in pro_files:
            print(f" {file}")
            df = pd.read_excel(file)
            
            if df.empty:
                continue
            
            if 'portfolio' in df.columns:
                df = df.drop('portfolio', axis=1)
            
            # Add Notes column if it doesn't exist
            if 'Notes' not in df.columns:
                df['Notes'] = ''
            
            df['Source'] = 'Coinbase Pro'
            df['Timestamp'] = pd.to_datetime(df['created at']).dt.tz_localize(None)
            df['Asset'] = df['product'].str.split('-').str[0]
            df['Type'] = df['side']
            
            # Rename columns and convert ID to string
            df = df.rename(columns={
                'trade id': 'ID',
                'size': 'Amount',
                'total': 'Total USD',
                'fee': 'Fee',
                'price': 'Spot Price'
            })
            df['ID'] = df['ID'].astype(str)  # Convert ID to string
            
            # Calculate Subtotal
            df['Subtotal'] = df.apply(
                lambda row: (abs(row['Total USD']) - (abs(row['Fee']))),
                axis=1
            )
            
            dfs.append(df[['ID', 'Timestamp', 'Source', 'Type', 'Asset', 'Amount', 'Subtotal', 'Fee', 'Total USD', 'Spot Price', 'Notes']])
        
        return pd.concat(dfs) if dfs else pd.DataFrame()
        
    except Exception as e:
        print(f"Error loading Coinbase Pro transactions: {str(e)}")
        return pd.DataFrame()

def load_kraken_transactions():
    """Load and process Kraken ledger and trades data"""
    try:
        excel_files = glob.glob("*.xlsx")
        
        # Find ledger file
        ledger_patterns = ['kraken-ledgers', 'kraken_ledgers', 'kraken-ledger', 'kraken_ledger']
        ledger_files = [f for f in excel_files if any(pattern in f.lower() for pattern in ledger_patterns)]
        
        if not ledger_files:
            print("Warning: No Kraken ledger file found. Skipping Kraken transactions.")
            return pd.DataFrame(columns=['ID', 'Timestamp', 'Source', 'Type', 'Asset', 'Amount', 'Total USD', 'Fee', 'Spot Price', 'Notes'])
            
        ledger_file = ledger_files[0]
        print(f"  **  Loading Kraken ledger...\n")
        print(f" {ledger_file}")
        ledger_df = pd.read_excel(ledger_file).reset_index(drop=True)  # Reset index immediately
        
        # Find trades file
        trades_patterns = ['kraken-trades', 'kraken_trades', 'kraken-trade', 'kraken_trade']
        trades_files = [f for f in excel_files if any(pattern in f.lower() for pattern in trades_patterns)]
        
        trades_df = pd.DataFrame()
        if trades_files:
            trades_file = trades_files[0]
            print(f"\n  **  Loading Kraken trades...\n")
            print(f" {trades_file}")
            trades_df = pd.read_excel(trades_file).reset_index(drop=True)  # Reset index immediately
        
        # Create a mapping of txid to ordertxid from trades file
        ordertxid_map = {}
        if not trades_df.empty:
            for _, row in trades_df.iterrows():
                ordertxid_map[row['txid']] = row['ordertxid']
        
        # Create price mapping from trades file
        price_map = {}
        if not trades_df.empty:
            for _, row in trades_df.iterrows():
                # Only map prices for crypto assets traded against USD
                if 'pair' in row and '/USD' in row['pair'] and row['txid']:
                    base_asset = row['pair'].split('/')[0]  # Get the crypto asset part
                    price_map[row['txid']] = {
                        'price': row['price'],
                        'asset': base_asset
                    }
        
        # Process ledger entries
        ledger_df['Timestamp'] = pd.to_datetime(ledger_df['time']).dt.tz_localize(None)
        ledger_df['Source'] = 'Kraken'
        ledger_df['ID'] = ledger_df['txid']
        
        # Remove .S from asset names
        ledger_df['asset'] = ledger_df['asset'].str.split('.').str[0]
        
        # Create empty DataFrame for results with correct columns
        result_columns = ['ID', 'Timestamp', 'Source', 'Type', 'Asset', 'Amount', 'Subtotal', 'Fee', 'Total USD', 'Spot Price', 'Notes']
        all_transactions = pd.DataFrame(columns=result_columns)
        
        # Process trades
        if not ledger_df.empty:
            trades = ledger_df[ledger_df['type'] == 'trade'].copy()
            trades = trades.sort_index()  # Ensure original file order is preserved
            trades['original_index'] = trades.index  # Add original index column
            
            trades_transactions = pd.DataFrame(columns=result_columns + ['original_index'])
            
            for refid, pair in trades.groupby('refid', sort=False):
                if len(pair) == 2:
                    # Get the rows in original file order - Kraken always lists SELL first
                    sell_row = pair.iloc[0]  # First row is always the SELL
                    buy_row = pair.iloc[1]   # Second row is always the BUY
                    
                    # Verify this assumption - if not true, we need to swap
                    if sell_row['amount'] > 0:  # If first row is actually a buy
                        sell_row, buy_row = buy_row, sell_row  # swap them
                    
                    # Get ordertxid and set up notes
                    ordertxid = ordertxid_map.get(refid, '')
                    notes = f"Order: {ordertxid}" if ordertxid else ""
                    notes += f" ~~ Trade: {refid}" if notes else f"Trade: {refid}"
                    
                    # Check if this is a USD pair trade
                    is_usd_pair = 'USD' in [sell_row['asset'], buy_row['asset']]
                    
                    if is_usd_pair:
                        # For USD pairs, use the actual USD values directly
                        if sell_row['asset'] == 'USD':
                            # USD is being sold (buying crypto)
                            total_usd = abs(sell_row['amount'])  # Total USD is what's being sold
                            spot_price = total_usd / abs(buy_row['amount'])
                            fee_in_usd = abs(buy_row['fee']) * spot_price if buy_row['fee'] != 0 else abs(sell_row['fee'])
                            base_subtotal = total_usd - fee_in_usd  # Subtotal is Total minus fee
                            
                            # For USD pairs, both sides share the same fee
                            sell_fee_in_usd = fee_in_usd
                            buy_fee_in_usd = fee_in_usd
                        else:
                            # USD is being bought (selling crypto)
                            total_usd = abs(buy_row['amount'])  # Total USD is what's being bought
                            spot_price = total_usd / abs(sell_row['amount'])
                            fee_in_usd = abs(sell_row['fee']) * spot_price if sell_row['fee'] != 0 else abs(buy_row['fee'])
                            base_subtotal = total_usd - fee_in_usd  # Subtotal is Total minus fee
                            
                            # For USD pairs, both sides share the same fee
                            sell_fee_in_usd = fee_in_usd
                            buy_fee_in_usd = fee_in_usd
                    else:
                        # For non-USD pairs, calculate fees separately for each side
                        base_row = sell_row if abs(sell_row['amountusd']) > abs(buy_row['amountusd']) else buy_row
                        total_usd = abs(base_row['amountusd'])  # amountusd is pre-fee
                        
                        # Calculate spot prices for both sides
                        sell_spot_price = total_usd / abs(sell_row['amount'])
                        buy_spot_price = total_usd / abs(buy_row['amount'])
                        
                        # Calculate fees separately for each side
                        sell_fee_in_usd = abs(sell_row['fee']) * sell_spot_price if sell_row['fee'] != 0 else 0
                        buy_fee_in_usd = abs(buy_row['fee']) * buy_spot_price if buy_row['fee'] != 0 else 0
                        
                        # For non-USD pairs, subtotal is total minus all fees to balance both sides
                        base_subtotal = total_usd - sell_fee_in_usd - buy_fee_in_usd

                    transactions = [
                        # SELL transaction
                        {
                            'ID': sell_row['txid'],
                            'Timestamp': sell_row['Timestamp'],
                            'Source': 'Kraken',
                            'Type': 'Sell',
                            'Asset': sell_row['asset'],
                            'Amount': abs(sell_row['amount']),
                            'Subtotal': base_subtotal,  # Same subtotal for both sides
                            'Fee': sell_fee_in_usd,  # Use sell-side fee
                            'Total USD': total_usd,
                            'Spot Price': total_usd / abs(sell_row['amount']),
                            'Notes': f"{notes} ~~ Base Asset: {base_asset}" if not is_usd_pair else notes
                        },
                        # BUY transaction
                        {
                            'ID': buy_row['txid'],
                            'Timestamp': buy_row['Timestamp'],
                            'Source': 'Kraken',
                            'Type': 'Buy',
                            'Asset': buy_row['asset'],
                            'Amount': abs(buy_row['amount']),
                            'Subtotal': base_subtotal,  # Same subtotal for both sides
                            'Fee': buy_fee_in_usd,  # Use buy-side fee
                            'Total USD': total_usd,
                            'Spot Price': total_usd / abs(buy_row['amount']),
                            'Notes': f"{notes} ~~ Base Asset: {base_asset}" if not is_usd_pair else notes
                        }
                    ]
                    
                    # Add original_index to transactions
                    transactions[0]['original_index'] = pair.iloc[0]['original_index']
                    transactions[1]['original_index'] = pair.iloc[1]['original_index']
                    
                    trades_transactions = safe_concat([trades_transactions, transactions], 
                                                   columns=result_columns + ['original_index'],
                                                   ignore_index=False)
            
            # Sort and drop original_index only for trades
            if not trades_transactions.empty:
                trades_transactions = trades_transactions.sort_values('original_index')
                trades_transactions = trades_transactions.drop('original_index', axis=1)
                all_transactions = safe_concat([all_transactions, trades_transactions], 
                                            columns=result_columns,
                                            ignore_index=True)
        
        # Process non-trade entries
        non_trades = ledger_df[ledger_df['type'] != 'trade'].copy()
        
        # Filter transfers
        non_trades = non_trades[
            (non_trades['type'] != 'transfer') | 
            ((non_trades['type'] == 'transfer') & (non_trades['subtype'] == 'spotfromfutures'))
        ]
        
        # Rename special types
        non_trades.loc[
            (non_trades['type'] == 'transfer') & 
            (non_trades['subtype'] == 'spotfromfutures'), 
            'type'
        ] = 'Staking Income'
        
        non_trades.loc[non_trades['type'] == 'staking', 'type'] = 'Staking Income'
        
        if not non_trades.empty:
            non_trades_formatted = pd.DataFrame({
                'ID': non_trades['txid'],
                'Timestamp': non_trades['Timestamp'],
                'Source': 'Kraken',
                'Type': non_trades['type'],
                'Asset': non_trades['asset'],
                'Amount': non_trades['amount'].abs(),
                'Subtotal': non_trades['amountusd'].abs(),
                'Fee': non_trades['fee'],
                'Total USD': non_trades['amountusd'].abs() + non_trades['fee'],
                'Spot Price': 0,
                'Notes': ''
            })
            
            all_transactions = safe_concat([all_transactions, non_trades_formatted], 
                                        columns=result_columns,
                                        ignore_index=True)
        
        # When creating trade entries, add ordertxid to Notes
        if not trades_df.empty:
            for idx, row in trades_df.iterrows():
                if row['ordertxid']:
                    if 'Notes' not in trades_df.columns:
                        trades_df['Notes'] = ''
                    trades_df.loc[idx, 'Notes'] = f"Order: {row['ordertxid']} " + str(trades_df.loc[idx, 'Notes'])
        
        return all_transactions[result_columns]
        
    except Exception as e:
        print(f"Error loading Kraken transactions: {str(e)}")
        return pd.DataFrame(columns=['ID', 'Timestamp', 'Source', 'Type', 'Asset', 'Amount', 'Total USD', 'Fee', 'Spot Price', 'Notes'])

def load_strike_transactions():
    """Load and process Strike transaction history"""
    try:
        # Find Strike transaction files
        excel_files = glob.glob("*.xlsx")
        transaction_patterns = ['transactions', 'txs', 'transaction', 'tx', 'trans', 'action', 'activity', 'log', 'report', 'history']
        strike_files = [
            f for f in excel_files 
            if 'strike' in f.lower() 
            and any(pattern in f.lower() for pattern in transaction_patterns)
        ]
        
        if not strike_files:
            print("Warning: No Strike transaction files found. Skipping Strike transactions.")
            return pd.DataFrame()
            
        dfs = []
        
        print(f"  **  Loading Strike transactions...\n")
        for file in strike_files:
            print(f" {file}")
            df = pd.read_excel(file)
            
            if df.empty:
                continue
                
            # Filter rows based on Transaction Type and State
            df = df[
                (df['Transaction Type'].isin(['Trade', 'Withdrawal'])) & 
                (df['State'] == 'Completed')
            ]
            
            # Create timestamp from date and time columns
            df['Timestamp'] = pd.to_datetime(
                df['Completed Date (UTC)'] + ' ' + df['Completed Time (UTC)']
            ).dt.tz_localize(None)
            
            # Set Source
            df['Source'] = 'Strike'
            
            # Determine Transaction Type
            def get_transaction_type(row):
                if row['Transaction Type'] == 'Withdrawal':
                    return 'Send'
                elif (row['Transaction Type'] == 'Trade' and 
                      row['Amount 1'] < 0 and 
                      row['Currency 1'] == 'USD' and 
                      row['Currency 2'] == 'BTC'):
                    return 'Buy'
                elif (row['Transaction Type'] == 'Trade' and 
                      ((row['Currency 1'] == 'BTC' and row['Currency 2'] == 'USD') or
                       (row['Currency 1'] == 'USD' and row['Currency 2'] == 'BTC' and 
                        row['Amount 1'] > 0 and row['Amount 2'] < 0))):
                    return 'Sell'
                return 'Unknown'
            
            df['Type'] = df.apply(get_transaction_type, axis=1)
            
            # Determine Asset
            df['Asset'] = df.apply(
                lambda row: row['Currency 1'] if row['Currency 2'] == 'USD' else row['Currency 2'],
                axis=1
            )
            
            # Map other fields
            df['ID'] = df['Transaction ID']
            df['Amount'] = pd.to_numeric(df['Amount 2'], errors='coerce').fillna(0)
            df['Total USD'] = pd.to_numeric(df['Amount 1'], errors='coerce').fillna(0)
            
            # Handle fees - replace NaN with 0 and convert to numeric
            df['Fee 1'] = pd.to_numeric(df['Fee 1'], errors='coerce').fillna(0)
            df['Fee 2'] = pd.to_numeric(df['Fee 2'], errors='coerce').fillna(0)
            df['Fee'] = df['Fee 1'].abs() + df['Fee 2'].abs()
            
            # Handle Spot Price
            df['Spot Price'] = pd.to_numeric(df['BTC Price'], errors='coerce').fillna(0)
            
            # Calculate Subtotal
            df['Subtotal'] = df['Total USD'].abs() - df['Fee']
            
            # Add empty Notes column
            df['Notes'] = ''
            
            # Clean up any remaining NaN values
            numeric_columns = ['Amount', 'Total USD', 'Fee', 'Spot Price', 'Subtotal']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Select and reorder columns
            result_df = df[[
                'ID', 'Timestamp', 'Source', 'Type', 'Asset', 'Amount',
                'Subtotal', 'Fee', 'Total USD', 'Spot Price', 'Notes'
            ]]
            
            dfs.append(result_df)
        
        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)

            return final_df
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error loading Strike transactions: {str(e)}")
        return pd.DataFrame()

def load_cashapp_transactions():
    """Load and process CashApp transaction history"""
    try:
        # Find CashApp transaction files
        excel_files = glob.glob("*.xlsx")
        transaction_patterns = ['transactions', 'txs', 'transaction', 'tx', 'trans', 'action', 'activity', 'log', 'report', 'history']
        cashapp_files = [
            f for f in excel_files 
            if any(f.lower().startswith(p) for p in ['cash_app', 'cashapp'])
            and any(pattern in f.lower() for pattern in transaction_patterns)
        ]
        
        if not cashapp_files:
            print("Warning: No CashApp files found. Skipping CashApp transactions.")
            return pd.DataFrame()
            
        dfs = []
        transaction_counter = 1  # For generating IDs
        
        for file in cashapp_files:
            print(f"  **  Loading CashApp transactions...\n")
            print(f" {file}")
            df = pd.read_excel(file)
            
            if df.empty:
                continue
                
            # Filter rows based on Transaction Type and Status
            df = df[
                (df['Transaction Type'].str.startswith('Bitcoin', na=False)) & 
                (df['Status'] == 'COMPLETE')
            ]
            
            if df.empty:
                continue
            
            # Create timestamp from Date column - handle various timezone formats
            def parse_timestamp(date_str):
                try:
                    # Split into date/time and timezone parts
                    parts = date_str.rsplit(' ', 1)  # Split from right side once
                    if len(parts) == 2:
                        datetime_str, tz = parts
                        # Parse the datetime string
                        dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                        
                        # Adjust for timezone
                        if tz == 'CDT':
                            dt = dt - timedelta(hours=5)  # CDT is UTC-5
                        elif tz == 'CST':
                            dt = dt - timedelta(hours=6)  # CST is UTC-6
                        
                        return dt
                    return pd.to_datetime(date_str)
                except Exception as e:
                    print(f"Error parsing date '{date_str}': {str(e)}")
                    return None

            # Convert dates and handle timezone
            df['Timestamp'] = df['Date'].apply(lambda x: parse_timestamp(str(x)))
            
            # Drop rows with invalid timestamps
            invalid_timestamps = df['Timestamp'].isna()
            if invalid_timestamps.any():
                print(f"Warning: Dropping {invalid_timestamps.sum()} rows with invalid timestamps")
                df = df.dropna(subset=['Timestamp'])
            
            # Set Source
            df['Source'] = 'CashApp'
            
            # Map Transaction Types
            type_mapping = {
                'Bitcoin Buy': 'Buy',
                'Bitcoin Sell': 'Sell',
                'Bitcoin Withdrawal': 'Send',
                'Bitcoin Send': 'Send'
            }
            df['Type'] = df['Transaction Type'].map(type_mapping)
            
            # Generate IDs for missing Transaction IDs
            next_id = transaction_counter
            def generate_id(x):
                nonlocal next_id
                if pd.notna(x):
                    return x
                else:
                    current_id = f'cashapp-{next_id:06d}'
                    next_id += 1
                    return current_id
                    
            df['ID'] = df['Transaction ID'].apply(generate_id)
            # Update counter for next file
            transaction_counter = next_id
            
            # Map other fields
            df['Asset'] = df['Asset Type']
            df['Spot Price'] = pd.to_numeric(df['Asset Price'], errors='coerce')
            df['Fee'] = pd.to_numeric(df['Fee'], errors='coerce').fillna(0).abs()
            
            # Handle Amount, Total USD, and Subtotal based on transaction type
            df['Amount'] = df.apply(
                lambda row: (
                    pd.to_numeric(row['Asset Amount'], errors='coerce')
                    if row['Type'] in ['Buy', 'Sell']
                    else pd.to_numeric(row['Amount'], errors='coerce')
                ),
                axis=1
            ).fillna(0)
            
            df['Total USD'] = df.apply(
                lambda row: (
                    pd.to_numeric(row['Net Amount'], errors='coerce')
                    if row['Type'] in ['Buy', 'Sell']
                    else pd.to_numeric(row['Amount'], errors='coerce')
                ),
                axis=1
            ).fillna(0)
            
            # For Buy/Sell, Subtotal is Total USD minus Fee
            df['Subtotal'] = df.apply(
                lambda row: (
                    row['Total USD'] - row['Fee']
                    if row['Type'] in ['Buy', 'Sell']
                    else 0
                ),
                axis=1
            )
            
            # Copy Notes
            df['Notes'] = df['Notes'].fillna('')
            
            # Clean up numeric columns
            numeric_columns = ['Amount', 'Total USD', 'Fee', 'Spot Price', 'Subtotal']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Select and reorder columns
            result_df = df[[
                'ID', 'Timestamp', 'Source', 'Type', 'Asset', 'Amount',
                'Subtotal', 'Fee', 'Total USD', 'Spot Price', 'Notes'
            ]]
            
            dfs.append(result_df)
        
        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)

            return final_df
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error loading CashApp transactions: {str(e)}")
        return pd.DataFrame()

def clean_price_string(value):
    """Convert price strings to numeric values, handling dollar signs and commas"""
    if pd.isna(value):
        return 0
    try:
        # If it's already a number, return it
        if isinstance(value, (int, float)):
            return float(value)
        # Clean string: remove $, commas, and whitespace
        cleaned = str(value).replace('$', '').replace(',', '').strip()
        return float(cleaned) if cleaned else 0
    except:
        return 0

def standardize_transaction_values(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize signs of Amount and Subtotal based on transaction type"""
    try:
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Convert numeric columns
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
        df['Subtotal'] = pd.to_numeric(df['Subtotal'], errors='coerce')
        df['Fee'] = pd.to_numeric(df['Fee'], errors='coerce')
        
        # Ensure all fees are positive
        df['Fee'] = df['Fee'].abs()
        
        # Convert Type to lowercase for consistent comparison
        df['Type_lower'] = df['Type'].str.lower()
        
        print("\n\nUnique transaction types:", df['Type_lower'].unique())
        
        # Filter out Exchange Withdrawal rows
        df = df[~df['Type_lower'].str.contains('exchange withdrawal', na=False)]
        
        # Define transaction type patterns
        sell_patterns = ['sell', 'send', 'convert']
        buy_patterns = ['buy', 'receive', 'reward', 'deposit', 'staking', 'dividend']
        
        # Create masks for different transaction types - with error checking
        def safe_pattern_check(x, patterns):
            try:
                if pd.isna(x):
                    return False
                return any(pattern in str(x).lower() for pattern in patterns)
            except Exception as e:
                print(f"Error in pattern check for value '{x}': {str(e)}")
                return False
        
        df['is_sell'] = df['Type_lower'].apply(lambda x: safe_pattern_check(x, sell_patterns))
        df['is_buy'] = df['Type_lower'].apply(lambda x: safe_pattern_check(x, buy_patterns))
        df['is_admin_debit'] = df['Type_lower'].str.contains('admin debit', na=False)
        df['is_withdrawal'] = df['Type_lower'].str.contains('withdrawal', na=False)
        
        print("\nTransaction count...")
        # Print mask counts for debugging
        print(f"   Sell transactions: {df['is_sell'].sum()}")
        print(f"   Buy transactions: {df['is_buy'].sum()}")
        print(f"   Admin debit transactions: {df['is_admin_debit'].sum()}")
        print(f"   Withdrawal transactions: {df['is_withdrawal'].sum()}")
        
        # Apply standardization using masks
        if df['is_sell'].any():
            df.loc[df['is_sell'], 'Amount'] = -df.loc[df['is_sell'], 'Amount'].abs()
            df.loc[df['is_sell'], 'Subtotal'] = -df.loc[df['is_sell'], 'Subtotal'].abs()
        
        if df['is_buy'].any():
            df.loc[df['is_buy'], 'Amount'] = df.loc[df['is_buy'], 'Amount'].abs()
            df.loc[df['is_buy'], 'Subtotal'] = df.loc[df['is_buy'], 'Subtotal'].abs()
        
        if df['is_admin_debit'].any():
            df.loc[df['is_admin_debit'], 'Amount'] = -df.loc[df['is_admin_debit'], 'Amount'].abs()
            df.loc[df['is_admin_debit'], 'Subtotal'] = -df.loc[df['is_admin_debit'], 'Subtotal'].abs()
        
        if df['is_withdrawal'].any():
            df.loc[df['is_withdrawal'], 'Amount'] = -df.loc[df['is_withdrawal'], 'Amount'].abs()
            df.loc[df['is_withdrawal'], 'Subtotal'] = 0
        
        print("\nFilling in the missing blanks...")
        # Calculate Total USD based on Subtotal and Fee
        df['Total USD'] = df.apply(
            lambda row: row['Subtotal'] + (abs(row['Fee']) * (1 if row['Subtotal'] >= 0 else -1)),
            axis=1
        )
        
        # Drop temporary columns
        df = df.drop(['Type_lower', 'is_sell', 'is_buy', 'is_admin_debit', 'is_withdrawal'], axis=1)
        
        return df
        
    except Exception as e:
        print(f"Error in standardize_transaction_values: {str(e)}")
        print(f"Error occurred at line: {e.__traceback__.tb_lineno}")
        raise

def process_kraken_groups(asset_df: pd.DataFrame) -> pd.DataFrame:
    """Pre-process Kraken transactions by grouping them by ordertxid.
    
    Args:
        asset_df: DataFrame containing transactions for a single asset
        
    Returns:
        DataFrame with Kraken transactions properly grouped and other transactions unchanged
    """
    # First, pre-group Kraken transactions by ordertxid
    kraken_groups = {}
    non_kraken_rows = []
    
    for idx, row in asset_df.iterrows():
        if (row['Source'] == 'Kraken' and 'Notes' in row and 
            isinstance(row['Notes'], str) and 'Order:' in row['Notes']):
            ordertxid = row['Notes'].split('Order:')[1].split()[0]
            if ordertxid not in kraken_groups:
                kraken_groups[ordertxid] = []
            kraken_groups[ordertxid].append(row)
        else:
            non_kraken_rows.append(row)
    
    # Process Kraken groups first
    for ordertxid, group in kraken_groups.items():
        if len(group) > 1:
            # Sort group by type_order first
            group = sorted(group, key=lambda x: x['type_order'])
            # Verify these transactions should be merged
            first_tx = group[0]
            if all(should_merge_transactions(first_tx.to_dict(), tx.to_dict()) 
                  for tx in group[1:]):
                combined = combine_transactions(pd.DataFrame(group))
                non_kraken_rows.append(combined)
            else:
                non_kraken_rows.extend(group)
        else:
            non_kraken_rows.extend(group)
    
    # Convert back to DataFrame and sort
    return pd.DataFrame(non_kraken_rows).sort_values(['Timestamp', 'type_order'])

def assign_lot_ids_and_group(df: pd.DataFrame, time_window_seconds: int = 90) -> pd.DataFrame:
    """Group similar transactions by source & type within time windows, assign lot IDs chronologically."""
    df = df.copy()
    df['Lot ID'] = ''
    
    # Define receiving types
    receiving_types = [
        'buy',
        'advanced trade buy',
        'receive',
        'learning reward',
        'staking income',
        'dividend',
        'convert'
    ]
    
    # First, handle all grouping
    result_dfs = []
    
    for asset in df['Asset'].unique():
        if asset == 'USD':
            continue
            
        asset_df = df[df['Asset'] == asset].copy()
        
        # Add type ordering for same-timestamp transactions
        asset_df['type_order'] = asset_df['Type'].str.lower().map({'sell': 0, 'buy': 1}).fillna(2)
        
        # Process Kraken transactions first
        asset_df = process_kraken_groups(asset_df)
        asset_df = asset_df.drop('type_order', axis=1)
        
        # Initialize variables for grouping
        current_group = []
        all_groups = []
        
        # Group transactions within time window
        for idx, row in asset_df.iterrows():
            if not current_group:
                current_group = [row]
            else:
                if should_merge_transactions(current_group[0].to_dict(), row.to_dict(), time_window_seconds):
                    current_group.append(row)
                else:
                    if len(current_group) > 1:
                        # Sort group by type before combining (sell first)
                        current_group = sorted(current_group, key=lambda x: 0 if x['Type'].lower() == 'sell' else 1)
                        all_groups.append(combine_transactions(pd.DataFrame(current_group)))
                    else:
                        all_groups.append(current_group[0])
                    current_group = [row]
        
        # Handle last group
        if current_group:
            if len(current_group) > 1:
                # Sort last group by type before combining (sell first)
                current_group = sorted(current_group, key=lambda x: 0 if x['Type'].lower() == 'sell' else 1)
                all_groups.append(combine_transactions(pd.DataFrame(current_group)))
            else:
                all_groups.append(current_group[0])
        
        if all_groups:
            result_dfs.append(pd.DataFrame(all_groups))
    
    # Combine all results
    if not result_dfs:
        return df
        
    # Combine and sort all transactions
    result = pd.concat(result_dfs, ignore_index=True)
    
    # Add type_order for final sorting
    result['type_order'] = result['Type'].str.lower().map({'sell': 0, 'buy': 1}).fillna(2)
    result = result.sort_values(['Timestamp', 'type_order']).reset_index(drop=True)
    result = result.drop('type_order', axis=1)
    
    # NOW assign lot IDs in a separate pass
    for asset in result['Asset'].unique():
        if asset == 'USD':
            continue
            
        # Get asset transactions
        asset_mask = result['Asset'] == asset
        
        # Assign lot IDs sequentially for receiving transactions only
        lot_counter = 1
        for idx in result[asset_mask].index:
            tx_type = result.loc[idx, 'Type'].lower()
            # Strict check: must be in receiving_types AND not contain 'sell'
            if tx_type in receiving_types and 'sell' not in tx_type:
                result.loc[idx, 'Lot ID'] = f"{asset}-{lot_counter:05d}"
                lot_counter += 1
    
    return result

def get_decimal_places(value_str: str) -> int:
    """Get the number of decimal places in a number string"""
    try:
        value_str = str(value_str)
        if '.' in value_str:
            return len(value_str.split('.')[1])
        return 0
    except:
        return 8  # Default to 8 decimal places if we can't determine

def normalize_amount(amount, precision=None):
    """Convert amount to Decimal with appropriate precision"""
    if amount is None or pd.isna(amount):
        return Decimal('0')
    
    # Convert to string first to preserve original precision
    amount_str = str(amount)
    
    # Determine precision if not specified
    if precision is None:
        precision = get_decimal_places(amount_str)
    
    # Create Decimal with specific precision
    d = Decimal(amount_str)
    return d.quantize(Decimal('0.' + '0' * precision), rounding=ROUND_HALF_UP)

def combine_transactions(group: pd.DataFrame) -> pd.Series:
    """Combine multiple transactions into a single transaction"""
    # Sort by timestamp to use earliest time
    group = group.sort_values('Timestamp')
    
    # Determine precision from the Amount column
    max_precision = max(
        get_decimal_places(str(amount))
        for amount in group['Amount']
        if pd.notna(amount)
    )
    
    # Convert amounts to Decimal and sum
    amount_sum = sum(
        normalize_amount(amount, max_precision)
        for amount in group['Amount']
        if pd.notna(amount)
    )
    
    # Calculate weighted average for spot price
    non_zero_mask = group['Spot Price'] > 0
    if non_zero_mask.any():
        valid_amounts = [normalize_amount(amt, max_precision) for amt in group.loc[non_zero_mask, 'Amount'].abs()]
        valid_prices = [normalize_amount(price, max_precision) for price in group.loc[non_zero_mask, 'Spot Price']]
        weighted_spot_price = sum(p * a for p, a in zip(valid_prices, valid_amounts)) / sum(valid_amounts)
    else:
        weighted_spot_price = Decimal('0')
    
    # Sum other numeric values with proper precision
    subtotal_sum = sum(normalize_amount(sub, max_precision) for sub in group['Subtotal'] if pd.notna(sub))
    fee_sum = sum(normalize_amount(fee, max_precision) for fee in group['Fee'] if pd.notna(fee))
    total_usd_sum = sum(normalize_amount(total, max_precision) for total in group['Total USD'] if pd.notna(total))
    
    # Organize Notes for Kraken transactions
    if group['Source'].iloc[0] == 'Kraken' and any('Order:' in str(note) for note in group['Notes']):
        # Extract ordertxid (should be the same for all rows in group)
        ordertxid = next(note.split('Order:')[1].split()[0] 
                        for note in group['Notes'] if 'Order:' in str(note))
        
        # Convert IDs to strings before joining
        trade_ids = ' | '.join(str(id_) for id_ in group['ID'])
        
        # Combine notes in order: Order, Trades, Group info
        combined_notes = f"From Order: {ordertxid} ~~ Grouped {len(group)} Trades: {trade_ids} ~~ Tx IDs: {' | '.join(str(id_) for id_ in group['ID'])}"
    else:
        # For non-Kraken transactions, preserve original notes
        original_notes = [note for note in group['Notes'] if note]
        grouped_note = f"Grouped {len(group)} Tx IDs: {' | '.join(str(id_) for id_ in group['ID'])}"
        combined_notes = ' ~~ '.join(filter(None, original_notes + [grouped_note]))
    
    # Create combined transaction
    combined = {
        'ID': f"{str(group['ID'].iloc[0])}_grouped",
        'Timestamp': group['Timestamp'].iloc[0],
        'Source': group['Source'].iloc[0],
        'Type': group['Type'].iloc[0],
        'Asset': group['Asset'].iloc[0],
        'Amount': float(amount_sum),
        'Subtotal': float(subtotal_sum),
        'Fee': float(fee_sum),
        'Total USD': float(total_usd_sum),
        'Spot Price': float(weighted_spot_price),
        'Notes': combined_notes
    }
    
    return pd.Series(combined)

def get_historical_price(asset: str, timestamp: datetime) -> float:
    """Get historical price from cache or CryptoCompare API with comprehensive error handling"""
    global price_cache
    
    # Format date for display
    date_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    # Handle LUNA/LUNA2 transition
    display_name = asset
    if asset == 'LUNA':
        # After transition date, original LUNA became LUNC
        if timestamp >= LUNA_TRANSITION_DATE:
            asset = 'LUNC'
            display_name = 'LUNA (AKA: LUNC)'
    elif asset == 'LUNA2':
        # After transition date, LUNA2 price data is under LUNA
        if timestamp >= LUNA_TRANSITION_DATE:
            asset = 'LUNA'
            display_name = 'LUNA2 (AKA: LUNA)'
    
    print(f"\nLooking up price for {display_name} at {date_str}")
    
    # Determine if we can use minute-level data
    use_minute_data = CRYPTOCOMPARE_API_KEY and CRYPTOCOMPARE_API_KEY != 'YOUR-API-KEY-HERE'
    
    # Round timestamp based on data granularity
    if use_minute_data:
        rounded_timestamp = timestamp.replace(second=0, microsecond=0)
        cache_key = f"{asset}_{rounded_timestamp.strftime('%Y-%m-%d_%H-%M')}"
    else:
        rounded_timestamp = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        cache_key = f"{asset}_{rounded_timestamp.strftime('%Y-%m-%d')}"
    
    no_price_key = f"no_price_{asset}_{rounded_timestamp.strftime('%Y-%m-%d')}"
    
    # Check regular price cache first
    if cache_key in price_cache:
        cached_data = price_cache[cache_key]
        if isinstance(cached_data, dict) and 'price' in cached_data:
            # If cache entry has timestamp, check if it's recent
            if 'timestamp' in cached_data:
                cache_age = datetime.now() - datetime.fromtimestamp(cached_data['timestamp'])
                if cache_age.days < 1:  # Cache valid for 24 hours
                    print(f"Found cached price for {display_name}: ${cached_data['price']:.8f}")
                    return float(cached_data['price'])
            else:
                # Legacy cache format - just return the price
                print(f"Found legacy cached price for {display_name}: ${float(cached_data['price']):.8f}")
                return float(cached_data['price'])
        elif isinstance(cached_data, (int, float)):
            # Direct price storage format
            print(f"Found simple cached price for {display_name}: ${float(cached_data):.8f}")
            return float(cached_data)
    
    # Check no-price cache
    if no_price_key in price_cache:
        cached_data = price_cache[no_price_key]
        if isinstance(cached_data, dict) and 'timestamp' in cached_data:
            cache_age = datetime.now() - datetime.fromtimestamp(cached_data['timestamp'])
            if cache_age.total_seconds() < 43200:  # No-price cache valid for 12 hours
                print(f"No price available for {display_name} (cached response)")
                return 0
        # If cache format is invalid or expired, remove it
        del price_cache[no_price_key]
    
    print(f"Fetching price from API for {display_name} at {date_str}...")
    
    try:
        # Convert timestamp to UNIX timestamp
        unix_time = int(rounded_timestamp.timestamp())
        
        # Try minute data first if available
        if use_minute_data and (datetime.now() - rounded_timestamp).days <= 7:
            url = f"https://min-api.cryptocompare.com/data/v2/histominute"
            params = {
                'fsym': asset,
                'tsym': 'USD',
                'limit': 1,
                'toTs': unix_time
            }
            headers = {'authorization': CRYPTOCOMPARE_API_KEY}
        else:
            # Fall back to daily data
            url = f"https://min-api.cryptocompare.com/data/v2/histoday"
            params = {
                'fsym': asset,
                'tsym': 'USD',
                'limit': 1,
                'toTs': unix_time
            }
            headers = {}
        
        # Add delay to avoid rate limiting
        time.sleep(0.25)
        
        # Make API request
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        
        if response.status_code != 200:
            error_msg = f"API Error ({response.status_code}): {data.get('Message', 'Unknown error')}"
            logging.error(error_msg)
            print(error_msg)  # Also print for immediate visibility
            return 0
        
        # Try to get price from response
        if data.get('Response') == 'Success' and data.get('Data', {}).get('Data'):
            close_price = data['Data']['Data'][0].get('close', 0)
            
            # If no price found, try previous day
            if close_price == 0 and not use_minute_data:
                yesterday = rounded_timestamp - timedelta(days=1)
                unix_time = int(yesterday.timestamp())
                params['toTs'] = unix_time
                
                time.sleep(0.25)  # Add delay for second request
                response = requests.get(url, params=params, headers=headers)
                data = response.json()
                
                if response.status_code == 200 and data.get('Response') == 'Success' and data.get('Data', {}).get('Data'):
                    close_price = data['Data']['Data'][0].get('close', 0)
            
            # Cache result if we found a price
            if close_price > 0:
                print(f"Found price for {display_name}: ${close_price:.8f}")
                price_cache[cache_key] = {
                    'price': float(close_price),
                    'timestamp': int(datetime.now().timestamp())
                }
                save_price_cache(price_cache)
                return float(close_price)
        
        # If we get here, no price was found
        msg = f"No price data found for {display_name} at {rounded_timestamp}"
        logging.warning(msg)
        print(msg)
        
        # Cache the no-price result
        price_cache[no_price_key] = {
            'timestamp': int(datetime.now().timestamp())
        }
        save_price_cache(price_cache)
        return 0
        
    except requests.exceptions.RequestException as e:
        msg = f"Network error fetching price for {display_name}: {str(e)}"
        logging.error(msg)
        print(msg)
        return 0
        
    except Exception as e:
        msg = f"Unexpected error getting price for {display_name}: {str(e)}"
        logging.error(msg)
        print(msg)
        return 0

def strip_trailing_zeros(num) -> str:
    """Convert number to string, removing trailing zeros after decimal while preserving significant digits"""
    try:
        # Convert input to float first if it's a string
        if isinstance(num, str):
            num = float(num)

        # Convert to string with high precision to preserve all significant digits
        str_val = f"{float(num):.10f}"

        # Remove trailing zeros after decimal point, but keep decimal if whole number
        return str_val.rstrip('0').rstrip('.') + ('0' if str_val.endswith('.0') else '')

    except Exception as e:
        print(f"Error in strip_trailing_zeros for value '{num}': {str(e)}")
        return str(num)  # Return original value as string if conversion fails

def update_missing_spot_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Update missing spot prices using API and recalculate missing subtotals"""
    df = df.copy()
    
    # Find rows with missing spot prices
    missing_prices = (df['Spot Price'] == 0) & (df['Asset'] != 'USD')
    missing_count = missing_prices.sum()
    
    if missing_count > 0:
        print(f"\nFetching {missing_count} missing spot prices...")
        
        # Track progress
        current = 0
        for idx, row in df[missing_prices].iterrows():
            current += 1
            
            if row['Asset'] != 'USD':
                # Store original print function
                old_print = print
                printed_messages = []
                
                # Create temporary print function to capture output
                def temp_print(*args, **kwargs):
                    message = ' '.join(str(arg) for arg in args)
                    printed_messages.append(message)
                
                # Replace print function
                globals()['print'] = temp_print
                
                # Get the price
                price = get_historical_price(row['Asset'], row['Timestamp'])
                
                # Restore print function
                globals()['print'] = old_print
                
                # Determine status based on captured messages
                if price > 0:
                    if any('Found cached price' in msg or 'Found legacy cached price' in msg or 'Found simple cached price' in msg for msg in printed_messages):
                        status = "CACHED SUCCESS"
                    elif any('Found price for' in msg for msg in printed_messages):
                        status = "FETCH SUCCESS"
                    else:
                        status = "SUCCESS"
                else:
                    status = "FAILED"
                
                # Format price string with significant digits
                if price == 0:
                    price_str = "$0.00"
                else:
                    # Strip trailing zeros but preserve significant digits
                    price_str = f"${strip_trailing_zeros(price)}"
                
                # Print single summary line
                print(f"Processing {current}/{missing_count}: {row['Asset']} at {row['Timestamp']} - {status} - Price: {price_str}")
                
                if price > 0:
                    # Store the price with full precision but stripped of trailing zeros
                    df.loc[idx, 'Spot Price'] = float(strip_trailing_zeros(price))
    
    # After updating spot prices, recalculate missing subtotals
    missing_subtotals = (
        (df['Subtotal'] == 0) & 
        (df['Amount'].fillna(0) != 0) & 
        (df['Spot Price'].fillna(0) != 0)
    )
    
    if missing_subtotals.any():
        print(f"\nRecalculating {missing_subtotals.sum()} missing subtotals...")
        df.loc[missing_subtotals, 'Subtotal'] = (
            df.loc[missing_subtotals, 'Amount'] * 
            df.loc[missing_subtotals, 'Spot Price']
        )
        
        # Recalculate Total USD for rows with updated subtotals
        df.loc[missing_subtotals, 'Total USD'] = df.loc[missing_subtotals].apply(
            lambda row: row['Subtotal'] + (abs(row['Fee']) * (1 if row['Subtotal'] >= 0 else -1)),
            axis=1
        )
    
    return df

def merge_all_transactions():
    """Merge all transaction data into one master sheet"""
    try:
        print("\n\nStarting merge process...\n")
        
        # Load all transaction data
        coinbase_df = load_coinbase_transactions()
        print(f"")
        coinbase_pro_df = load_coinbase_pro_transactions()
        print(f"")
        kraken_df = load_kraken_transactions()
        print(f"")
        strike_df = load_strike_transactions()
        print(f"")
        cashapp_df = load_cashapp_transactions()
        
        # Ensure all DataFrames have the required columns and reset indices
        required_columns = [
            'ID', 'Timestamp', 'Source', 'Type', 'Asset', 'Amount', 
            'Subtotal', 'Fee', 'Total USD', 'Spot Price', 'Notes'
        ]
        empty_df = pd.DataFrame(columns=required_columns)
        
        # Reset index for non-empty DataFrames
        if not coinbase_df.empty:
            coinbase_df = coinbase_df[required_columns].reset_index(drop=True)
        else:
            coinbase_df = empty_df.copy()
            
        if not coinbase_pro_df.empty:
            coinbase_pro_df = coinbase_pro_df[required_columns].reset_index(drop=True)
        else:
            coinbase_pro_df = empty_df.copy()
            
        if not kraken_df.empty:
            kraken_df = kraken_df[required_columns].reset_index(drop=True)
        else:
            kraken_df = empty_df.copy()
            
        if not strike_df.empty:
            strike_df = strike_df[required_columns].reset_index(drop=True)
        else:
            strike_df = empty_df.copy()
            
        if not cashapp_df.empty:
            cashapp_df = cashapp_df[required_columns].reset_index(drop=True)
        else:
            cashapp_df = empty_df.copy()
        
        if all(df.empty for df in [coinbase_df, coinbase_pro_df, kraken_df, strike_df, cashapp_df]):
            print("Error: No transaction data found in any files.")
            return
        
        # Force timezone-naive for all DataFrames before concat
        for df in [coinbase_df, coinbase_pro_df, kraken_df, strike_df, cashapp_df]:
            if not df.empty and 'Timestamp' in df.columns:
                df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.tz_localize(None)
        
        # Combine all transactions with explicit column order
        all_transactions = pd.concat(
            [coinbase_df, coinbase_pro_df, kraken_df, strike_df, cashapp_df],
            ignore_index=True,
            axis=0
        )
        
        # Standardize signs
        all_transactions = standardize_transaction_values(all_transactions)
        
        # Sort by timestamp
        all_transactions = all_transactions.sort_values('Timestamp')
        
        # Clean up and standardize
        all_transactions['Timestamp'] = pd.to_datetime(all_transactions['Timestamp'])
        all_transactions['Amount'] = pd.to_numeric(all_transactions['Amount'], errors='coerce')
        all_transactions['Subtotal'] = pd.to_numeric(all_transactions['Subtotal'], errors='coerce')
        all_transactions['Total USD'] = pd.to_numeric(all_transactions['Total USD'], errors='coerce')
        all_transactions['Fee'] = pd.to_numeric(all_transactions['Fee'], errors='coerce')
        
        # Clean and convert spot prices to numeric values
        all_transactions['Spot Price'] = all_transactions['Spot Price'].apply(clean_price_string)
        
        # Calculate missing spot prices
        spot_price_mask = (
            (all_transactions['Spot Price'] == 0) & 
            (all_transactions['Amount'].fillna(0) != 0) & 
            (all_transactions['Subtotal'].fillna(0) != 0)  # Changed from Total USD to Subtotal
        )
        
        # Update only rows with zero spot prices
        all_transactions.loc[spot_price_mask, 'Spot Price'] = (
            all_transactions.loc[spot_price_mask, 'Subtotal'] / 
            all_transactions.loc[spot_price_mask, 'Amount']
        )
        
        # Fill NaN values
        all_transactions['Notes'] = all_transactions['Notes'].fillna('')
        all_transactions['Fee'] = all_transactions['Fee'].fillna(0)
        
        # Calculate Total USD based on Subtotal and Fee
        def calculate_total_usd(row):
            try:
                subtotal = float(row['Subtotal'])
                fee = abs(float(row['Fee']))
                return subtotal + (fee * (1 if subtotal >= 0 else -1))
            except:
                return 0
        
        all_transactions['Total USD'] = all_transactions.apply(calculate_total_usd, axis=1)
        
        # Update missing spot prices using API
        all_transactions = update_missing_spot_prices(all_transactions)
        
        # Group transactions and assign lot IDs in one step
        all_transactions = assign_lot_ids_and_group(all_transactions)
        
        # Reorder columns
        column_order = [
            'ID', 'Timestamp', 'Source', 'Type', 'Asset', 'Amount', 
            'Subtotal', 'Fee', 'Total USD', 'Spot Price', 'Lot ID', 'Notes'
        ]
        
        # Now reorder after all columns exist
        all_transactions = all_transactions[column_order]
        
        print(f"\nWriting to Excel file...")

        # Save to Excel
        with pd.ExcelWriter('ALL-MASTER-crypto-transactions.xlsx', engine='openpyxl') as writer:
            # Convert only Amount column to string to preserve full precision
            numeric_df = all_transactions.copy()
            
            # Only convert Amount to string with full precision
            if 'Amount' in numeric_df.columns:
                numeric_df['Amount'] = numeric_df['Amount'].apply(
                    lambda x: format(Decimal(str(x)), 'f') if pd.notnull(x) else ''
                )
            
            numeric_df.to_excel(writer, index=False, sheet_name='Transactions')
            
            # Get the worksheet
            worksheet = writer.sheets['Transactions']

            # Format worksheet
            format_excel_worksheet(worksheet, df=all_transactions, sheet_name='Transactions')
            
        print(f"   DONE!!\n\nFile saved as...\n   ALL-MASTER-crypto-transactions.xlsx")
    except Exception as e:
        print(f"Error merging transactions: {str(e)}")

def format_excel_worksheet(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame = None, sheet_name: str = '') -> None:
    """Comprehensive worksheet formatting function that handles all formatting needs"""
    # Add filters
    if df is not None:
        worksheet.auto_filter.ref = f"A1:{openpyxl.utils.get_column_letter(len(df.columns))}{len(df) + 1}"
    
    # Define styles
    header_fill = openpyxl.styles.PatternFill(
        start_color='CCE5FF',  # Light blue
        end_color='CCE5FF',
        fill_type='solid'
    )
    header_font = openpyxl.styles.Font(bold=True)
    center_align = openpyxl.styles.Alignment(horizontal='center')
    left_align = openpyxl.styles.Alignment(horizontal='left')
    
    # First pass: Apply basic header formatting and calculate content lengths
    column_content_lengths = {}
    
    for column in worksheet.columns:
        col_letter = column[0].column_letter
        header_cell = column[0]
        header_value = str(header_cell.value)
        
        # Apply basic header formatting
        header_cell.fill = header_fill
        header_cell.font = header_font
        header_cell.alignment = center_align  # Default to center
        
        # Calculate max content length (excluding header)
        max_content_length = 0
        for cell in column[1:]:  # Skip header
            try:
                if cell.value:
                    max_content_length = max(max_content_length, len(str(cell.value)))
            except:
                pass
        column_content_lengths[col_letter] = max_content_length
    
    # Second pass: Handle column widths, alignments, and number formats
    for column in worksheet.columns:
        col_letter = column[0].column_letter
        header_cell = column[0]
        header_value = str(header_cell.value)
        header_length = len(header_value)
        max_content_length = column_content_lengths[col_letter]
        
        # Calculate base widths
        content_width = max_content_length + 2  # Add minimal padding
        header_width = header_length + 4  # Add space for filter dropdown
        
        # Use the larger of content or header width, with minimum of 10
        base_width = max(content_width, header_width, 10)
        
        # Reduce width for monetary columns
        if header_value in ['Subtotal', 'Fee', 'Total USD', 'Spot Price']:
            base_width = max(base_width - 6, 10)  # Subtract 5, but keep minimum of 10
        
        # Special handling for Notes column
        if header_value == 'Notes':
            final_width = min(base_width, 200)  # Cap Notes column at 200
        else:
            final_width = min(base_width, 100)  # Other columns capped at 100
        
        # Set column width
        worksheet.column_dimensions[col_letter] = openpyxl.worksheet.dimensions.ColumnDimension(
            worksheet, 
            index=col_letter, 
            width=final_width,
            bestFit=False
        )
        
        # Apply number formatting to data cells
        header_value_lower = header_value.lower()
        for cell in column[1:]:  # Skip header
            if any(x in header_value_lower for x in ['price', 'usd', 'fee', 'subtotal']):
                cell.number_format = openpyxl.styles.numbers.BUILTIN_FORMATS[44]  # Currency
            elif header_value == 'Amount':  # Exact match for Amount column
                if cell.value is not None:
                    # Count significant decimal places in the actual value
                    str_val = str(cell.value)
                    if '.' in str_val:
                        decimal_part = str_val.split('.')[1]
                        # Find the last non-zero digit position
                        last_nonzero = len(decimal_part)
                        for i in range(len(decimal_part) - 1, -1, -1):
                            if decimal_part[i] != '0':
                                last_nonzero = i + 1
                                break
                        decimals = last_nonzero
                        if decimals > 0:
                            cell.number_format = f'#,##0.{"0" * decimals}'
                        else:
                            cell.number_format = '#,##0'
                    else:
                        cell.number_format = '#,##0'
            elif 'date' in header_value_lower:
                cell.number_format = 'YYYY-MM-DD HH:MM:SS'
            elif 'timestamp' in header_value_lower:
                cell.number_format = 'YYYY-MM-DD HH:MM:SS'
                cell.alignment = openpyxl.styles.Alignment(horizontal='left')

def group_similar_transactions(df: pd.DataFrame, time_window_minutes: int = 2) -> pd.DataFrame:
    """Group similar transactions that occur within the specified time window"""
    def can_group_transactions(group: pd.DataFrame) -> bool:
        """Check if transactions in group can be combined"""
        if len(group) <= 1:
            return True
            
        # Sort by timestamp to check time differences
        sorted_group = group.sort_values('Timestamp')
        timestamps = sorted_group['Timestamp'].tolist()
        
        # Check if any consecutive transactions are more than time_window_minutes apart
        for i in range(len(timestamps) - 1):
            time_diff = (timestamps[i + 1] - timestamps[i]).total_seconds() / 60
            if time_diff > time_window_minutes:
                return False
                
        return True
    
    def combine_group(group: pd.DataFrame) -> pd.Series:
        """Combine transactions in a group into a single transaction"""
        if len(group) == 1:
            return group.iloc[0]
            
        # Sort by timestamp to use earliest time
        group = group.sort_values('Timestamp')
        
        # Calculate weighted average for spot price
        total_amount = group['Amount'].abs().sum()
        weighted_spot_price = (
            (group['Spot Price'] * group['Amount'].abs()).sum() / total_amount
            if total_amount > 0 else 0
        )
        
        # Sum the numeric values
        combined = {
            'ID': f"{group['ID'].iloc[0]}_grouped",  # Use first ID with suffix
            'Timestamp': group['Timestamp'].iloc[0],  # Use earliest timestamp
            'Source': group['Source'].iloc[0],
            'Type': group['Type'].iloc[0],
            'Asset': group['Asset'].iloc[0],
            'Amount': group['Amount'].sum(),
            'Subtotal': group['Subtotal'].sum(),
            'Fee': group['Fee'].sum(),
            'Total USD': group['Total USD'].sum(),
            'Spot Price': weighted_spot_price,
            'Lot ID': group['Lot ID'].iloc[0] if 'Lot ID' in group.columns else '',
            'Notes': f"Grouped {len(group)} transactions"
        }
        
        return pd.Series(combined)
    
    # Create copy of dataframe
    df = df.copy()
    
    # Group by asset, source, and type
    grouped = df.groupby(['Asset', 'Source', 'Type'])
    
    # Process each group
    result_dfs = []
    for name, group in grouped:
        if len(group) <= 1:
            result_dfs.append(group)
            continue
            
        # Sort by timestamp
        group = group.sort_values('Timestamp')
        
        # Initialize subgroups
        current_subgroup = []
        all_subgroups = []
        
        # Group transactions within time window
        for idx, row in group.iterrows():
            if not current_subgroup:
                current_subgroup = [row]
            else:
                time_diff = (row['Timestamp'] - current_subgroup[-1]['Timestamp']).total_seconds() / 60
                if time_diff <= time_window_minutes:
                    current_subgroup.append(row)
                else:
                    # Check if current_subgroup can be grouped
                    temp_df = pd.DataFrame(current_subgroup)
                    if can_group_transactions(temp_df):
                        all_subgroups.append(combine_group(temp_df))
                    else:
                        result_dfs.extend(current_subgroup)
                    current_subgroup = [row]
        
        # Handle last subgroup
        if current_subgroup:
            temp_df = pd.DataFrame(current_subgroup)
            if can_group_transactions(temp_df):
                all_subgroups.append(combine_group(temp_df))
            else:
                result_dfs.extend(current_subgroup)
        
        # Add combined subgroups
        if all_subgroups:
            result_dfs.append(pd.DataFrame(all_subgroups))
    
    # Combine all results
    result = pd.concat(result_dfs, ignore_index=True)
    
    # Sort by timestamp
    return result.sort_values('Timestamp').reset_index(drop=True)

def clear_old_cache_entries(days_old: int = 30):
    """Clear cache entries older than specified days"""
    cache = load_price_cache()
    current_time = datetime.now()
    
    # Filter out old entries
    new_cache = {}
    for key, value in cache.items():
        try:
            # Extract date from cache key
            date_str = key.split('_')[1]
            entry_date = datetime.strptime(date_str, '%Y-%m-%d')
            
            # Keep if within time window
            if (current_time - entry_date).days <= days_old:
                new_cache[key] = value
        except:
            continue
    
    save_price_cache(new_cache)

def should_merge_transactions(tx1: Dict, tx2: Dict, time_threshold: int = 90) -> bool:
    """Determine if two transactions should be merged based on criteria"""

    # Don't merge Receive or Learning Reward transactions
    if tx1['Type'] in ['Receive', 'Learning Reward'] or tx2['Type'] in ['Receive', 'Learning Reward']:
        return False
    
    # Don't merge if either transaction is a Convert (they're already paired)
    if 'Converted' in tx1.get('Notes', '') or 'Converted' in tx2.get('Notes', ''):
        return False
    
    # Check if these are Kraken transactions
    if tx1['Source'] == 'Kraken' and tx2['Source'] == 'Kraken':
        # Only proceed with ordertxid matching if both have Notes with Order
        if ('Notes' in tx1 and 'Notes' in tx2 and 
            'Order:' in tx1['Notes'] and 'Order:' in tx2['Notes']):
            
            # Extract ordertxid from Notes
            ordertxid1 = tx1['Notes'].split('Order:')[1].split()[0]
            ordertxid2 = tx2['Notes'].split('Order:')[1].split()[0]
            
            # For Kraken, only check ordertxid match and basic criteria (ignore time)
            return (
                ordertxid1 == ordertxid2 and
                tx1['Type'] == tx2['Type'] and
                tx1['Asset'] == tx2['Asset'] and
                ((tx1['Amount'] > 0) == (tx2['Amount'] > 0))  # Same sign
            )
    
    # For non-Kraken transactions, use time-based merging
    time_diff = abs((tx2['Timestamp'] - tx1['Timestamp']).total_seconds())
    
    return (
        time_diff <= time_threshold and
        tx1['Type'] == tx2['Type'] and
        tx1['Asset'] == tx2['Asset'] and
        tx1['Source'] == tx2['Source'] and
        ((tx1['Amount'] > 0) == (tx2['Amount'] > 0))  # Same sign
    )

def main():
    merge_all_transactions()

if __name__ == "__main__":
    main()
