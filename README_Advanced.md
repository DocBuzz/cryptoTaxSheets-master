# Advanced Developer Guide: Crypto Tax Scripts

This document explains **what each script actually does** under the hood so any developer can understand the data flow, adapt the logic, or extend it for different exchanges, assets, or accounting rules.

---

## Table of Contents

1. [Overview: The Two-Script Pipeline](#overview-the-two-script-pipeline)
2. [Script 1: `1-merge_crypto_txs.py`](#script-1-1-merge_crypto_txspy)
3. [Script 2: `2-calculate_all_gains.py`](#script-2-2-calculate_all_gainspy)
4. [Customization Guide](#customization-guide)

---

## Overview: The Two-Script Pipeline

```
Exchange CSVs/Excel (Coinbase, Kraken, Strike, CashApp, etc.)
                    │
                    ▼
    1-merge_crypto_txs.py
    • Loads each exchange’s files
    • Normalizes to one schema
    • Fills missing USD/price data
    • Groups and assigns Lot IDs
                    │
                    ▼
    ALL-MASTER-crypto-transactions.xlsx  (+ optional add-manual-transactions.xlsx)
                    │
                    ▼
    2-calculate_all_gains.py
    • Reads master file
    • Applies FIFO/LIFO/HIFO/LOFO
    • Computes cost basis & gains per sale
    • Outputs tax-year summaries & holdings
                    │
                    ▼
    ALL-crypto-profit-and-loss[-METHOD].xlsx
```

**Run order:** Always run `1-merge_crypto_txs.py` first, then `2-calculate_all_gains.py`. The second script expects the master file (and its columns, including `Lot ID`) produced by the first.

---

## Script 1: `1-merge_crypto_txs.py`

### Purpose

Combines cryptocurrency transactions from multiple exchanges into **one master file** with a **single schema**. It:

- Detects and loads exchange-specific files (by filename patterns).
- Maps each exchange’s columns to a common set of fields.
- Fills in missing USD amounts and spot prices (including via historical price API).
- Groups trades that belong together (e.g. same order, or same asset within a time window).
- Assigns **Lot IDs** to “receiving” transactions (buys, receives, staking, etc.) for later cost-basis tracking.
- Writes one Excel file and optionally creates a template for manual transactions.

### Entry Point and Main Flow

- **Entry:** `main()` → `merge_all_transactions()`.
- **Steps inside `merge_all_transactions()`:**
  1. Call each `load_*_transactions()` for every supported source.
  2. Optionally load `add-manual-transactions.xlsx` if it exists.
  3. Deduplicate by `(Timestamp, Type, Asset, Amount, Source)` (and warn on duplicates).
  4. Standardize signs and numeric types via `standardize_transaction_values()`.
  5. Sort by `Timestamp`.
  6. **Group and assign Lot IDs:** `assign_lot_ids_and_group()`.
  7. **Fill missing values:** `fill_missing_transaction_values()`.
  8. **Backfill missing spot prices:** `update_missing_spot_prices()` (uses cache + CryptoCompare).
  9. Apply final sign convention (e.g. buys = negative USD, sells = positive).
  10. Write `ALL-MASTER-crypto-transactions.xlsx` and optionally create `add-manual-transactions.xlsx`.

### Supported Sources and File Detection

| Source         | How it’s detected                                                                 | Key behavior |
|----------------|------------------------------------------------------------------------------------|--------------|
| **Coinbase**   | Filenames containing `coinbase` or `csv_version-` (and not `pro`)                 | Converts “Convert” into Buy + Sell; renames “Inflation Reward” → “Staking Income”. |
| **Coinbase Pro** | Filenames with `coinbasepro`/`coinbase-pro` and `fill`/`fills`                  | Uses fills report; derives Subtotal from Total USD and Fee. |
| **Kraken**     | Ledger: `kraken-ledgers`, `kraken_ledgers`, etc. Trades: `kraken-trades`, etc.     | Pairs ledger trades by `refid`; handles USD vs non-USD pairs; maps staking/transfers. |
| **Strike**     | Filenames with `strike` or `btc-account-`                                          | Two formats: “BTC account” (Time (UTC), etc.) and legacy (Completed Date/Time). |
| **CashApp**    | Filenames starting with `cash_app` or `cashapp` + transaction-style names          | Filters to Bitcoin-related, COMPLETE only; maps types to Buy/Sell/Send. If Transaction Type is blank but Notes contain "paid" (e.g. "bitcoin paid"), labels as **Purchase** (spent crypto; script 2 treats as sale). |
| **Manual**     | File `add-manual-transactions.xlsx` in the same directory                         | Loaded as-is; must have the same column set as the master. |

Adding a new exchange means: (1) adding a `load_NEWEXCHANGE_transactions()` that returns `(DataFrame, list_of_files)` and (2) calling it from `merge_all_transactions()` and appending the DataFrame to the combined list before deduplication.

### Standard Output Schema (Master File)

Every row in the master file (and in manual transactions) is expected to have these columns:

| Column      | Meaning |
|------------|---------|
| `ID`       | Unique transaction ID (exchange ID or generated). |
| `Timestamp`| When the transaction occurred (timezone-naive after processing). |
| `Source`   | Exchange or `"Manual"`. |
| `Type`     | e.g. Buy, Sell, Send, Receive, Staking Income, Learning Reward, Convert, etc. |
| `Asset`    | Symbol (BTC, ETH, USD, etc.). |
| `Amount`   | Signed: positive = receive/buy, negative = send/sell. |
| `Subtotal` | USD value before fees (signed). |
| `Fee`      | Fee in USD (stored as positive). |
| `Total USD`| USD value including fees (signed). |
| `Spot Price` | Price per unit in USD at transaction time. |
| `Lot ID`   | Assigned by the merge script for “receiving” rows (e.g. `BTC-00001`). Empty for sends/sells. |
| `Notes`    | Free text (e.g. order IDs, conversion text). |

The script also keeps a `Source File` column for debugging; the gains script does not depend on it.

### Key Functions (What They Actually Do)

- **`read_transaction_file(file_path)`**  
  Reads `.xlsx` or `.csv`. For Coinbase CSV, locates the header row (e.g. by “Timestamp”, “Transaction Type”) and skips to data. For `csv_version-` files, tries several delimiters and encodings.

- **`safe_concat(dfs, columns, **kwargs)`**  
  Concatenates DataFrames and ensures every one has exactly `columns`; missing columns are added (numeric as 0, others as `""`).

- **`standardize_transaction_values(df)`**  
  Ensures consistent sign convention: sells/sends/withdrawals have negative `Amount` and (where applicable) negative `Subtotal`; buys/receives have positive. Drops “Exchange Withdrawal” rows. Recomputes `Total USD` from `Subtotal` and `Fee`.

- **`assign_lot_ids_and_group(df, time_window_seconds=90)`**  
  - For each asset (except USD):  
    - Runs `process_kraken_groups()` to merge Kraken rows that share the same order (via Notes).  
    - Groups rows that are “the same” by `should_merge_transactions()` (same Source, Type, Asset, same sign of Amount, within `time_window_seconds`).  
    - Merges each group with `combine_transactions()` (weighted average spot price, summed amounts/fees/subtotals, combined notes).  
  - Then assigns **Lot IDs** to receiving types only: Buy, Advanced Trade Buy, Receive, Learning Reward, Staking Income, Dividend, Convert. Format: `{Asset}-{counter:05d}` (e.g. `BTC-00001`).

- **`should_merge_transactions(tx1, tx2, time_threshold=90)`**  
  Returns True if the two transactions have the same Type, Asset, Source, same sign of Amount, and are within `time_threshold` seconds.

- **`combine_transactions(group)`**  
  One merged row: sum Amount/Subtotal/Fee/Total USD, weighted-average Spot Price, earliest Timestamp, combined Notes and a single ID (first ID + `_grouped`).

- **`fill_missing_transaction_values(df)`**  
  Fills missing Spot Price, Fee, Subtotal, Total USD when enough other fields exist (e.g. Spot Price from Subtotal/Amount or Total USD/Amount). Tracks which cells were calculated (by transaction ID) for Excel highlighting.

- **`update_missing_spot_prices(df)`**  
  For rows with `Spot Price == 0` and non-USD Asset, calls `get_historical_price(asset, timestamp)`. Then recalculates Subtotal (and Total USD if Fee is 0) where possible. Tracks which IDs got historical prices for highlighting.

- **`get_historical_price(asset, timestamp)`**  
  - Resolves symbol (e.g. LUNA → LUNC after `LUNA_TRANSITION_DATE`; LUNA2 → LUNA; CORECHAIN → CORE for CryptoCompare).  
  - Looks up `price_cache.json` (key = asset + date or minute, depending on API key).  
  - On cache miss: with a valid CryptoCompare API key, uses minute-level data for the last 7 days; otherwise uses daily.  
  - Caches result and caches “no price” for 12 hours to avoid hammering the API.  
  - Returns 0 if no price is found.

### Important Constants

- **`LUNA_TRANSITION_DATE`** (2022-05-28): After this date, “LUNA” in the file is treated as LUNC for price lookups; “LUNA2” is treated as LUNA.
- **`CRYPTOCOMPARE_API_KEY`**: Set to a real key for minute-level recent prices; otherwise only daily prices are used.
- **`price_cache.json`**: Persisted cache; can be cleared or edited if you need to refresh prices.

### Output Files

- **`ALL-MASTER-crypto-transactions.xlsx`**  
  One sheet, `Transactions`, with the columns above. Cell highlighting: **yellow** = value calculated/filled from other columns; **red** = value filled from historical price API (CryptoCompare). Applied in `format_excel_worksheet()`.

- **`add-manual-transactions.xlsx`**  
  Created only if it doesn’t exist; template with the same column headers. Instructions are printed to the console for filling it (e.g. required vs optional fields).

- **`ALL-MASTER-crypto-transactions.log`**  
  Created only when an error is logged (ErrorOnlyFileHandler).

### How to Change Behavior

- **New exchange:** Add a `load_X_transactions()` that returns `(pd.DataFrame with standard columns, list of filenames)` and call it in `merge_all_transactions()`; append to `all_dfs` and `df_sources`.
- **New asset/rename (e.g. LUNA, CORECHAIN):** Add or adjust the symbol mapping at the start of `get_historical_price()` (and in the gains script if it also does price lookups). Use date-based logic for transitions (e.g. LUNA→LUNC, NU→T); use a simple alias for exchange vs API naming (e.g. CORECHAIN→CORE for Coinbase vs CryptoCompare, CGLD→CELO).
- **Grouping rules:** Change `time_window_seconds` in `assign_lot_ids_and_group()` or the logic in `should_merge_transactions()`.
- **Which rows get Lot IDs:** Edit the `receiving_types` list in `assign_lot_ids_and_group()` and the condition that assigns `Lot ID`.
- **Price source:** Replace or wrap `get_historical_price()` to use another API or CSV; keep the same return convention (float USD, 0 on failure) and cache if desired.

---

## Script 2: `2-calculate_all_gains.py`

### Purpose

Reads the master transaction file, applies a chosen **lot accounting method** (FIFO, LIFO, HIFO, LOFO), and:

- Builds **lots** (inventory) per asset from buys, receives, staking, gifts, deposits, etc.
- On each **sale** (and on the sell leg of Convert), **matches lots** according to the method and computes cost basis and gain/loss.
- Tracks **sends/deposits/withdrawals** for transfers and cost-basis carryover.
- Produces **yearly summaries** (short-term vs long-term sales, staking income, gifts) and **current holdings**, and writes Excel reports.

### Entry Point and Main Flow

- **Entry:** `main()`  
  - Prompts for accounting method (ALL / FIFO / LIFO / HIFO / LOFO).  
  - Reads `ALL-MASTER-crypto-transactions.xlsx`.  
  - Calls `validate_input_file(df)`.  
  - For each selected method:  
    - Builds `TransactionProcessor(accounting_method=method)`.  
    - Calls `processor.process_all_transactions(df)`.  
    - Builds `GainsCalculator(processor)` and `yearly_data = calculator.calculate_yearly_summary()`.  
    - Calls `verify_final_calculations(yearly_data, processor)`.  
    - Calls `generate_excel_report(yearly_data, processor, suffix=f'-{method.name}')`.  
  - If ALL was chosen, runs `compare_all_method_outputs()`.  
  - Reports any collected issues (e.g. negative balances, proceeds mismatches).

### Input Requirements

The script expects the master file (or a file with the same schema) to have at least:

- `ID`, `Timestamp`, `Source`, `Type`, `Asset`, `Amount`, `Total USD`, `Fee`, `Spot Price`, `Notes`  
and in practice **`Lot ID`** and **`Subtotal`** (from the merge script).  
`validate_input_file()` checks for the set above and that Timestamp is datetime and key columns are numeric; it does not explicitly require `Lot ID`, but the rest of the code uses `row['Lot ID']` and `row['Subtotal']`, so the merge output (which includes Lot ID and Subtotal) is the intended input.

### Core Data Structures

- **`TransactionType` (Enum)**  
  Buy, Sell, Convert, Staking Income, Gift (includes Learning Reward/Receive), Send, Deposit, Withdrawal, Admin Debit, Dividend, Pro Withdrawal/Deposit, etc. Internal transfers (e.g. “Exchange Withdrawal”, “Pro Withdrawal/Deposit”) are mapped to `None` and skipped for lot tracking.

- **`AssetLot` (dataclass)**  
  One “lot”: `timestamp`, `amount`, `cost_basis`, `source`, `transaction_type`, `remaining`, `transaction_id`, `lot_id`, optional `gift_market_value`, optional `fee` (USD fee on acquisition, for Total Fees Paid on Current Holdings).  
  `cost_per_unit` is derived (cost_basis/amount, or for gifts the market value at receipt).

- **`SaleLotDetail` (dataclass)**  
  One lot’s contribution to a sale: `lot_id`, `amount_sold`, `cost_basis`, `proceeds`, `gain_loss`, `is_long_term`, `purchase_date`, `holding_period_days`, `cost_basis_per_unit`.

- **`SaleTransaction` (dataclass)**  
  One sale event: `timestamp`, `amount`, `proceeds`, `cost_basis`, `source`, `transaction_id`, `lot_details` (list of `SaleLotDetail`), `is_long_term`, `lots_sold` (comma-separated lot IDs).

- **`AccountingMethod` (Enum)**  
  FIFO, LIFO, HIFO, LOFO — controls sort order of lots when selecting which to use for a sale.

- **`AssetTracker`**  
  Per-asset state: lists of `lots`, `sales`, `staking_income`, `gifts`, `sends`, `deposits`, `transfers`, `withdrawals`.  
  - **`add_lot(lot)`**  
    Appends a lot; if `lot_id` is missing/nan, generates e.g. `GEN_{symbol}_{timestamp}`.  
  - **`process_sale(sale_info)`**  
    Gets lots with `remaining > 0`, sorts by the chosen accounting method, then consumes from them until the sale amount is covered; builds `SaleLotDetail`s and one `SaleTransaction`. Proceeds are distributed proportionally when multiple lots are used.  
  - **`find_matching_send(deposit, window_hours=8)`**  
    Tries to match a deposit to a prior send (same asset, amount within 1%, within 8 hours) for transfer cost-basis carryover.  
  - **`process_deposit_with_send(deposit, matching_send)`**  
    Adds a new lot with cost basis = deposit’s Total USD and records a transfer.  
- **`get_current_holdings()`**  
  Returns aggregates (total amount, cost basis, average cost, **total_fees** (fees on remaining holdings), unrealized P&amp;L, exchange distribution, lot list, etc.); excludes Pro Withdrawal/Pro Deposit from holdings. Each `AssetLot` has an optional `fee` field; Total Fees Paid = sum of `lot.fee * (lot.remaining / lot.amount)` over valid lots.

- **`TransactionProcessor`**  
  Holds a dict of `AssetTracker` per symbol and the chosen `AccountingMethod`.  
  - **`process_transaction(row)`**  
    Dispatches by `TransactionType`: Buy/Convert (add lot), Sell (process_sale), Staking Income (add to staking_income + add lot with 0 cost basis), Gift (add to gifts + add lot with gift_market_value), Send (record send), Deposit (match send or add new lot), Admin Debit (reduce lots without sale), Dividend (add lot 0 cost basis), Withdrawal (record). Convert is handled as sell of one asset + buy of another (received asset from `extract_received_asset(row['Notes'])`).  
  - **`process_all_transactions(df)`**  
    Sorts by Timestamp, then runs a **pre-pass**: `calculate_gains(df, self.accounting_method)` to get lot assignments and validation errors. Then iterates rows and calls `process_transaction(row)` for each. So the “gains” path and the “tracker” path both exist; the tracker path is what drives the report.

- **`GainsCalculator.calculate_yearly_summary()`**  
  Groups by year and asset: short-term vs long-term sales (from `SaleTransaction.is_long_term`), staking income, gifts. Excludes staking/gift amounts that were sold in the same year (to avoid double-counting). Returns a nested dict `year -> symbol -> { short_term_sales, long_term_sales, staking_income, gifts }`.

### Accounting Methods (What They Do)

- **FIFO:** Sort lots by `timestamp` ascending; use oldest first.  
- **LIFO:** Sort by `timestamp` descending; use newest first.  
- **HIFO:** Sort by `cost_per_unit` descending then timestamp; use highest cost first.  
- **LOFO:** Sort by `cost_per_unit` ascending then timestamp; use lowest cost first.

Long-term = holding period &gt; 365 days (per lot); a sale is “long-term” only if every lot used has holding &gt; 365 days.

### Sale Proceeds and Cost Basis

- For **sale proceeds**, the script uses **Subtotal** (USD before fees) from the master file when processing sales.  
- **Cost basis** comes from the chosen lots’ `cost_per_unit * amount_sold`.  
- **Gain/Loss** = proceeds − cost basis (per lot and per sale).  
- Proceeds are distributed across lots in proportion to `amount_sold` when a sale uses multiple lots.

### Historical Prices in This Script

- **`get_historical_price(asset, timestamp, ...)`**  
  Used for **current holdings** (e.g. unrealized P&amp;L at “now”) and possibly other spots. Same cache file `price_cache.json` and same symbol mapping as in the merge script (LUNA/LUNA2, NU→T, CGLD→CELO, CORECHAIN→CORE); NU has an extra `NU_TRANSITION_DATE` (2023-02-06) for NU→T.

### Output Files

- **`ALL-crypto-profit-and-loss-{FIFO|LIFO|HIFO|LOFO}.xlsx`**  
  One file per method (or one if a single method was chosen). Sheets typically include:  
  - **Method Info:** Accounting method and processing date.  
  - **Year_YYYY:** Per-year, per-asset summary rows (Short-term Sales, Long-term Sales, Staking Income, Gifts) with Total Amount, Proceeds, Cost Basis, Gain/Loss, count, date range.  
  - **Current Holdings:** Per-asset totals, cost basis, spot value, unrealized P&amp;L, **Total Fees Paid**, exchange distribution, lot list. **Total Fees Paid** is the sum of fees from acquisitions (Buy, Staking Income, Gift, Deposit, Convert, Dividend) that are still in holdings, allocated by remaining amount per lot (`lot.fee * (lot.remaining / lot.amount)`). Each lot stores a `fee` field from the master file’s Fee column; cost basis is unchanged and already includes fees.  
  - **Transfers:** Sends, deposits, withdrawals (exchange-to-exchange and to external wallet).  
  - **Sale Details:** Each sale with Asset, Sale Date, Amount Sold, Proceeds, Cost Basis, Gain/Loss, Lots Used, Term (Short/Long/Mixed).

- **`compare_all_method_outputs()`**  
  When ALL methods are run, compares the generated files and highlights cells where values differ between FIFO/LIFO/HIFO/LOFO in **yellow**.

- **`ALL-crypto-profit-and-loss-errors.log`**  
  Created only when errors are logged (ErrorOnlyFileHandler).

### Cell highlights and colors

| File / context | Color | Meaning |
|----------------|-------|--------|
| **`ALL-MASTER-crypto-transactions.xlsx`** (Script 1) | **Yellow** | Value was **calculated or filled** by the script from other columns (e.g. Spot Price from Subtotal/Amount, Fee from Total USD). No external API. |
| **`ALL-MASTER-crypto-transactions.xlsx`** (Script 1) | **Red** | Value was **filled using historical prices** from the CryptoCompare API (e.g. Spot Price when missing). Double-check these. |
| **`ALL-crypto-profit-and-loss-{METHOD}.xlsx`** when you run **ALL** methods (Script 2) | **Yellow** | This cell’s value **differs** between accounting methods (FIFO, LIFO, HIFO, LOFO). Use it to see where method choice changes gains, cost basis, etc. |
| **Sale Details** sheet (Script 2) | **Light red** (row) | That sale row had a validation issue (e.g. proceeds mismatch). Check the Errors sheet or log if present. |

### Validation and Errors

- **`validate_input_file(df)`**  
  Required columns, datetime Timestamp, numeric columns, non-null ID/Asset/Type.

- **`verify_final_calculations(yearly_data, processor)`**  
  Sanity checks on totals and consistency.

- **Negative balance:** If, for an asset, the sum of Amount (ignoring sends and similar) is negative, a validation error is recorded.

- **Proceeds mismatch:** If expected vs actual proceeds don’t match (e.g. from Subtotal vs sum of lot portions), the processor records a proceeds mismatch and can store details for reporting.

### How to Change Behavior

- **Different input file or columns:** Change the path in `main()` and/or column names used in `process_transaction()` and `calculate_gains()`; update `validate_input_file()` to match.
- **New transaction type:** Add it to `TransactionType` and handle it in `TransactionType.from_string()` and in `process_transaction()` (e.g. add a new lot type or skip).
- **New accounting method:** Add to `AccountingMethod` and add the corresponding sort in `AssetTracker.process_sale()` (and in `calculate_gains()` if that path is used for validation).
- **Long-term threshold:** The 365-day rule is in `AssetLot.is_long_term_at_date()` and in the sale processing (holding_period_days &gt; 365). Change there for a different threshold.
- **Gift/Staking cost basis:** Gifts use `gift_market_value` at receipt; staking uses 0. Adjust in the lot-creation logic in `process_transaction()`.
- **Transfer matching:** Adjust `find_matching_send()` (time window, amount tolerance) or add alternative matching (e.g. by memo/txid).
- **Report layout or sheets:** Edit `generate_excel_report()` and `format_holdings_dataframe()` / `format_excel_worksheet()`.
- **Asset renames / price lookups:** Update the same symbol/date logic as in the merge script in `get_historical_price()` (and any other places that map symbols).
- **Amount column decimal places:** Both scripts use **`AMOUNT_DECIMALS`** (default 16) and a custom number format so Amount/Total Amount columns stay **numeric** (Sum/Average work in Excel/LibreOffice), display without scientific notation, and show only significant digits (no trailing zeros). Format uses `#` placeholders after the decimal (e.g. `#,##0.################`). Change `AMOUNT_DECIMALS` at the top of each script to show more or fewer decimal places (e.g. 24 if needed).

---

## Customization Guide

### Adding a New Exchange (Merge Script)

1. Add a function `load_NEWEXCHANGE_transactions()` that:
   - Uses `glob` (or similar) to find files (e.g. `*newexchange*.csv`).
   - Uses `read_transaction_file()` for each file.
   - Maps columns to: `ID`, `Timestamp`, `Source`, `Type`, `Asset`, `Amount`, `Subtotal`, `Fee`, `Total USD`, `Spot Price`, `Notes`, `Source File`.
   - Ensures `Source` is e.g. `'NewExchange'`.
   - Returns `(pd.DataFrame, list_of_file_paths)`.
2. In `merge_all_transactions()`, call it and append the DataFrame to `all_dfs` and the source label to `df_sources`.
3. Ensure timestamps are timezone-naive and numeric columns are consistent (e.g. Decimal or float) so downstream steps don’t break.

### Adding a New Transaction Type (Gains Script)

1. Add the type to `TransactionType` (e.g. `NEW_TYPE = 'new type'`).
2. In `TransactionType.from_string()`, map the string from the master file (e.g. “New Type”) to that enum member.
3. In `TransactionProcessor.process_transaction()`, add a branch that either:
   - Adds a lot (with the right cost basis and transaction_type), or
   - Calls `process_sale()` or records a send/deposit/withdrawal, or
   - Skips (return without adding to lots).

### Using a Different Price API

- In **merge script:** Replace or wrap the logic inside `get_historical_price()` to call your API; keep the same signature `(asset, timestamp) -> float` (0 on failure) and update the cache key/format if needed.
- In **gains script:** Same for `get_historical_price()` there (used for current holdings, etc.). Keep cache keying consistent if both scripts share `price_cache.json`, or use a separate cache for the gains script.

### Changing Lot ID Format or Assignment

- **Merge script:** In `assign_lot_ids_and_group()`, change the `receiving_types` list and the line that sets `result.loc[idx, 'Lot ID'] = f"{asset}-{lot_counter:05d}"` (e.g. different format or different rows that get an ID).
- **Gains script:** It only reads `Lot ID` from the master file; no need to change the gains script unless you add new logic that generates or displays Lot IDs differently.

### Changing Sign Conventions

- **Merge script:** `standardize_transaction_values()` and the final loop before writing (buys negative, sells positive) define the convention. Change those masks and formulas consistently.
- **Gains script:** It assumes the master file’s signs; it uses `Amount` sign and `Subtotal`/`Total USD` for buys vs sells. If you change the merge output convention, ensure the gains script’s interpretation of “buy” vs “sell” (and sign of amounts) is updated in `process_transaction()` and in `calculate_gains()`.

---

## Quick Reference: Key Files and Constants

| Item | Location |
|------|----------|
| Merge entry | `1-merge_crypto_txs.py` → `main()` → `merge_all_transactions()` |
| Gains entry | `2-calculate_all_gains.py` → `main()` |
| Master output | `ALL-MASTER-crypto-transactions.xlsx` |
| Manual template | `add-manual-transactions.xlsx` |
| Price cache | `price_cache.json` (used by both scripts) |
| LUNA/LUNC date | `LUNA_TRANSITION_DATE` (merge + gains) |
| NU→T date | `NU_TRANSITION_DATE` (gains script) |
| Price API symbol aliases | In `get_historical_price()` (merge + gains): CORECHAIN→CORE, CGLD→CELO; plus LUNA/LUNA2/NU→T by date. |
| API key | `CRYPTOCOMPARE_API_KEY` (merge + gains) |
| Amount decimals | `AMOUNT_DECIMALS` (merge + gains; default 16; controls display of Amount/Total Amount columns, no trailing zeros) |
| Report output | `ALL-crypto-profit-and-loss-{METHOD}.xlsx` (METHOD = FIFO, LIFO, HIFO, or LOFO) |

---

## Support this project

If this software helped you, consider donating Bitcoin (optional):  
**bc1qfhe46gxujnuhgm3qcfzhj4u2wfy4jq8g8f0mka**

---

With this guide, you can trace any behavior to the right function, extend support for new exchanges or transaction types, and adjust accounting or reporting to fit your crypto taxation and accounting needs.
