# cryptoTaxSheets – v0.6 (2-part Python script)

This document gives you a **basic understanding** of what the two scripts do, plus step-by-step instructions to get your exchange data and run them. For technical details (data flow, schemas, extending the scripts), see **[README_Advanced.md](README_Advanced.md)**.

---

## What do these two scripts do? (Simple overview)

You may have bought and sold crypto on several sites (Coinbase, Kraken, Strike, CashApp, etc.). Each site gives you a different kind of download. The two scripts work together so you don’t have to figure out gains and losses by hand.

- **Script 1 — `1-merge_crypto_txs.py`**  
  **“Put everything in one list.”**  
  It reads your CSV/Excel files from the supported exchanges, turns them into one common format, fills in missing info (like fees or USD values) when it can, and looks up historical prices for crypto when needed. At the end it creates **one master list** of all your transactions in a single file: `ALL-MASTER-crypto-transactions.xlsx`. If you have trades from other places (not in the supported list), it can create a template file so you can add those by hand and then run Script 1 again to include them in the master list.

- **Script 2 — `2-calculate_all_gains.py`**  
  **“Figure out profit and loss from that list.”**  
  It reads that master list and figures out your **capital gains and losses** for each sale, using different tax-style methods (FIFO, HIFO, LIFO, LOFO). It produces spreadsheets with things like: which “lot” was used for each sale, yearly gains by category (short-term, long-term, staking, gifts, etc.), your current holdings, and detailed sale-by-sale data. So you get clear numbers you can use for taxes or to compare accounting methods.

**In short:** Script 1 builds one combined transaction list; Script 2 uses that list to compute your gains and losses and output the tax-friendly reports.

---

## INSTRUCTIONS

### (1) Retrieve .CSV files from these locations

- **Coinbase:** https://accounts.coinbase.com/statements  
  Under “Generate custom statement”, use Custom Date range from before your first transaction until today, and select **.CSV** format.

- **Coinbase Pro:** https://accounts.coinbase.com/statements/pro  
  Use **“Fills”** Report Type, **.CSV** format. You must download each year individually through 2022 (when it became “Coinbase Advanced”).

- **Kraken:** https://pro.kraken.com/app/settings/documents  
  Under “Exports”, click “Create Report”, choose **“Ledgers”** and **“Trades”** types, each with the widest date ranges and **.CSV** format — then **UNZIP** the file(s).

- **Strike:** https://dashboard.strike.me/transactions  
  Click “Generate Report” with the widest date ranges.

- **CashApp:** https://cash.app/account/activity  
  Use the **desktop** site. Click “Download” to the right of the transaction filter.

**Tip:** Include the exchange name in the filename (e.g. `coinbase-2024.csv`, `kraken-ledgers.csv`) — it helps both you and the scripts.

### (2) Place your files with the scripts

Put the .CSV (or .XLSX converted) exchange files in the **same folder** as the two Python scripts.

- For Kraken: the **kraken-ledgers** file is essential; **kraken-trades** helps group multiple trades by order.

### (3) Install Python and PIP

- Install Python from https://www.python.org/downloads/  
- During setup, ensure **PIP** is installed (there is usually a checkbox).

### (4) Install required Python packages

From the command line, in the folder with the scripts, run:

```text
pip install pandas openpyxl requests
```

### (5) Run the scripts in order

1. Merge all transactions (builds the master list):

   ```text
   python 1-merge_crypto_txs.py
   ```
   
   OR if using a *nix OS:
   
   ```text
   python3 1-merge_crypto_txs.py
   ```

2. Calculate gains and losses (builds the tax reports):

   ```text
   python 2-calculate_all_gains.py
   ```
   
   OR if using a *nix OS:
   
   ```text
   python3 2-calculate_all_gains.py
   ```

**Pro tip:** From the command line in that folder, you can type `python 1-` and press **TAB**, then Enter; same for `python 2-` and TAB  (or `python3 1-` **TAB** and `python3 2-` **TAB** if using a *nix OS).

### (6) Adding transactions from other exchanges or wallets

If you have transactions from places **other** than the five supported exchanges, the first script can create a file called **`add-manual-transactions.xlsx`**. Fill in as much info as you can, then run `python 1-merge_crypto_txs.py` again. That way **all** your transactions are in the master file, and Script 2 can give you accurate profit/loss for every method.

---

## FEATURES

- **Two main outputs:**
  - **#1** — A **master list** of all transactions (`ALL-MASTER-crypto-transactions.xlsx`).
  - **#2** — Spreadsheets with **profit/loss and tax-related data**: which lot was sold for each method, yearly gains by category (short-term, long-term, gifts, staking), current holdings, and a detailed list of sales.

- **Checks:** The scripts warn you if you’re selling more than you acquired and perform other validation.

- **Filling in blanks:** Missing Fee, Spot Price, etc. are calculated when possible (shown in **yellow** on the master list), or filled using historical prices from CryptoCompare (shown in **red**). Fetched prices are cached so later runs are faster.

- **Grouping:** Buys and sales are grouped where possible to simplify the data.

- **Four accounting methods:** After the master list and price cache are ready, Script 2 quickly produces **four** variants (FIFO, HIFO, LIFO, LOFO). Differences between methods are **highlighted in yellow** so you can see where profit/loss differs most.

- **Current Holdings – Total Fees Paid:** The “Total Fees Paid” column on the Current Holdings sheet shows fees paid on acquisitions (buys, staking, gifts, deposits, etc.) that are still in your holdings, allocated by the remaining amount in each lot. Cost basis (Average/Total) already includes fees; this column is only an informational breakdown.

- **Amount columns (no scientific notation):** Amount and Total Amount columns in all outputs are stored as **numbers** now (not text), so you can use Sum, Average, etc. when you select cells in Excel or LibreOffice. They display up to 16 decimal places without scientific notation and without trailing zeros (e.g. `0.004652` instead of `0.0046520000000000`). You can change `AMOUNT_DECIMALS` at the top of each script if you want more or fewer decimal places.

**Note:** You generally can’t switch accounting methods freely if you’ve been holding for years — discuss with a tax professional. For someone just starting to file crypto taxes, comparing these methods can save a lot of work.

### What the cell colors mean

- **In `ALL-MASTER-crypto-transactions.xlsx` (Script 1 output):**
  - **Yellow** — The value was **calculated or filled** by the script from other columns (e.g. Spot Price from Subtotal/Amount, Fee from Total USD). No external API was used.
  - **Red** — The value was **filled using historical prices** from the CryptoCompare API (e.g. Spot Price when it was missing). These cells are good to double-check.

- **In the profit-and-loss files when you run Script 2 with “ALL” methods (FIFO, LIFO, HIFO, LOFO):**
  - **Yellow** — The cell’s value **differs** between the accounting methods. Use this to see where your choice of method changes gains, cost basis, or other numbers.

---

## DISCLAIMER

These scripts handle many edge cases but are not guaranteed to cover every situation. Always **double-check** the results. Crypto-to-crypto conversions involve some estimation of USD value (since no actual USD was used); the logic has been carefully considered but if you see an issue or have a better approach, feedback is welcome.

---

## Support this project

If this software helped you, consider donating Bitcoin (optional):  
**bc1qfhe46gxujnuhgm3qcfzhj4u2wfy4jq8g8f0mka**

---

## More technical detail

For developers or advanced users: data flow, column schemas, and how to add exchanges or customize behavior are documented in **[README_Advanced.md](README_Advanced.md)**.
