# cryptoTaxSheets - v0.4 (2-part Python script)

______________________
__** INSTRUCTIONS **__
======================

(1) Retrieve .CSV files from these locations:
  - Coinbase: https://accounts.coinbase.com/statements

     (Under 'Generate custom statement', use Custom Date range from before first transaction until today, and select .CSV format)
  - Coinbase Pro: https://accounts.coinbase.com/statements/pro

    (Use 'Fills' Report Type, .CSV format, and you must download each year individually thru 2022, when it became "Coinbase Advanced")
  - Kraken: https://pro.kraken.com/app/settings/documents

    (Under 'Exports', click "Create Report", choose 'Ledgers' *and* 'Trades' types, each with widest date ranges and .CSV format... then UNZIP!)
  - Strike: https://dashboard.strike.me/transactions

    (Click "Generate Report" with widest date ranges)
  - CashApp: https://cash.app/account/activity

    (Must use desktop site.  Click "Download" to the right of the transaction filter)
    
  - **(Try to include the exchange name in the filename for your own benefit, as well as the benefit of these scripts...)**

(2) Place .CSV [or .XLSX converted] exchange files into the same folder as these 2 python scripts.
  - If using Kraken, the kraken-ledgers file is essential, but the kraken-trades also helps with grouping multiple transactions by each actual "order".

(3) Install Python and PIP (which is installed with Python, just need to check a box to make sure it installs).
  - https://www.python.org/downloads/

(4) The required Python dependences can be installed from the command line with this one command:
  - pip install pandas openpyxl requests

(5) You can then run them 1 (merge txs) and 2 (calculate gains), like so:
  -  python 1-merge_crypto_txs.py
  -  python 2-calculate_all_gains.py

  - (+PRO TIP+  From command line, if you're in the same directory as the files, you can just type "python 1" and press the TAB button, and same with "python 2" (TAB key))
 
(6) If you need to add some transactions that were made outside of the 5 supported exchanges, the first script will automatically create a file called "add-manual-transactions.xlsx" for you to add any transactions made outside the ~5 supported exchanges.  Include as much info as possible, and then run 'python 1-merge_crypto_txs.py' again. This will ensure that ALL your transactions make it to the master transaction file, and then give you the most true and accurate profit/loss calculations for all the different accounting methods.


__________________
__** FEATURES **__
==================

Creates 2 main spreadsheets:
- #1 is a master list of ALL transactions.
- #2 has several sub-sheets that has a ton of useful data, identifying which tax lot is sold for each accounting method, yearly gains in different categories (short-term sales, long-term sales, gifts, staking income), current holdings stats, detailed list of all sales, etc.

Details:
- Fills in / Calculates missing blanks for Fee, Spot Price, etc. if there's enough data to calculate it (highlighted in yellow on the MASTER Tx List).
- Otherwise, Fetches missing spot prices from CryptoCompare.com for your first run, and then caches all that spot price data (highlighted in red on the MASTER Tx List).
- This script groups buys/sales together as best as it can, to lessen the complexities.
- It has quite a few verification/validation checks. 
- After the missing spot prices have been cached, then in a matter of seconds, the 2nd script will generate 4 different spreadsheets, each calculated using a different accounting method (FIFO, HIFO, LIFO, LOFO).
- It will even highlight the different cells (in yellow) between the different accounting method files for you to identify EXACTLY where the biggest differences in profit/loss are between the different methods at the end of a given year.  [NOTE:  You can't just switch accounting methods if you've already been acquiring, holding for years... at least not easily.  You'd have to talk to a tax professional about that.  But...] For someone just now starting to file their taxes, this can definitely save you A TON of headache.  If you buy and sell crypto non-stop for a year, you would easily have more work cut out for you just wrapping your head around what is going on with those transactions from the past.


DISCLAIMER:  This script has a fairly decent amount of "edge case" scenarios put into it, considering I'm just one person.... but I know there's a lot more work that needs to be done.  I would love to hear some feedback or see if anybody either has any other weird scenarios or CSVs from other exchanges to add to the list....    

DOUBLE CHECK the work.  When it comes to "Converting" 1 crypto to another, there's a little bit of number fudging that has to happen with USD valuations of a crypto to crypto conversion / trade (because there wasn't any actual USD used in the trade).  I feel like I did a pretty good job, but if someone knows a better way or sees flaw in my logic or process, I would honeslty love to hear it.  Thank you!
