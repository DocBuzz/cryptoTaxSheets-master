# cryptoTaxSheets - v0.2 (2-part Python script)

______________________
__** INSTRUCTIONS **__
======================

(1) Convert CSV files from Coinbase [Pro], Kraken, Strike, & CashApp into .XLSX files.
- Use Excel, LibreOffice, etc.  Just open each one and "Save As.." .XLSX file.

(2) Either save the .XLSX files directly into the same folder as these 2 python scripts.... or move them into the same folder as these Python scripts.

(3) You will need Python and PIP (which is installed with Python, just need to check a box to make sure it installs).

(4) The required Python dependences can be installed from the command line with this one command:
  - pip install pandas openpyxl requests

(4) You can then run them 1 (merge txs) and 2 (calculate gains), like so:
  -  python 1-merge_crypto_txs.py
  -  python 2-calculate_all_gains.py

  - (+PRO TIP+  From command line, if you're in the same directory as the files, you can just type "python 1" and press the TAB button, and same with "python 2" (TAB key))
 
(5) If you need to add some transactions that were made outside of the 4 supported exchanges, the first script will automatically create a file called "add-manual-transactions.xlsx" for you to add any transactions made outside the 4 supported exchanges.  Include as much info as possible, and then run Step 1-Merge again. This will ensure that ALL your transactions make it to the master transaction file, and then give you the most true and accurate profit/loss calculations for all the different accounting methods.


__________________
__** FEATURES **__
==================

Creates 2 main spreadsheets:
- #1 is a master list of ALL transactions.
- #2 has several sub-sheets that has a ton of useful data, identifying which tax lot is sold for each accounting method, yearly gains in different categories (short-term sales, long-term sales, gifts, staking income), current holdings stats, detailed list of all sales, etc.

Details:
- Fills in / Calculates missing blanks if Fee, Spot Price, etc. if there's enough data to calculate it.
- Otherwise, Fetches missing spot prices from CryptoCompare.com for your first run, and then caches all that spot price data.
- This script groups buys/sales together as best as it can, to lessen the complexities.
- It has quite a few verification/validation checks. 
- After the missing spot prices have been cached, then in a matter of seconds, the 2nd script will generate 4 different spreadsheets, each calculated using a different accounting method (FIFO, HIFO, LIFO, LIHO).
- It will even highlight the different cells (in yellow) between the different accounting method files for you to identify EXACTLY where the biggest differences in profit/loss are between the different methods at the end of a given year.  [NOTE:  You can't just switch accounting methods if you've already been acquiring, holding for years... at least not easily.  You'd have to talk to a tax professional about that.  But...] For someone just now starting to file their taxes, this can definitely save you A TON of headache.  If you buy and sell crypto non-stop for a year, you would easily have more work cut out for you just wrapping your head around what is going on with those transactions from the past.


DISCLAIMER:  This script has a fairly decent amount of "edge case" scenarios put into it, considering I'm just one person.... but I know there's a lot more work that needs to be done.  I would love to hear some feedback or see if anybody either has any other weird scenarios or CSVs from other exchanges to add to the list....    

DOUBLE CHECK the work.  When it comes to "Converting" 1 crypto to another, there's a little bit of number fudging that has to happen with USD valuations of a crypto to crypto conversion / trade (because there wasn't any actual USD used in the trade).  I feel like I did a pretty good job, but if someone knows a better way or sees flaw in my logic or process, I would honeslty love to hear it.  Thank you!
