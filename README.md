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

  - (+ PRO TIP +  From command line, if you're in the same directory as the files, you can just type "python 1" and press the TAB button, and same with "python 2" <TAB>)
 
(5) If you need to add some transactions in there after Step 1, the script will automatically create a file called "add-manual-transactions.xlsx" for you to add any transactions made outside the 4 supported exchanges.  Include as much info as possible, and then run Step 1-Merge again. This will ensure that ALL your transactions make it to the master transaction file, as well as give you true and accurate profit/loss calculations for all the different accounting methods.


Prepare to be amazed at the 2 spreadsheets these scripts create.  The 2nd spreadsheet has several sheets with a ton of useful data, identifying which tax lot is sold for each accounting method, yearly gains in different categories, current holdings stats, detailed list of all sales, etc.

__________________
__** FEATURES **__
==================

- Breaks down yearly gains in different categories (short-term, long-term, gifts, staking income), current holdings stats, detailed sales.
- In a matter of seconds, you'll have 4 different spreadsheets, each calculated from a different accounting method (FIFO, HIFO, LIFO, LIHO).
- It will even highlight the different cells between the differen sheets all in yellow for you to see where the biggest differences are between them.  [NOTE:  You can't just switch accounting methods if you've already been acquiring, holding for years... at least not easily.  You'd have to talk to a tax professional about that.  But...] For someone just now starting to file their taxes, this can definitely save you A TON of headache.
- If you buy and sell crypto non-stop for a year, you would easily have more work cut out for you just wrapping your head around what is going on with those transactions from the past.
- This script groups buys/sales together as best as it can, to lessen the complexities.
- It grabs daily data from CryptoCompare if necessary... fills in a lot of missing blanks... and has quite a bit of verification/validation checks. 

DISCLAIMER:  This script has a fairly decent amount of "edge case" scenarios put into it, considering I'm just one person.... but I know there's a lot more work that needs to be done.  I would love to hear some feedback or see if anybody either has any other weird scenarios or CSVs from other exchanges to add to the list....    

Word to the wise... DOUBLE CHECK your work.  When it comes to "Converting" 1 crypto to another, there's a decent bit of number fudging that has to happen with Kraken and Coinbase Converts.  I feel like I did a pretty good job, but if someone knows a better way or sees flaw in my logic or process, I would honeslty love to hear it.  Thanks again...  Hope you enjoy!!
