# cryptoTaxSheets - v0.1 (2-part Python script)

______________________
__** INSTRUCTIONS **__
======================

(1) Convert CSV files from Coinbase [Pro], Kraken, Strike, & CashApp into *1* MASTER .XLSX file...

(2) After converting the CSVs into XLSX spreadsheets, place them into the same directory as these Python scripts, and then run them 1 (merge txs) and 2 (calculate gains)....  Prepare to be amazed at the 2 spreadsheets it creates.  The 2nd spreadsheet has several sheets with a ton of useful data, identifying which tax lot is sold for each accounting method, yearly gains in different categories, current holdings stats, detailed list of all sales, etc.

(3) If you need to add some transactions in there between Step 1 (merging the various .XLSX files (after you convert them from .CSV of course!) and Step 2, that's obviously no problem... just add some data to the end of the Master Sheet... fill out as much as you possibly can... and the 2nd script should be able to fill in the blanks.


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