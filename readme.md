
 --- Refinitiv (LSEG) ---
 --- Firm-level data extraction ---
 --- Written by Anthonin Levelu ---

This Python script allows for:

- (i) automatic extraction of firm-level data from Refinitiv (LSEG Workspace)
- (ii) minor data cleaning steps
  
Pre-requisite:

- Log to LSEG workspace and keep it open
- Generate API Key (via API Key generator)
- Install eikon on your python environment:     -- pip install eikon

 Warning:

- If you have too many request, you may face the API daily data limit. (e.g. use time.sleep() built-in function to pause the execution)
- The script creates a lot of .csv files to avoid losing extracted data in case of unexpected failure.
