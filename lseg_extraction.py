#########################################
# --- Refinitiv (LSEG) ---
# --- Firm level data extraction ---
# --- Written by Anthonin Levelu ---
#########################################

# This Python script allows for:

# - (i) automatic extraction of firm level data from Refinitiv (LSEG Workspace)
# - (ii) minor data cleaning steps

# Pre-requisite:

# - Log to LSEG workspace and keep it open
# - Generate Api Key (via Api Key generator)
# - Install eikon on your python environment:     -- pip install eikon

# Warning:

# - If you have too many request, you may face the API daily data limit. (e.g. use time.sleep() built-in function to pause the execution)
# - The script creates a lot of .csv files to avoid losing extracted data in case of unexpected failure.

#########################################
# 1 Import packages

import eikon as ek
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import glob
import os
import time
import pickle

#########################################
# 2 Set ur API Key and working directory
# One need to generate an Api key directly from LSEG workspace

ek.set_app_key('YOUR_API_KEY')
os.chdir('/Users/anthoninlevelu/Desktop/LSEG_extraction')

#########################################
# 3 Generate lists of list of variables to extract
# If you need more firm-level data, go to DIB in LSEG workspace and browse for the variables of your choice.


listfields1 = [
    ["TR.UltimateParent", "TR.UltimateParentId"] #5
]

listfields2 = [
    ["TR.CommonName","TR.HQCountryCode","TR.OrganizationStatusCode"], #1
    ["TR.NAICSSector","TR.LegalEntityIdentifier","TR.NAICSSectorCode"], #2
    ["TR.TRBCBusinessSector", "TR.NAICSSubsector"],  # 3
    ["TR.TRBCIndustryGroup", "TR.SICIndustryCode", "TR.GICSSubIndustryCode"],  # 4
    ["TR.UltimateParent", "TR.UltimateParentId", "TR.UltimateParentISOCountryHQ"] #5
]

listfields3 = [
    ["TR.F.TotRevenue", "TR.F.TotRevenue.date"], #1
    ["TR.F.TotAssets", "TR.F.TotAssets.date"], #2
    ["TR.F.TotFixedAssetsNet","TR.F.TotFixedAssetsNet(Period=FQ0).date"], #3
    ["TR.F.IntangGrossTot", "TR.F.IntangGrossTot.date"], #4
    ["TR.PCFullTimeEmployee", "TR.PCFullTimeEmployee.date"], #5
    ["TR.F.OpProfBefNonRecurIncExpn", "TR.F.OpProfBefNonRecurIncExpn.date"], #6
    ["TR.F.RevGoodsSrvcASR", "TR.F.RevGoodsSrvcASR.date"], #7
    ["TR.F.CostOfOpRev", "TR.F.CostOfOpRev.date"], #8
    ["TR.F.MktCap", "TR.F.MktCap.date"], #9
    ["TR.F.IntangExclGoodwGrossTotASR", "TR.F.IntangExclGoodwGrossTotASR.date"], #10
    ["TR.F.LaborRelExpnInclStockBasedCompInCOGS", "TR.F.LaborRelExpnInclStockBasedCompInCOGS.date"], #11
    ["TR.F.COGSTot","TR.F.COGSTot.date"], #12
    ["TR.StockBasedCompMean","TR.StockBasedCompMean.date"], #13
    ["TR.F.LaborRelExpnTot", "TR.F.LaborRelExpnTot.date"], #14
    ["TR.F.BUSEmpFTEEquivPrdEndASR", "TR.F.BUSEmpFTEEquivPrdEndASR.date"], #15
    ["TR.F.GEOExportSales","TR.F.GEOExportSales.date"], #16
    ["TR.F.EmpFTEEquivPrdEnd", "TR.F.EmpFTEEquivPrdEnd).date"],  # 17
    ["TR.SalariesCSRreporting", "TR.SalariesCSRreporting.date"],  # 18
    ["TR.F.SalesPerEmp", "TR.F.SalesPerEmp.date"],  # 19
    ]

#########################################
# 4 Define function to split the list of firms ID into chunks
# May not be useful depending on the size of your list.

def split(list_a, chunk_size):
  for i in range(0, len(list_a), chunk_size):
    yield list_a[i:i + chunk_size]
chunk_size = 1000


#########################################
# 5 Extract time invariant data.

# gen list of list of all firms
os.chdir('/Users/anthoninlevelu/Desktop/LSEG_extraction')
listoffirm = pd.read_csv('list_example.csv')
listoffirm = listoffirm['0'].unique().tolist()
listoffirm = [str(x) for x in listoffirm]
listoffirm = [s.replace('.0', '') for s in listoffirm]
#listofpar.remove('nan')

# Split the list
newlist = list(split(listoffirm, chunk_size))

# Loop through all chunks and get time invariant data
# Failure could come from "Request Timeout"

os.chdir('/Users/anthoninlevelu/Desktop/LSEG_extraction/data/invariant')
appended_data = pd.DataFrame()
l = 0
idn = 0
for id in newlist:
    print(id)
    idn += 1
    for k in listfields2:
        l += 1
        m = 'field' + str(l)
            while True:
                try:
                    print(k)
                    print(m)
                    tinit = time.time()
                    data, err = ek.get_data(id, k, parameters = {'SDate': '1980-01-01', 'EDate': '2021-12-31', 'Frq': 'FY', 'Curn':'USD'})
                    print(data)
                    if data.columns[0:1] == 0:
                        del data
                        print('Empty data --> deleted')
                    else:
                        print("Time elapsed: ")
                        print(time.time()-tinit)
                        appended_data = appended_data.append(data, True)
                        appended_data.to_csv('invariant_firm'+ "_" + str(idn) + "_" + str(m) + "_" + "1980" + "_" + "2021" + '.csv', index=False, mode='w')
                        appended_data = pd.DataFrame()
                    print("Success for Chunk n°" + str(idn) + "/" + str(len(newlist)) + " and field:" + str(m))
                except Exception as e:
                    print("FAILED but TRY AGAIN")
                    continue
                break
        l = 0


#########################################
# 6 Extract time variant data.

# gen list of list of all firms
os.chdir('/Users/anthoninlevelu/Desktop/LSEG_extraction')
listoffirm = pd.read_csv('list_example.csv')
listoffirm = listoffirm['0'].unique().tolist()
listoffirm = [str(x) for x in listoffirm]
listoffirm = [s.replace('.0', '') for s in listoffirm]

# Split the list
newlist = list(split(listoffirm, chunk_size))

# Loop through all chunks and get time variant data
# Failure could come from "Request Timeout"

os.chdir('/Users/anthoninlevelu/Desktop/LSEG_extraction/data/variant')
appended_data = pd.DataFrame()
l = 0
idn = 0
for id in newlist:
    print(id)
    idn += 1
    for k in listfields3:
        l += 1
        m = 'field' + str(l)
            while True:
                try:
                    print(k)
                    print(m)
                    tinit = time.time()
                    data, err = ek.get_data(id, k, parameters = {'SDate': '1980-01-01', 'EDate': '2021-12-31', 'Frq': 'FY', 'Curn':'USD'})
                    print(data)
                    if data.columns[0:1] == 0:
                        print('Empty data --> keep')
                    else:
                        print("Time elapsed: ")
                        print(time.time()-tinit)
                        appended_data = appended_data.append(data, True)
                        appended_data.to_csv('variant_firm'+ "_" + str(idn) + "_" + str(m) + "_" + "1980" + "_" + "2021" + '.csv', index=False, mode='w')
                        appended_data = pd.DataFrame()
                    print("Success for Chunk n°" + str(idn) + "/" + str(len(newlist)) + " and field:" + str(m))
                except Exception as e:
                    print("FAILED but TRY AGAIN")
                    continue
                break
        l = 0


#########################################
# 7 Merge resulting .csv.

# 7.1. time-invariant

os.chdir('/Users/anthoninlevelu/Desktop/LSEG_extraction/data/invariant')
for i in range(1,6):
    print(i)
    path = '/Users/anthoninlevelu/Desktop/LSEG_extraction/data/invariant/'
    list_csv = [csv for csv in glob.glob(path + "invariant_firm*"+ "*_field"+ str(i) +"_*.csv")]
    merged_csv = pd.concat([pd.read_csv(f, low_memory=False) for f in list_csv])
    merged_csv = merged_csv.dropna()
    merged_csv.drop_duplicates(inplace=True)
    merged_csv = merged_csv.drop_duplicates(subset=["Instrument"], keep='last')
    merged_csv.to_csv('annualy_invariant_' + str(i) +'.csv', index=False, mode='w')


# Merge field files by identifier (time invariant)!

files = glob.glob(os.path.join(path, "annualy_invariant_*.csv"))
df = pd.merge(
    pd.read_csv(files.pop()),
    pd.read_csv(files.pop()),
    on=['Instrument'],
    how='outer',
)

while files:
    df = pd.merge(
        df,
        pd.read_csv(files.pop()),
        how='outer',
        on=['Instrument']
    )

df.to_csv('annualy_invar_all.csv', index=False, mode='w')

# 7.2. time-variant

#time-variant

os.chdir('/Users/anthoninlevelu/Desktop/LSEG_extraction/data/variant/')
for i in range(1,20):
    print(i)
    path = '/Users/anthoninlevelu/Desktop/LSEG_extraction/data/variant/'
    list_csv = [csv for csv in glob.glob(path + "variant_firm*"+ "*_field"+ str(i) +"_*.csv")]
    if len(list_csv) != 0:
        merged_csv = pd.concat([pd.read_csv(f, low_memory=False) for f in list_csv])
        merged_csv = merged_csv.dropna()
        merged_csv['Date'] = merged_csv['Date'].astype(str)
        merged_csv['Date'] = merged_csv['Date'].str[:4]
        merged_csv.drop_duplicates(inplace=True)
        merged_csv = merged_csv.drop_duplicates(subset=["Instrument", "Date"], keep='last')
        merged_csv.to_csv('annualy_var_par' + str(i) +'.csv', index=False, mode='w')
    else:
        print('Empty data')


# Merge field files by identifier and date (time variant)!

files = glob.glob(os.path.join(path, "annualy_var_par*.csv"))
df = pd.merge(
    pd.read_csv(files.pop(), dtype=str),
    pd.read_csv(files.pop(), dtype=str),
    on=['Instrument', 'Date'],
    how='outer',
)

while files:
    df = pd.merge(
        df,
        pd.read_csv(files.pop(), dtype=str),
        how='outer',
        on=['Instrument', 'Date']
    )

df.to_csv('annualy_var_all.csv', index=False, mode='w')


# 7.3. merge time-invariant and time-variant data

os.chdir('/Users/anthoninlevelu/Desktop/LSEG_extraction/data/')

invar = pd.read_csv('/Users/anthoninlevelu/Desktop/LSEG_extraction/data/invariant/annualy_invar_all.csv',low_memory=False)
var = pd.read_csv('/Users/anthoninlevelu/Desktop/LSEG_extraction/data/variant/annualy_var_all.csv',low_memory=False)
merged = pd.merge(invar,var, how='outer', on=['Instrument'])
merged.to_csv("firm_data.csv", index=False, mode='w')




