{\rtf1\ansi\ansicpg1252\cocoartf2639
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 #########################################\
# --- Refinitiv (LSEG) ---\
# --- Firm level data extraction ---\
# --- Written by Anthonin Levelu ---\
#########################################\
\
# This Python script allows for:\
\
# - (i) automatic extraction of firm level data from Refinitiv (LSEG Workspace)\
# - (ii) minor data cleaning steps\
\
# Pre-requisite:\
\
# - Log to LSEG workspace and keep it open\
# - Generate Api Key (via Api Key generator)\
# - Install eikon on your python environment:     -- pip install eikon\
\
# Warning:\
\
# - If you have too many request, you may face the API daily data limit. (e.g. use time.sleep() built-in function to pause the execution)\
# - The script creates a lot of .csv files to avoid losing extracted data in case of unexpected failure.}