# Parquet Reading Tools

Functions to read and write chunked parquet files to and from dataframes.


### Contents

- [General Information](#general-info)
- [Features](#features)
- [Technologies](#tech)
- [License](#license)


### General Information
Developed to simplify reading and writing of chunked parquets on EC2.  Identifies data files in a specified directory and reads data into a pandas dataframe.  Writes data to specified number of parquet files.


### Features
- Restrict number of chunks read
- Prints chunk file size number of rows


### Technologies
Built with Python 3.7
##### Uses the following packages:
- os
- glob
- numpy
- pandas


### License
MIT 2019