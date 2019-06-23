

# Author: Wah (Nick) Tran
# Status: Production


import os
import glob

import numpy as np
import pandas as pd



def _get_file_size(file):
    '''
    Helper function to determine the appropriate order of magnitude
    for file size.

    Parameters
    ----------

    file : string
        Path and name of file to be sized.

    Returns
    -------

    fsize : string
        File size in bytes with appropriate order of magnitude.
    '''

    size = os.stat(file).st_size

    if size < 10**3:
        fsize = '{:.1f}B'.format(size)
    elif size < 10**6:
        fsize = '{:.1f}KB'.format(size/10**3)
    elif size < 10**9:
        fsize = '{:.1f}MB'.format(size/10**6)
    else:
        fsize = '{:.1f}GB'.format(size/10**9)

    return fsize



def read_parquets(path, limit=None, reset_index=True):
    '''
    Imports parquet files for a chunked dataset.

    Scans for data files in a location, reads using pandas read_parquet,
    and concatenates data into a single dataset.

    * Path must only contain datafiles and must be parquet format. *

    Parameters
    ----------

    path : string
        Directory in which data files are stored.

    limit : int or None (default=None)
        Read only specified number of files.
        If None, all file are read.

    Returns
    -------

    data : DataFrame, shape (n_samples, n_features)
        Single dataframe containing data from all files read.
    '''  
    
    # Identify files in directory
    data_files = glob.glob(path + '*')
    n_files = len(data_files)
    
    print('\n{} files detected\n'.format(n_files))
    print('Reading...')

    
    # If only one file is detected, read that file directly.
    if n_files == 1:
        data = pd.read_parquet(data_files[0])
        
        data_len = len(data)
        data_size = _get_file_size(data_files[0])
        
        print('{:<8}{:>8}{:>10} rows'.format('(1/{})'.format(n_files), data_size, data_len))

    # If multiple files are detected, read iteratively and concatenate.
    else:
        dataset_list = []
        for i, ii in enumerate(data_files):

            if i == limit:
                print('\nFile constraint ({}) reached'.format(limit))
                
                break

            dataset_list.append(pd.read_parquet(ii))

            data_len = len(dataset_list[-1])
            data_size = _get_file_size(ii)

            print('{:<8}{:>8}{:>10} rows'.format('({}/{})'.format(i+1, n_files), data_size, data_len))
            
        print('Merging datasets...')
        data =  pd.concat(dataset_list)
        if reset_index:
            data.reset_index(drop=True, inplace=True)
        
    print('\nImported data shape: {}'.format(data.shape))
    return data



def _write_parquet(data, path, prefix, ind, n):
    '''
    Helper function to export parquet files.

    Parameters
    ----------

    data : DataFrame, shape (n_samples, n_features)
        Dataframe to be written.

    path : string
        Directory and file name to which data is to be written.

    prefix : string (default+'data')
        String appended to file name along with file index.

    ind : int
        Index number to append to file name.

    n : int
        Total number of chunks to write.
    '''

    data.to_parquet(path + '{}_{}.parquet'.format(prefix, ind+1))
    
    data_len = len(data)
    data_size = _get_file_size(path + '{}_{}.parquet'.format(prefix, ind+1))
    
    print('{:<8}{:>8}{:>10} rows'.format('({}/{})'.format(ind+1, n), data_size, data_len))



def write_parquets(data, path, chunks=1, prefix='data'):
    '''
    Exports parquet file(s).

    Parameters
    ----------

    data : DataFrame, shape (n_samples, n_features)
        Dataframe to be written.

    path : string
        Directory and file name to which data is to be written.

    chunks : int (default=1)
        Number of files across which to distribute written data.

    prefix : string (default+'data')
        String appended to file name along with file index.
    '''  

    print('Exported data shape: {}\n'.format(data.shape))
    print('Writing...')

    # Define row indicies for each chunk
    rows = np.linspace(0, len(data), chunks, dtype=int)
    n = len(rows)

    
    for i in range(n):
        # If only one chunk is defined, write the full dataset
        if i==n-1:
            data_out = data.iloc[rows[i]:]

            _write_parquet(data_out, path, prefix, i, n)

            break

        # If multiple chunks are defined, write datasets bound by two indices
        else:
            data_out = data.iloc[rows[i]:rows[i+1]]

            _write_parquet(data_out, path, prefix, i, n)
            

            