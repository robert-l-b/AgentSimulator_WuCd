# Script to provide additional functions to allow for a warm-up and a cool-down phase of AgentSimulator not affecting the simulated event logs

import math
import numpy as np
import pandas as pd
import pickle
import os




def get_wip_threshold(df, timedelta='D'):
    # make copy to avoid any format issues with agent simulator
    df = df.copy()
    wip_list = get_wip(df, timedelta)
    return wip_list

def get_wip(df, timedelta):

    # set timedelta
    if timedelta == 'H':
        window_size: pd.Timedelta = pd.Timedelta(hours=1)
    elif timedelta == 'D':
        window_size: pd.Timedelta = pd.Timedelta(days=1)
    elif timedelta == 'W':
        window_size: pd.Timedelta = pd.Timedelta(weeks=1)
    else:
        raise ValueError('timedelta should be one of H, D or W')
    

    # format timestamps
    df['start_timestamp'] = pd.to_datetime(df['start_timestamp'], format='mixed', utc=True)
    df['end_timestamp']   = pd.to_datetime(df['end_timestamp'], format='mixed', utc=True)

    # Get timeline (reset to day in case daily frequency is used)
    start = df['start_timestamp'].min().floor(freq='24H')
    end   = df['end_timestamp'].max().ceil(freq='24H')
    
    # Transform event logs to cases
    cases = []
    for _case_id, events in df.groupby('case_id'):
        cases += [{'start': events['start_timestamp'].min(), 'end': events['end_timestamp'].max()}]
    cases = pd.DataFrame(cases)
    # Go over each bin computing the active area
    wip = {}
    wip_complete   = []
    for offset in range(math.ceil((end - start) / window_size)):
        current_window_start = start + window_size * offset
        current_window_end = current_window_start + window_size
        # Compute overlapping intervals (0s if no overlapping)
        within_window = (np.minimum(cases['end'], current_window_end) - np.maximum(cases['start'], current_window_start))
        # Sum positive ones (within the current window) and normalize area
        wip_value = sum(within_window[within_window > pd.Timedelta(0)], pd.Timedelta(0)) / window_size
        if wip_value > 0:
            wip[offset] = wip_value
        wip_complete.append(wip_value)

    return wip_complete



def cut_after_cool_down(df, nr_cases):
    
    # Group by 'case_id' and calculate the minimum 'start_timestamp' for each group
    grouped = df.groupby('case_id').agg(min_start_timestamp=('start_timestamp', 'min')).reset_index()

    # Sort the groups based on the minimum 'start_timestamp'
    sorted_case_ids = grouped.sort_values('min_start_timestamp')['case_id']

    # Get the top 'nr_cases' case_ids
    selected_case_ids = sorted_case_ids.head(nr_cases)

    # Filter the original DataFrame based on the selected 'case_id's
    filtered_df = df[df['case_id'].isin(selected_case_ids)]

    # Sort the filtered DataFrame by 'case_id' first and then by 'start_timestamp'
    filtered_df = filtered_df.sort_values(by=['case_id', 'start_timestamp'])

    return filtered_df


def cut_after_warm_up(df, cutoff_timestamp):
    # Group by 'case_id' and calculate the minimum 'start_timestamp' for each group
    grouped = df.groupby('case_id').agg(min_start_timestamp=('start_timestamp', 'min')).reset_index()

    # Filter the groups based on the cutoff timestamp
    valid_case_ids = grouped[grouped['min_start_timestamp'] >= cutoff_timestamp]['case_id']

    # Filter the original DataFrame based on the valid 'case_id's
    filtered_df = df[df['case_id'].isin(valid_case_ids)]

    # Sort the filtered DataFrame by 'case_id' first and then by 'start_timestamp'
    filtered_df = filtered_df.sort_values(by=['case_id', 'start_timestamp'])

    return filtered_df



def save_wip(file_path, identifier, wip):
    # Check if the file exists; if so, load the existing data
    if os.path.exists(file_path):
        with open(file_path, 'rb') as file:
            data = pickle.load(file)
    else:
        # Initialize an empty dictionary if the file does not exist
        data = {}

    # # Check if the identifier already exists
    # if identifier in data:
    #     # print(f"Identifier '{identifier}' already exists. No changes made.")
    #     pass
    # else:
    # Update the data with the new identifier and list
    data[identifier] = wip

    # Save the updated data back to the file
    with open(file_path, 'wb') as file:
        pickle.dump(data, file)
    # print(f"Saved under identifier '{identifier}'.")


def save_hyperparameter(file_path, file_name, discover_delays, central_orchestration):
    # Check if the file exists; if so, load the existing data
    if os.path.exists(file_path):
        with open(file_path, 'rb') as file:
            data = pickle.load(file)
    else:
        # Initialize an empty dictionary if the file does not exist
        data = {}

    # if len(list(data.keys()))>0:
    if file_name not in data.keys():
        data[file_name] = {}
        data[file_name]['discover_delays'] = discover_delays
        data[file_name]['central_orchestration'] = central_orchestration
            

        # Save the updated data back to the file
        with open(file_path, 'wb') as file:
            pickle.dump(data, file)


def check_hyperparameter(file_path, file_name):
    # Load data if the file exists
    discover_delays = None
    central_orchestration = None

    if os.path.exists(file_path):
        with open(file_path, 'rb') as file:
            data = pickle.load(file)
        if len(list(data.keys()))>0:
            if file_name in data.keys():
                discover_delays = data[file_name]['discover_delays']
                central_orchestration = data[file_name]['central_orchestration']
            
    return discover_delays, central_orchestration