# Standard library imports
# Standard library imports
import os
import csv
from datetime import datetime, timedelta
import copy
import itertools

# Third-party library imports
import pandas as pd
import pm4py
import graphviz
import networkx as nx
import matplotlib.pyplot as plt


import os
import itertools
import re


# a class storing case,activity and timestamp for each event
class Event:
    def __init__(self, case, activity, timestamp):
        self.case = case
        self.activity = activity
        self.timestamp = timestamp




def split_list(lst, val):
    return [list(group) for k, group in itertools.groupby(lst, lambda x: x.strip() == val) if not k]


def add_second(date_object):
    try:
        in_format_time = datetime.strptime(str(date_object), '%Y-%m-%d %H:%M:%S.%f%z')
    except ValueError:
        in_format_time = datetime.strptime(str(date_object), '%Y-%m-%d %H:%M:%S%z')
    result = in_format_time + timedelta(0, 3)
    return result


def sub_second(date_object):
    try:
        in_format_time = datetime.strptime(str(date_object), '%Y-%m-%d %H:%M:%S.%f%z')
    except ValueError:
        in_format_time = datetime.strptime(str(date_object), '%Y-%m-%d %H:%M:%S%z')
    result = in_format_time - timedelta(0, 3)
    return result


input_file_path = 'BPIC15_5_Filtered.csv'

# Write to Pandas Dataframe
#log = pm4py.read_xes(input_file_path)
df = pd.read_csv(input_file_path, sep=';',header=0)
#df = pm4py.convert_to_dataframe(log)

df = df[['Activity', 'time:timestamp', 'case:concept:name']]

df = df.rename(columns={'Activity': 'concept:name'})

df = df[~df['concept:name'].isin(['Start', 'End'])]
df = df.reset_index(drop=True)
case_node_thresholds = df['case:concept:name'].value_counts().to_dict()
#df = df[0:10000]
print(df.head())
#df = df[df['case:Rfp-id'].str.strip() == 'request for payment 73550']
#df.to_csv('prova.csv')
#hhhh

# useful for mapping with instance-graphs file
df['case_number_id_graphs'] = (
    'instance_graph_' + (df['case:concept:name'] != df['case:concept:name'].shift()).cumsum().astype(str)
)
filtered_df = df[df["case_number_id_graphs"] == "instance_graph_1"]
print(filtered_df)

# Add the remaining time feature
# compute a list of all the Case ID
listaCaseID = df["case:concept:name"].unique()

# Create dictionary for max timestamp per case
dCaTi = df.groupby("case:concept:name")["time:timestamp"].max().astype(str).to_dict()


def calculate_remaining_time(row):
    try:
        max_time = datetime.strptime(str(dCaTi[row["case:concept:name"]]), '%Y-%m-%d %H:%M:%S.%f%z')
    except ValueError:
        max_time = datetime.strptime(str(dCaTi[row["case:concept:name"]]), '%Y-%m-%d %H:%M:%S%z')
    try:
        actual_time = datetime.strptime(str(row["time:timestamp"]), '%Y-%m-%d %H:%M:%S.%f%z')
    except ValueError:
        actual_time = datetime.strptime(str(row["time:timestamp"]), '%Y-%m-%d %H:%M:%S%z')
    return (max_time - actual_time).total_seconds()


df['remainingTime_sec'] = df.apply(calculate_remaining_time, axis=1)


# Add columns for remaining time in minutes, hours, and days
df['remainingTime_minutes'] = df['remainingTime_sec'] / 60
df['remainingTime_hours'] = df['remainingTime_sec'] / 3600
df['remainingTime_days'] = df['remainingTime_sec'] / 86400



# Add columns for
# 1) time between event and previous event
# 2) time between event and start event
# 3) time between event and previous sunday at midnight. (00:00)

# 2) time between event and start event

# Create dictionary for min timestamp per case
#dStartTi = df.groupby("case:concept:name")["time:timestamp"].min().astype(str).to_dict()


#def calculate_time_from_start(row):
    #try:
    #    min_time = datetime.strptime(str(dStartTi[row["case:concept:name"]]), '%Y-%m-%d %H:%M:%S.%f%z')
    #except ValueError:
    #    min_time = datetime.strptime(str(dStartTi[row["case:concept:name"]]), '%Y-%m-%d %H:%M:%S%z')
    #try:
    #    actual_time = datetime.strptime(str(row["time:timestamp"]), '%Y-%m-%d %H:%M:%S.%f%z')
   #except ValueError:
    #    actual_time = datetime.strptime(str(row["time:timestamp"]), '%Y-%m-%d %H:%M:%S%z')
    #return (actual_time-min_time).total_seconds()


#df['Time_from_Start_sec'] = df.apply(calculate_time_from_start, axis=1)


# 1) time between event and previous event


# ---------------------------------------------------------------------------------------

df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
df['time:timestamp'] = df['time:timestamp'].dt.round('1s')
df.sort_values(by=['case:concept:name', 'time:timestamp'], inplace=True)
df = df.reset_index(drop=True)
df['Index'] = df.index
df.sort_values(by=['time:timestamp', 'Index'], inplace=True)


# Trova la data minima nel dataset
start_date = df['time:timestamp'].min()

# Definisci la data limite (un mese dopo)
end_date = start_date + pd.DateOffset(months=22)

# Filtra solo gli eventi tra start_date e end_date
df = df[(df['time:timestamp'] >= start_date) & (df['time:timestamp'] < end_date)]

#print(df)
#filtered_df_1 = df[df["case_number_id_graphs"] == "instance_graph_2"]
#print(filtered_df_1)

# df["Status_ALL"] = None

dCaLE = {}  # dictionary of Cases and List of Events occurred
# i = 0
inner_list = []

for index, r in df.iterrows():
    print(index)
    cID = str(r['case:concept:name']).strip()
    print(cID)
    act = str(r['concept:name']).strip().replace(' ', '')
    print(act)
    date = r['time:timestamp']
    print(date)

    ev = Event(cID, act, date)

    if cID in dCaLE:
        lista = copy.deepcopy(dCaLE[cID])  # .append(ev)
        newL = []
        for item in lista:
            newL.append(item)
        newL.append(ev.activity)


        dCaLE[cID] = copy.deepcopy(newL)
    else:
        dCaLE[cID] = copy.deepcopy([ev.activity])

    if len(dCaLE[cID]) >= case_node_thresholds[int(cID)]:
        del dCaLE[cID]

    state = copy.deepcopy(dCaLE)

    inner_list.append(state)

df["Status_ALL"] = inner_list

# mapping creation to map case ID to instance-graph ID
mapping = df[["case:concept:name", "case_number_id_graphs"]].drop_duplicates()

status = df['Status_ALL'].tolist()

if not os.path.exists("Instance_graphs"):
    os.makedirs("Instance_graphs")

# We first need to open the IG_file in reading mode
# Define file paths
input_file = "BPIC15_5_Filtered.g"
output_dir = "Instance_graphs"
os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists

# Read the file
with open(input_file, "r") as file:
    lines = file.readlines()

# Split the list based on the 'case id {number}' pattern
case_id_pattern = re.compile(r"^case id \d+")
instance_graphs = []
current_graph = []

for line in lines:
    if case_id_pattern.match(line):  # New instance graph detected
        if current_graph:
            instance_graphs.append(current_graph)
        current_graph = [line]  # Start a new instance graph
    else:
        current_graph.append(line)

if current_graph:  # Append the last graph if any
    instance_graphs.append(current_graph)

# Save each instance graph to a separate file
for i, graph in enumerate(instance_graphs, start=1):
    graph_file_path = os.path.join(output_dir, f"instance_graph_{i}.g")
    with open(graph_file_path, "w") as graph_file:
        graph_file.writelines(graph)

print(f"Successfully split {len(instance_graphs)} instance graphs.")
# We want to create a function that creates sub-graphs of instance graphs
# Will create "Sub_Instance_graphs" directory if it does not exit
if not os.path.exists("Sub_Instance_graphs"):
    os.makedirs("Sub_Instance_graphs")

df.sort_values(by=['Index'], inplace=True)
print(df['Status_ALL'].iloc[1])

hh
for index, r in df.iterrows():
    prova = r['Status_ALL']
    list_to_graph = []
    for key in prova.keys():
        graph = mapping.loc[mapping["case:concept:name"].astype(str) == key]["case_number_id_graphs"].tolist()[0]
        print(graph)
        graph_number = graph.split('_')[2]
        print(f'Il numero del grafo è {graph_number}')
        graph_path = f'Instance_graphs/instance_graph_{graph_number}.g'
        print(graph_path)
        try:
            with open(graph_path, 'r') as file:
                testo = file.readlines()
                inner_list = []
                pluto = 1
                node_list = []
                for i in testo:
                    for j in prova[key]:
                        if j in i:
                            if i[0] == 'v':
                                node = int(i.strip().split(' ')[1])
                                if pluto == node:
                                    node_list.append(node)
                                    if i not in inner_list:
                                        inner_list.append(i)
                                        pluto = pluto + 1
                            else:
                                arc = i.strip().split(' ')[1:3]
                                if int(arc[0]) in node_list:
                                    if int(arc[1]) in node_list:
                                        if i not in inner_list:
                                            inner_list.append(i)
                inner_list.insert(0, f'{key}\n')
                inner_list.append('\n')
                list_to_graph = list_to_graph + inner_list
        except:
            print(f'Instance_graph_{graph} does not exist!')
            continue
    with open(f'Sub_Instance_graphs/sub_instance_graph_{index}.g', 'w') as f:
        f.writelines(list_to_graph)

df.to_csv('log_ordered_by_index')

