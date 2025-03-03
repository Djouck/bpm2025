# Standard Library imports

import os
import csv
from datetime import datetime, timedelta
import copy
import itertools
import re

# Third party Library imports
import pandas as pd
import pm4py
import graphviz
import networkx as nx
import matplotlib.pyplot as plt


# a class storing case,activity and timestamp for each event
class Event:
    def __init__(self, case, activity, timestamp):
        self.case = case
        self.activity = activity
        self.timestamp = timestamp


def split_list(lst, val):
    return [list(group) for k, group in itertools.groupby(lst, lambda x: x.strip() == val) if not k]


# Write to pandas dataframe
input_file_path = 'BPIC15_5_Final_Data_File_Reg.xes'
log = pm4py.read_xes(input_file_path)
df = pm4py.convert_to_dataframe(log)


# retrieve only useful columns
df = df[['case:concept:name', 'concept:name', 'time:timestamp']]



