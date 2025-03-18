import os
import torch
import re
import pandas as pd
from torch_geometric.data import Data, Dataset
from utils import set_seed

#set_seed(42)

class DataTuple():
    def __init__(self, data):
        self.current_graph = data
        self.additional_graphs = []

    def set_target(self, target):
        self.current_graph.y = target

class GraphDatasetAggregator:
    def __init__(self, instance_graph_file_path, sub_instance_graphs_folder_path, log_file_path,
                 task_type_column_name='concept:name', target_column_name='remainingTime_hours', problem_type='regression', n_samples = 0, min_activity_current_graph = 3):

        self.instance_graph_file_path = instance_graph_file_path
        self.sub_instance_graphs_folder_path = sub_instance_graphs_folder_path
        self.log_file_path = log_file_path

        self.min_activity_current_graph = min_activity_current_graph

        self.n_samples = n_samples

        # load pandas dataframe from log file
        self.dataset = pd.read_csv(self.log_file_path)

        #check if column case:Rfp-id is in the log file
        if 'case:Rfp-id' in self.dataset.columns:
            self.dataset = self.dataset.rename(columns={'case:Rfp-id': 'case_id'})
            print('Changed case:Rfp-id column name')
        elif 'case:Rfpid' in self.dataset.columns:
            self.dataset = self.dataset.rename(columns={'case:Rfpid': 'case_id'})
            print('Changed case:Rfpid column name')

        elif 'case:concept:name' in self.dataset.columns:
            self.dataset = self.dataset.rename(columns={'case:concept:name': 'case_id'})
            print('Changed case:concept:name column name')
        else:
            raise ValueError('Column case:Rfp-id not found in log file')


        #rename first column (it's the new_index)
        self.dataset.rename(columns={self.dataset.columns[0]: 'new_index'}, inplace=True)

        self.problem_type = problem_type

        # ensure the  task_type_column_name is made of strings
        self.dataset[task_type_column_name] = self.dataset[task_type_column_name].astype(str)
        # get unique task types
        self.unique_task_types = self.dataset[task_type_column_name].unique()

        #if the problem is regression, targets are in the target_column_name
        # get unique target values as tensor
        if problem_type == 'regression':
            #self.dataset[target_column_name] = pd.to_numeric(self.dataset[target_column_name], errors='coerce')
            self.dataset['target'] = torch.tensor(self.dataset[target_column_name].values, dtype=torch.float)

        elif problem_type == 'classification':  #if the problem is classification, targets are to be extracted from the task_type_column_name of the next row
            # we group by 'case:Rfp-id' and create a new column containing the task_type_column_name of the next row in the group, None if it's the last
            self.dataset['target'] = self.dataset.groupby('case_id')[task_type_column_name].transform(lambda x: x.shift(-1))
            #drop rows with nan targets
            self.dataset.dropna(subset=['target'], inplace=True)

        else:
            raise ValueError("Invalid problem type")

        # get current graph identifiers in order
        self.current_graph_identifiers = self.dataset[['new_index', 'case_id', 'target']]

    def aggregate(self):
        # Aggregates the dataset
        pass

    def get_aggregated_dataset(self):
        return self.dataset

    def parse_graph_with_subgraphs_file(self):
        graphs = []
        #target_index = 0

        # Retrieve and sort filenames numerically
        filenames = os.listdir(self.sub_instance_graphs_folder_path)
        filenames.sort(key=lambda f: int(re.sub('\D', '', f)))

        for filename in filenames:
            #print(f'Opening file {filename}')
            file_path = os.path.join(self.sub_instance_graphs_folder_path, filename)
            is_current_graph = False  # flag to check if the current graph is the one being processed
            target_index = int(file_path.split('_')[-1].split('.')[0])  # get the target index from the filename
            #proceed only if the target_index is in the 'new_index' column of the dataset
            if target_index not in self.current_graph_identifiers['new_index'].values:
                print(f'Skipping file {target_index}')
                continue

            with open(file_path, 'r') as file:
                current_graph_found = False

                lines = file.readlines()

                observation = DataTuple(Data(x=torch.empty((0, len(self.unique_task_types))),
                                             edge_index=torch.empty((2, 0), dtype=torch.long)))
                num_graphs = 0  # current graph + sub_instance graphs


                current_graph_id = str(self.current_graph_identifiers.loc[self.current_graph_identifiers['new_index'] == target_index, 'case_id'].values[0]).strip().split(' ')[
                    -1]  # get the current graph id (case_rfp_id

                new_extra_graph = Data(x=torch.empty((0, len(self.unique_task_types))),
                                       edge_index=torch.empty((2, 0), dtype=torch.long))
                for line in lines:
                    parts = line.strip()
                    #ATTENTION! new datasets need to be addressed here
                    if parts.startswith('request') or parts.startswith('travel') or parts.startswith('declaration') or len(parts.split(' ')) == 1:  #Last condition is DANGEROUS (used for HelpDesk)
                        if new_extra_graph.x.size(0):
                            observation.additional_graphs.append(new_extra_graph)
                            new_extra_graph = Data(x=torch.empty((0, len(self.unique_task_types))),
                                                                      edge_index=torch.empty((2, 0), dtype=torch.long))
                        num_graphs += 1
                        if line.strip().split(' ')[-1] == current_graph_id:  # current graph
                            is_current_graph = True
                            current_graph_found = True

                            observation.set_target(self.current_graph_identifiers.loc[
                                                       self.current_graph_identifiers['new_index'] == target_index, 'target'].values[0])

                        else:
                            is_current_graph = False
                            new_extra_graph = Data(x=torch.empty((0, len(self.unique_task_types))),
                                                                      edge_index=torch.empty((2, 0), dtype=torch.long))

                    elif line.startswith('v'):
                        parts = line.strip().split(' ')
                        # vertex_id = int(parts[1]) - 1  # Adjusting vertex index to start from 0

                        # parts[2] contains the activity label
                        # the vertex attribute is a OOE of the activity labels
                        vertex_attr = torch.tensor([[1 if task == parts[2].strip() else 0 for task in self.unique_task_types]],
                                                   dtype=torch.float)

                        # to add other attributes, use something like vertex_attr = torch.cat([vertex_attr, ADDITIONAL_ATTRS], dim = 0)

                        if is_current_graph:
                            observation.current_graph.x = torch.cat([observation.current_graph.x, vertex_attr],
                                                                    dim=0) if observation.current_graph.x.size(
                                0) else vertex_attr
                        else:
                            new_extra_graph.x = torch.cat(
                                [new_extra_graph.x, vertex_attr], dim=0)

                    elif line.startswith('e'):
                        parts = line.strip().split(' ')
                        source_vertex, target_vertex = int(parts[1]) - 1, int(
                            parts[2].replace('\n', '')) - 1  # Adjusting indices to 0-based

                        edge_index = torch.tensor([[source_vertex], [target_vertex]], dtype=torch.long)
                        if is_current_graph:
                            observation.current_graph.edge_index = torch.cat(
                                [observation.current_graph.edge_index, edge_index],
                                dim=1) if observation.current_graph.edge_index.size(1) else edge_index
                        else:
                            # try simple solution for mismatch in edge index
                            v_ind = len(new_extra_graph.x) - 1  # when edges are calculated, all the vertex are already added
                            if source_vertex >= v_ind:
                                source_vertex = v_ind
                                edge_index = torch.tensor([[source_vertex], [target_vertex]], dtype=torch.long)
                            if target_vertex >= v_ind:
                                target_vertex = v_ind
                                edge_index = torch.tensor([[source_vertex], [target_vertex]], dtype=torch.long)

                            # in pytorch_geometric the vertexes are always a list of two lists ex: [[0, 1], [1, 2]]
                            # where the first list contains the source vertexes and the second list contains the target vertexes
                            new_extra_graph.edge_index = torch.cat(
                                [new_extra_graph.edge_index, edge_index], dim=1)

                # TODO: erase
                if observation.current_graph.edge_index.nelement() == 0:  # Initialize edge_index for the last subgraph if not already done
                    observation.current_graph.edge_index = torch.empty((2, 0), dtype=torch.long)

                if torch.equal(observation.current_graph.x, torch.empty((0, len(self.unique_task_types)))):
                    print('Empty graph')
                    continue #empty graphs are not added to the dataset
                elif observation.current_graph.x.size(0) < self.min_activity_current_graph:
                    print(f'Current graph has less than {self.min_activity_current_graph} activities')
                    continue

                #target_index += 1

                if current_graph_found and not str(observation.current_graph.y) == 'None': #if the target is none (in classification) we dont enter the element
                    graphs.append(observation)


                #reduced dataset
                if self.n_samples:
                    if target_index >= self.n_samples:
                        print("Max number of samples reached")
                        break

                #TODO: rewrite this condition to avoid unintended behaviors
                if str(observation.current_graph.y) == 'nan':
                    print('nan target removed')

        return graphs


class CustomDataset(Dataset):
    def __init__(self, data_tuples):
        super().__init__()
        self.data_tuples = data_tuples

    def __len__(self):
        return len(self.data_tuples)

    def __getitem__(self, idx):
        return self.data_tuples[idx]



class EarlyStopping:
    def __init__(self, patience=5, delta=0):
        self.patience = patience
        self.delta = delta
        self.counter = 0
        self.best_loss = None
        self.model = None
        self.early_stop = False

    def __call__(self, val_loss, model):
        if self.best_loss is None:
            self.best_loss = val_loss
            self.model = model
        elif val_loss > self.best_loss + self.delta:
            self.counter += 1
            if self.counter >= self.patience:
                self.early_stop = True
        else:
            self.best_loss = val_loss
            self.model = model
            self.counter = 0
