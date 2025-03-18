import argparse
import os
import pickle
import random
import torch
import torch_geometric

import numpy as np

from matplotlib import pyplot as plt
from sklearn.metrics import r2_score
from metrics import calculate_f1_score, calculate_roc_auc, calculate_mse

def initialize_weights(m):
    if isinstance(m, torch.nn.Linear) or isinstance(m, torch.nn.Conv2d):
        torch.nn.init.kaiming_uniform_(m.weight, nonlinearity='relu')
        if m.bias is not None:
            torch.nn.init.constant_(m.bias, 0)

def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)  # if you are using multi-GPU.
    os.environ['PYTHONHASHSEED'] = str(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.enabled = False
    torch_geometric.seed_everything(seed)
    torch.use_deterministic_algorithms(True)

set_seed(42)

def one_hot_encoding_targets(unique_task_types, task_type):
    one_hot = [1 if task == task_type else 0 for task in unique_task_types]
    return one_hot



def simple_batching(data_tuples, batch_size):
    for i in range(0, len(data_tuples), batch_size):
        batch = data_tuples[i:i + batch_size]
        batch_current_graph = [item.current_graph for item in batch]
        batch_additional_graphs = [item.additional_graphs for item in batch]
        yield {'current_graph': batch_current_graph, 'additional_graphs': batch_additional_graphs}


def stratified_batching(data_tuples, batch_size):
    # Step 1: Create a dictionary to store lists of graphs for each target class
    class_dict = {}
    for data in data_tuples:
        label = data.current_graph.y
        if label not in class_dict:
            class_dict[label] = []
        class_dict[label].append(data)

    # Step 2: Shuffle the lists to ensure randomness
    for label in class_dict:
        random.shuffle(class_dict[label])

    # Step 3: Extract batches while maintaining class distribution
    batches = []
    while any(class_dict.values()):
        batch = []
        for label in list(class_dict.keys()):
            if class_dict[label]:
                batch.append(class_dict[label].pop())
                if len(batch) == batch_size:
                    batches.append(batch)
                    batch = []
        if batch:
            batches.append(batch)

    # Step 4: Yield batches
    for batch in batches:
        batch_current_graph = [item.current_graph for item in batch]
        batch_additional_graphs = [item.additional_graphs for item in batch]
        yield {'current_graph': batch_current_graph, 'additional_graphs': batch_additional_graphs}


def check_for_nans(data):
    for el in data:
        if torch.isnan(el.current_graph.x).any() or torch.isnan(el.current_graph.edge_index).any():
            print("NaNs found in input data")
            return True
    return False


def get_graphs(path):
    # Check if the file exists and is not empty
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            with open(path, 'rb') as f:
                g = pickle.load(f)
                print("File loaded successfully.")
        except EOFError:
            print("EOFError: The file could not be read. It may be corrupted.")
    else:
        print("The file does not exist or is empty.")
    return g


def split_target(dataset, percentage):
    # Step 1: Create a dictionary to store lists of graphs for each target class
    class_dict = {}
    for data in dataset:
        label = data.current_graph.y
        if label not in class_dict:
            class_dict[label] = []
        class_dict[label].append(data)

    # Step 2: Split each list of graphs into training and testing sets
    train_set = []
    test_set = []
    for label, graphs in class_dict.items():
        split_index = int(len(graphs) * percentage)
        train_set.extend(graphs[:split_index])
        test_set.extend(graphs[split_index:])

    return random.shuffle(train_set), random.shuffle(test_set)

from collections import Counter

def get_class_weights(dataset):
    # Extract target labels from the dataset
    targets = [data.current_graph.y for data in dataset]

    # Count occurrences of each class
    class_counts = Counter(targets)

    # Compute class weights as the inverse of class frequencies
    total_samples = len(targets)
    class_weights = {cls: total_samples / count for cls, count in class_counts.items()}

    # Normalize class weights
    max_weight = max(class_weights.values())
    class_weights = {cls: weight / max_weight for cls, weight in class_weights.items()}

    # Convert to tensor
    class_weights_tensor = torch.tensor([class_weights[cls] for cls in sorted(class_weights.keys())], dtype=torch.float)

    return class_weights_tensor



def split_dataset(dataset, train_percentage, val_percentage, stratify=True):
    if stratify:
        # Step 1: Create a dictionary to store lists of graphs for each target class
        class_dict = {}
        for data in dataset:
            label = data.current_graph.y
            if label not in class_dict:
                class_dict[label] = []
            class_dict[label].append(data)

        # Step 2: Split each list of graphs into training, validation, and testing sets
        train_set = []
        val_set = []
        test_set = []
        for label, graphs in class_dict.items():
            train_split = int(len(graphs) * train_percentage)
            val_split = int(len(graphs) * (train_percentage + val_percentage))
            train_set.extend(graphs[:train_split])
            val_set.extend(graphs[train_split:val_split])
            test_set.extend(graphs[val_split:])

            random.shuffle(train_set)
            random.shuffle(val_set)
            random.shuffle(test_set)

    else:
        # Step 1: Shuffle the dataset
        random.shuffle(dataset)

        # Step 2: Split the dataset into training, validation, and testing sets
        train_split = int(len(dataset) * train_percentage)
        val_split = int(len(dataset) * (train_percentage + val_percentage))
        train_set = dataset[:train_split]
        val_set = dataset[train_split:val_split]
        test_set = dataset[val_split:]

    return train_set, val_set, test_set


def plot_metric_by_prefix_length(metric, metric_name, title, problem_spec):
    #metric = metric['predictions']
    # Step 1: Extract the metric values for each prefix length
    if metric_name == 'f1_score':
        metric_values = [calculate_f1_score(torch.stack(metric['predictions'][prefix_length]), torch.stack(metric['targets'][prefix_length]).argmax(dim=1)) for prefix_length in sorted(metric['predictions'].keys())]
    elif metric_name == 'roc_auc':
        metric_values = [calculate_roc_auc(torch.stack(metric['predictions'][prefix_length]), torch.stack(metric['targets'][prefix_length]).argmax(dim=1)) for prefix_length in sorted(metric['predictions'].keys())]
    elif metric_name == 'mse':
        metric_values = [calculate_mse(metric['predictions'][prefix_length], metric['targets'][prefix_length]) for prefix_length in sorted(metric['predictions'].keys())]
    elif metric_name == 'r2':
        metric_values = [r2_score(metric[prefix_length]['targets'], metric[prefix_length]['predictions']) for prefix_length in sorted(metric.keys())]
    else:
        raise ValueError("Invalid metric name. Please choose from: 'roc_auc', 'accuracy', 'precision', 'recall', 'f1_score', 'r2'.")


    # Step 2: Plot the metric values as bar plot
    plt.figure()
    plt.bar(np.arange(len(metric_values)), metric_values)
    #plt.title(title)
    plt.xlabel('Prefix Length')
    plt.ylabel(metric_name)
    plt.show()

    #save x_ax and y_ax to a file
    np.save(f'./data/{problem_spec}_{metric_name}.npy', metric_values)


# Normalize the input data
def normalize_data(data):
    mean = data.mean(dim=0, keepdim=True)
    std = data.std(dim=0, keepdim=True)
    return (data - mean) / std

# Normalize the target values
def fit_normalize_targets(targets):
    mean = targets.mean()
    std = targets.std()
    return (targets - mean) / std, mean, std

def apply_normalization(targets, mean, std):
    return (targets - mean) / std

def denormalize_predictions(predictions, mean, std):
    return predictions * std + mean


def parse_args():
    parser = argparse.ArgumentParser(description='Training arguments')
    parser.add_argument('--problem_name', type=str, help='Name of the problem')
    parser.add_argument('--problem_type', type=str, choices=['regression', 'classification'], help='Type of the problem')
    parser.add_argument('--generate_dataset', action='store_true', help='Flag to generate dataset')
    parser.add_argument('--use_extra_graphs', action='store_true', help='Flag to use extra graphs')
    parser.add_argument('--n_epochs', type=int, help='Number of epochs')
    parser.add_argument('--reduced_dataset', action='store_true', help='Flag to use reduced dataset')
    parser.add_argument('--batch_size', type=int, help='Batch size')
    parser.add_argument('--lr', type=float, help='Learning rate')
    return parser.parse_args()


def save_results_to_file(results, train_args):
    filename_completion = "without_extra_graphs"
    if train_args["use_extra_graphs"]:
        filename_completion = "with_extra_graphs"
    elif train_args["use_n_graphs"]:
        filename_completion = "with_n_graphs"

    results_file_path = f"results_{train_args['problem_name']}_{train_args['problem_type']}_{filename_completion}.txt"

    with open(results_file_path, 'w') as f:
        for key, value in results.items():
            f.write(f"{key}: {value}\n")