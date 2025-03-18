import pickle
import torch
import numpy as np

from data_structures import GraphDatasetAggregator, CustomDataset, EarlyStopping
from networks import GraphRegression, GraphClassification, GraphClassificationM
from metrics import calculate_accuracy, calculate_precision, calculate_recall, calculate_f1_score, calculate_roc_auc, \
    calculate_mse, calculate_r2_score
from utils import split_dataset, check_for_nans, simple_batching, one_hot_encoding_targets, get_graphs, \
    plot_metric_by_prefix_length, parse_args, fit_normalize_targets, apply_normalization, set_seed, initialize_weights, \
    save_results_to_file

#set_seed(42)
# Disable multi-threading in PyTorch
torch.set_num_threads(1)
torch.set_num_interop_threads(1)



if __name__ == '__main__':
    train_args = {
        'problem_name': 'RFP', #possible names: 'prepaid', 'dataset_3', 'dataset_4', 'InternationalDeclarations', 'HelpDesk'
        'problem_type': 'regression', #'regression' or 'classification'
        'generate_dataset': False,
        'use_extra_graphs': True,
        'use_n_graphs': False, #if True, the number of graphs the sample will be used in the decoder (overrides use_extra_graphs)
        'n_epochs': 10,
        'reduced_dataset': False,
        'batch_size': 64,
        'lr': 0.001
    }


    args = parse_args()

    # Update train_args with provided command-line arguments
    if args.problem_name:
        train_args['problem_name'] = args.problem_name
    if args.problem_type:
        train_args['problem_type'] = args.problem_type
    if args.generate_dataset:
        train_args['generate_dataset'] = args.generate_dataset
    if args.use_extra_graphs:
        train_args['use_extra_graphs'] = args.use_extra_graphs
    if args.n_epochs is not None:
        train_args['n_epochs'] = args.n_epochs
    if args.reduced_dataset:
        train_args['reduced_dataset'] = args.reduced_dataset
    if args.batch_size is not None:
        train_args['batch_size'] = args.batch_size
    if args.lr is not None:
        train_args['lr'] = args.lr

    #print the training arguments
    print(train_args)


    problem_specification = f"{train_args['problem_name']}_{train_args['problem_type']}_{'extra_graphs' if train_args['use_extra_graphs'] else 'no_extra_graphs'}_{train_args['use_n_graphs']}"

    if train_args['use_extra_graphs']:
        print("Training with extra graphs")
    else:
        print("Training without extra graphs")

    if train_args['problem_name'] == 'prepaid':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/prepaid/PrepaidTravelCost_instance_graphs_withPromConformance.g'
            sub_instance_graphs_folder_path = './data/prepaid/Sub_Instance_graphs/Sub_Instance_graphs'
            log_file_path = './data/prepaid/log_ordered_by_index.csv'

            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                              log_file_path, problem_type = train_args['problem_type'])#, n_samples=n_samples)
            #graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            if train_args['reduced_dataset']:
                with open(f"data/graphs_minimal_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:
                    pickle.dump(graphs, file)
            else:
                with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:  # with open('data/graphs_minimal.pkl', 'wb') as file:
                    pickle.dump(graphs, file)
        else:
            if train_args['reduced_dataset']:
                file_path = f"data/graphs_minimal_{train_args['problem_name']}_{train_args['problem_type']}.pkl"
            else:
                file_path = f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl"

            graphs = get_graphs(file_path)

    elif train_args['problem_name'] == 'TP':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/TP/PermitLog_SE_noSpace.g' #snellius path
            sub_instance_graphs_folder_path = './data/TP/Sub_Instance_graphs/Sub_Instance_graphs'
            log_file_path = './data/TP/log_ordered_by_index.csv'
            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                              log_file_path, problem_type = train_args['problem_type'], n_samples = 10000)
            graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:
                pickle.dump(graphs, file)
        else:
            graphs = get_graphs(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl")

    elif train_args['problem_name'] == 'RFP':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/RFP/RFP.g'
            sub_instance_graphs_folder_path = './data/RFP/Sub_Instance_graphs/Sub_Instance_graphs'
            log_file_path = './data/RFP/log_ordered_by_index.csv'
            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                                log_file_path, target_column_name='remaining_time_hours', problem_type = train_args['problem_type'])
            graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:
                pickle.dump(graphs, file)
        else:
            graphs = get_graphs(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl")

    elif train_args['problem_name'] == 'InternationalDeclarations':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/InternationalDeclarations/InternationalDeclarations.g'
            sub_instance_graphs_folder_path = './data/InternationalDeclarations/Sub_Instance_graphs/Sub_Instance_graphs'
            log_file_path = './data/InternationalDeclarations/log_ordered_by_index.csv'
            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                              log_file_path, problem_type = train_args['problem_type'])
            graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:
                pickle.dump(graphs, file)
        else:
            graphs = get_graphs(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl")

    elif train_args['problem_name'] == 'HelpDesk':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/HelpDesk/HelpDesk.g'
            sub_instance_graphs_folder_path = './data/HelpDesk/Sub_Instance_graphs'
            log_file_path = './data/HelpDesk/log_ordered_by_index.csv'
            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                              log_file_path, problem_type = train_args['problem_type'])
            graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:
                pickle.dump(graphs, file)
        else:
            graphs = get_graphs(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl")
    elif train_args['problem_name'] == 'BPI17c_approved':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/BPI17c_approved/HelpDesk.g'
            sub_instance_graphs_folder_path = './data/BPI17c_approved/Sub_Instance_graphs/Sub_Instance_graphs'
            log_file_path = './data/BPI17c_approved/log_ordered_by_index.csv'
            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                              log_file_path, problem_type = train_args['problem_type'])
            graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:
                pickle.dump(graphs, file)
        else:
            graphs = get_graphs(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl")
    elif train_args['problem_name'] == 'BPIC17':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/BPIC17/HelpDesk.g'
            sub_instance_graphs_folder_path = './data/BPIC17/Sub_Instance_graphs/Sub_Instance_graphs'
            log_file_path = './data/BPIC17/log_ordered_by_index.csv'
            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                              log_file_path, problem_type = train_args['problem_type'])
            graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:

                pickle.dump(graphs, file)
        else:
            graphs = get_graphs(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl")

    elif train_args['problem_name'] == 'BPIW12':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/BPIW12/HelpDesk.g'
            sub_instance_graphs_folder_path = './data/BPIW12/Sub_Instance_graphs/Sub_Instance_graphs'
            log_file_path = './data/BPIW12/log_ordered_by_index.csv'
            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                              log_file_path, problem_type = train_args['problem_type'])
            graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:

                pickle.dump(graphs, file)
        else:
            graphs = get_graphs(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl")

    elif train_args['problem_name'] == 'BPI12_SE':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/BPI12_SE/HelpDesk.g'
            sub_instance_graphs_folder_path = './data/BPI12_SE/Sub_Instance_graphs/Sub_Instance_graphs'
            log_file_path = './data/BPI12_SE/log_ordered_by_index.csv'
            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                              log_file_path, problem_type=train_args['problem_type'])
            graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:

                pickle.dump(graphs, file)
        else:
            graphs = get_graphs(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl")
    elif train_args['problem_name'] == 'BPI15_1':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/BPI15_1/BPI15_5.g'
            sub_instance_graphs_folder_path = './data/BPI15_1/Sub_Instance_graphs/Sub_Instance_graphs'
            log_file_path = './data/BPI15_1/log_ordered_by_index.csv'
            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                              log_file_path, problem_type=train_args['problem_type'])
            graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:

                pickle.dump(graphs, file)
        else:
            graphs = get_graphs(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl")

    elif train_args['problem_name'] == 'BPIC11':
        if train_args['generate_dataset']:
            instance_graph_file_path = './data/BPIC11/BPIC11.g'
            sub_instance_graphs_folder_path = './data/BPIC11/Sub_Instance_graphs/Sub_Instance_graphs'
            log_file_path = './data/BPIC11/log_ordered_by_index.csv'
            graph_dataset_aggregator = GraphDatasetAggregator(instance_graph_file_path, sub_instance_graphs_folder_path,
                                                              log_file_path, problem_type=train_args['problem_type'])
            graph_dataset_aggregator.aggregate()
            graphs = graph_dataset_aggregator.parse_graph_with_subgraphs_file()

            # save graphs as a pickle file
            with open(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl", 'wb') as file:

                pickle.dump(graphs, file)
        else:
            graphs = get_graphs(f"./data/graphs_{train_args['problem_name']}_{train_args['problem_type']}.pkl")



    if train_args['problem_type'] == 'classification':
        train_dataset, val_dataset, test_dataset = split_dataset(graphs, 0.7, 0.15)
    elif train_args['problem_type'] == 'regression':
        train_dataset, val_dataset, test_dataset = split_dataset(graphs, 0.7, 0.15, stratify=False)

    # split the graphs in 67% training and 33% testing
    #train_dataset = graphs[:int(len(graphs) * 0.67)]



    if train_args['problem_type'] == 'classification':
        unique_classes =  sorted(list(set([graph.current_graph.y for graph in graphs])))

    # print percentage of targets of each class in the training set
    #if train_args['problem_type'] == 'classification':
    #    for target in unique_classes:
    #        print(f'Percentage of targets of class {target}: {len([graph for graph in train_dataset if graph.current_graph.y == target]) / len(train_dataset)}')


    if train_args['problem_type'] == 'regression':
        targets = np.array([graph.current_graph.y for graph in train_dataset])
        targets, target_mean, target_std = fit_normalize_targets(targets)
        for i, graph in enumerate(train_dataset):
            graph.current_graph.y = targets[i]

    train_dataset = CustomDataset(train_dataset)
    val_dataset = CustomDataset(val_dataset)

    #test_dataset = graphs[int(len(graphs) * 0.67):]

    print(f'Training dataset size: {len(train_dataset)}')
    print(f'Test dataset size: {len(test_dataset)}')

    # Check train and test datasets
    if check_for_nans(train_dataset) or check_for_nans(test_dataset):
        raise ValueError("NaNs found in the dataset")
    else:
        print("No NaNs found in the dataset")


    if train_args['problem_type'] == 'regression':
        if not train_args['use_n_graphs']:
            model = GraphRegression(use_extra_graphs=train_args['use_extra_graphs'])
        else:
            model = GraphRegression(use_extra_graphs=False, use_n_graphs=train_args['use_n_graphs'])

    elif train_args['problem_type'] == 'classification':
        if not train_args['use_n_graphs']:
            model = GraphClassificationM(original_classes=unique_classes, use_extra_graphs=train_args['use_extra_graphs'])
        else:
            model = GraphClassificationM(original_classes=unique_classes, use_extra_graphs=False, use_n_graphs=train_args['use_n_graphs'])

    model.train()

    optimizer = torch.optim.Adam(model.parameters(), lr=train_args['lr'], amsgrad=True)
    batch_size = train_args['batch_size']
    criterion = torch.nn.CrossEntropyLoss()
    early_stopping = EarlyStopping(patience=10, delta=0.01)

    for epoch in range(train_args['n_epochs']):
        model.train()
        loss_list = []
        for batch in simple_batching(train_dataset, batch_size):#stratified_batching(train_dataset, batch_size):

            optimizer.zero_grad()
            out = model(batch)

            # Assuming the target is stored in the current graph's `y` attribute
            if train_args['problem_type'] == 'regression':
                batch_y = torch.stack([torch.tensor(graph.y) for graph in batch['current_graph']])
                loss = torch.nn.functional.mse_loss(out.squeeze(), batch_y)

            elif train_args['problem_type'] == 'classification':
                #batch_y = torch.stack([torch.tensor(one_hot_encoding_targets(model.unique_classes, graph.y)) for graph in batch['current_graph']]).float()
                #loss = torch.nn.functional.cross_entropy(out.squeeze(1), batch_y)#, weight=class_weights)
                batch_y = torch.stack(
                    [torch.tensor(one_hot_encoding_targets(model.unique_classes, graph.y)) for graph in
                     batch['current_graph']]).float()
                batch_y = batch_y.argmax(dim=1)
                loss = torch.nn.functional.nll_loss(out, batch_y)

            if torch.isnan(loss):
                print("NaN loss detected")
                continue

            loss.backward()
            #torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)  # Gradient clipping
            loss_list.append(loss.item())
            optimizer.step()
        print(
            f'Epoch: {epoch}, Loss: {np.mean(loss_list)}, Std: {np.std(loss_list)}, Max: {np.max(loss_list)}, Min: {np.min(loss_list)}')

        # Validate the model
        model.eval()
        val_loss_list = []
        for batch in simple_batching(val_dataset, 1):
            out = model(batch)

            if train_args['problem_type'] == 'regression':
                batch_y = apply_normalization(torch.stack([torch.tensor(graph.y) for graph in batch['current_graph']]), target_mean, target_std)
                val_loss = torch.nn.functional.mse_loss(out.squeeze(-1), batch_y)
            elif train_args['problem_type'] == 'classification':
                batch_y = torch.stack([torch.tensor(one_hot_encoding_targets(model.unique_classes, graph.y)) for graph in batch['current_graph']]).float()
                #val_loss = torch.nn.functional.cross_entropy(out.squeeze(1), batch_y)
                batch_y = batch_y.argmax(dim=1)
                #loss = torch.nn.functional.nll_loss(out.squeeze(1), batch_y)
                val_loss = torch.nn.functional.nll_loss(out.squeeze(1), batch_y)

            val_loss_list.append(val_loss.item())
        print(
            f'Validation Loss: {np.mean(val_loss_list)}, Std: {np.std(val_loss_list)}, Max: {np.max(val_loss_list)}, Min: {np.min(val_loss_list)}')

        early_stopping(np.mean(val_loss_list), model=model)
        if early_stopping.early_stop:
            print("Early stopping")
            break

    # test the model on the test set and compute mse
    model = early_stopping.model
    model.eval()
    results_list = {'predictions': [], 'targets': [], 'random': []}

    #utilities for plotting results sorted by prefix length
    results_list_prefix = {'predictions': {}, 'targets': {}}


    for data in test_dataset:
        out = model(data).squeeze(0).detach().cpu()  # unidimensional output

        if train_args['problem_type'] == 'regression':
            #out = denormalize_predictions(out, target_mean, target_std)
            target = apply_normalization(torch.tensor(data.current_graph.y), target_mean, target_std)
            #out = torch.exp(model(data)[0]) - 1


        elif train_args['problem_type'] == 'classification':
            target = torch.tensor(one_hot_encoding_targets(model.unique_classes, data.current_graph.y))
            #if [1 if el == max(out) else 0 for el in out] == one_hot_encoding_targets(model.unique_classes, data.current_graph.y):
                #print(f"CORRECT: predicted index: {out.argmax()}")
            #else:
            #    print(f"WRONG: predicted index: {out.argmax()} when target was {one_hot_encoding_targets(model.unique_classes, data.current_graph.y).index(1)}")


        results_list['predictions'] = results_list['predictions'] + [out]
        results_list['targets'] =results_list['targets'] + [target]


        #append results divided according to prefix length
        pl = len(data.current_graph.x)
        results_list_prefix['predictions'][pl] = results_list_prefix['predictions'].get(pl, []) + [out]
        results_list_prefix['targets'][pl] = results_list_prefix['targets'].get(pl, []) + [target]



    if train_args['problem_type'] == 'regression':

        test_mse = calculate_mse(results_list['predictions'], results_list['targets'])
        print(f"Test MSE: {test_mse}")
        #plot results sorted by prefix length
        plot_metric_by_prefix_length(results_list_prefix, 'mse', 'MSE by prefix length', problem_specification)

        test_r2 = calculate_r2_score(torch.stack(results_list['predictions']).detach().cpu(), torch.stack(results_list['targets']).detach().cpu())
        print(f"Test R2 score: {test_r2}")

        #save results to a file
        regression_results = {
            "Test MSE": test_mse,
            "Test R2 score": test_r2
        }



        save_results_to_file(regression_results, train_args)

        #save model
        torch.save(model.state_dict(), f"model_{train_args['problem_name']}_{train_args['problem_type']}_{train_args['use_extra_graphs']}_{train_args['use_n_graphs']}.pt")



    elif train_args['problem_type'] == 'classification':
        test_acc = calculate_accuracy(torch.stack(results_list['predictions']), torch.stack(results_list['targets']).argmax(dim=1))
        print(f'Test accuracy: {test_acc}')
        test_precision = calculate_precision(torch.stack(results_list['predictions']), torch.stack(results_list['targets']).argmax(dim=1))
        print(f'Test precision: {test_precision}')
        test_recall = calculate_recall(torch.stack(results_list['predictions']), torch.stack(results_list['targets']).argmax(dim=1))
        print(f'Test recall: {test_recall}')
        test_f1_score = calculate_f1_score(torch.stack(results_list['predictions']), torch.stack(results_list['targets']).argmax(dim=1))
        print(f'Test f1 score: {test_f1_score}')
        test_auc = calculate_roc_auc(torch.stack(results_list['predictions']), torch.stack(results_list['targets']).argmax(dim=1))
        print(f'Test roc auc: {test_auc}')

        classification_results = {
            "Test accuracy": test_acc,
            "Test precision": test_precision,
            "Test recall": test_recall,
            "Test f1 score": test_f1_score,
            "Test roc auc": test_auc
        }
        save_results_to_file(classification_results, train_args)

        #plot results sorted by prefix length
        plot_metric_by_prefix_length(results_list_prefix, 'f1_score',  'F1 score by prefix length', problem_specification)
        plot_metric_by_prefix_length(results_list_prefix, 'roc_auc', 'ROC AUC score by prefix length', problem_specification)
        #save model
        torch.save(model.state_dict(), f"model_{train_args['problem_name']}_{train_args['problem_type']}_{train_args['use_extra_graphs']}_{train_args['use_n_graphs']}.pt")


