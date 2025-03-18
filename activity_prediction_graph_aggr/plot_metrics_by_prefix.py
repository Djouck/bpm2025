# this script creates the plots for the metrics by prefix
import os
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import matplotlib


def plot_metrics_by_prefix_from_files(input_dir):
    # get all files in the input directory
    files = os.listdir(input_dir)
    # the files are saved as .npy
    files = [file for file in files if file.endswith(".npy")]
    # initialize the dataframes
    dfs = []
    # iterate over all files
    for file in files:
        # load the data
        data = np.load(os.path.join(input_dir, file), allow_pickle=True)
        model = ''

        print(file)

        if 'no_extra_graphs' in file:
            if "False" in file.split('.')[0].split('_'):
                model = 'GNN'
            elif "True" in file.split('.')[0].split('_'):
                model = 'GNNc'
        elif 'n_graphs' in file:
            model = 'GNNc'  # UNUSED
        else:
            model = 'GNNs'

        data = [{"prefix": prefix + 3, "metric": file.split('.')[0].split('_')[-1], "value": value, "model": model} for
                prefix, value in enumerate(data)]

        # create a dataframe
        df = pd.DataFrame(data)
        # add the file name as a column
        # add the file name as a column
        if file.split('.')[0].split('_')[0] == 'prepaid':
            df["instance"] = 'Prepaid'
        elif file.split('.')[0].split('_')[0] == 'RFP':
            df["instance"] = 'RfP'
        elif file.split('.')[0].split('_')[0] == 'TP':
            df["instance"] = 'TP'
        elif file.split('.')[0].split('_')[0] == 'BPIC11':
            df["instance"] = 'BPIC11'
        elif file.split('.')[0].split('_')[0] == 'BPI15':
            df["instance"] = 'BPI15'
        elif file.split('.')[0].split('_')[0] == 'InternationalDeclarations':
            df["instance"] = 'International'
        elif file.split('.')[0].split('_')[0] == 'HelpDesk':
            df["instance"] = 'HelpDesk'
        elif file.split('.')[0].split('_')[0] == 'BPI12':
            df["instance"] = 'BPI12_SE'
        elif file.split('.')[0].split('_')[0] == 'BPIW12':
            df["instance"] = 'BPIW12_SE'
        else:
            df['instance'] = 'Unkown'
            continue
        # append the dataframe to the list
        dfs.append(df)

    # concatenate all dataframes
    df = pd.concat(dfs)

    # Iterate over unique instances
    for instance in df['instance'].unique():
        # Filter the dataframe for the current instance
        df_instance = df[df['instance'] == instance]
        print(instance)
        print(df_instance.head())

        for metric_name in df_instance["metric"].unique():
            print(metric_name)

            df_metric = df_instance[df_instance['metric'] == metric_name]

            # elaborate data to contain 'prefix', 'metric' and 'value'
            if metric_name == 'mse':
                metric_name = 'MSE'
            elif metric_name == 'score':
                metric_name = 'F1 Score'
            else:
                print('wrong name')
                continue

            # initialize the plot
            sns.set(style="whitegrid")
            # Initialize the plot
            plt.figure(figsize=(10, 6))

            # Create the seaborn bar plot
            sns.barplot(x='prefix', y='value', hue='model', data=df_metric)
            # Set the title and labels
            plt.gca().xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(nbins=40,integer=True))
            plt.title(f"{instance}")
            plt.xlabel('Prefix Length')
            plt.ylabel(f'{metric_name}')
            # Save the plot to a file
            plt.savefig(os.path.join(input_dir, f"{metric_name}_by_prefix_{instance}.png"))
            # Close the plot
            plt.close()


if __name__ == "__main__":
    plot_metrics_by_prefix_from_files("./data/plots_giulia")
    # plot_metrics_by_prefix_from_files("./data")
