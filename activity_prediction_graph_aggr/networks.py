import torch
import torch.nn.functional as F
import torch_geometric
from torch.nn import Linear, Conv1d, Sigmoid
from torch_geometric.data import Batch
from torch_geometric.nn import SAGEConv, SortAggregation
from utils import set_seed

set_seed(42)

class GraphRegression(torch.nn.Module):
    def __init__(self, use_extra_graphs=True, use_n_graphs=False):
        super(GraphRegression, self).__init__()
        hidden_channels = 16
        self.current_graph_encoder = GraphEncoder(-1, hidden_channels)
        self.extra_graph_encoder = GraphSetEncoder(-1, hidden_channels)

        self.use_extra_graphs = use_extra_graphs  # flag to choose whether to use the subgraphs
        self.use_n_graphs = use_n_graphs  # flag to choose whether to use the number of graphs as input

        self.hidden_size = 64


        # Define the output layers
        self.conv1d = Conv1d(hidden_channels, 32, 3)  # Adjust kernel size as needed
        if not use_n_graphs:
            self.lin1 = Linear(160, self.hidden_size)
        else:
            self.lin1 = Linear(160 + 1, self.hidden_size)

        self.lin2 = Linear(self.hidden_size, 1)  # Single output for regression

        # Output layers for extra graphs (regression)
        self.conv1d_extra = Conv1d(hidden_channels, 32, 3)  # Adjust kernel size as needed
        self.lin1_extra = Linear(32 * (10 - 2 + 1), self.hidden_size)
        self.lin2_extra = Linear(self.hidden_size, 1)  # Single output for regression

        if not use_n_graphs:
            self.lin_final1 = Linear(320, self.hidden_size)
        else:
            self.lin_final1 = Linear(320 + 1, self.hidden_size)
        self.lin_final2 = Linear(self.hidden_size, 1)  # Single output for regression

    def forward(self, x):
        if type(x) == dict:
            current_graph = x['current_graph']
            extra_graphs = x['additional_graphs']
            x_current = self.current_graph_encoder(Batch.from_data_list(current_graph))
        else:  # it means it is a DataTuple
            current_graph = x.current_graph
            extra_graphs = x.additional_graphs
            x_current = self.current_graph_encoder(Batch.from_data_list([current_graph]))

        if self.use_extra_graphs:
            if len(extra_graphs):
                if type(extra_graphs[0]) == list:
                    x_extra = self.extra_graph_encoder(extra_graphs)
                else:
                    x_extra = self.extra_graph_encoder([extra_graphs])
            else:
                x_extra = torch.zeros_like(x_current)

            # x_current
            x_current = x_current.view(len(x_current), 7, -1).permute(0, 2, 1)
            x_current = F.relu(self.conv1d(x_current))
            x_current = x_current.view(x_current.size(0), -1)  # Flatten

            # x_extra
            x_extra = x_extra.view(len(x_extra), 7, -1).permute(0, 2, 1)
            x_extra = F.relu(self.conv1d_extra(x_extra))
            x_extra = x_extra.view(x_extra.size(0), -1)  # Flatten

            x = torch.cat([x_current, x_extra], dim=1)
            x = F.relu(self.lin_final1(x))
            #x = F.dropout(x, p=0.1, training=self.training)  # Increased dropout
            x = self.lin_final2(x)

            return x
        else:
            x = x_current
            x = x.view(len(x), 7, -1).permute(0, 2, 1)
            x = F.relu(self.conv1d(x))
            x = x.view(len(x), -1)

            if self.use_n_graphs:
                #check if x is unidimensional
                if x.shape[0] != 1:
                    n_graphs = torch.tensor([[len(g)] for g in extra_graphs], dtype=torch.float32).to(x.device)
                else:
                    n_graphs = torch.tensor([[len(extra_graphs)]], dtype=torch.float32).to(x.device)

                x = torch.cat([x, n_graphs], dim=1)

            x = F.relu(self.lin1(x))
            #x = F.dropout(x, p=0.1, training=self.training)
            x = self.lin2(x)
            return x

class GraphEncoder(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, dropout=0.2):
        super(GraphEncoder, self).__init__()
        self.k = 7 #sort pool parameter
        self.num_layers = 3
        self.conv1 = SAGEConv(in_channels, hidden_channels)

        self.convs = torch.nn.ModuleList()

        for i in range(self.num_layers - 1):
            self.convs.append(SAGEConv(hidden_channels, hidden_channels))

        kernel_size = self.num_layers
        self.dropout = dropout

    def forward(self, batch):
        x, edge_index, batch = batch.x, batch.edge_index, batch.batch

        x = self.conv1(x, edge_index).relu()


        for conv in self.convs:
            x = conv(x, edge_index).relu()

        sort_aggr = SortAggregation(self.k)
        x = sort_aggr(x, batch)

        #x = self.out(x)  # Optional: Transform to a single value per graph
        return x

class GraphSetEncoder(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels):
        super(GraphSetEncoder, self).__init__()
        self.graph_encoder = GraphEncoder(in_channels, hidden_channels)


    def forward(self, data_list):
        # Process each graph through the GraphEncoder
        all_graphs_representations = [self.graph_encoder(Batch.from_data_list(data)) if data else torch.zeros((1, 112)) for data in data_list]
        mean_repr = [torch.mean(graph_representation, dim=0) for graph_representation in all_graphs_representations]

        return torch.stack(mean_repr)



class GraphClassification(torch.nn.Module):
    def __init__(self, original_classes, use_extra_graphs=True, use_n_graphs=False):
        super(GraphClassification, self).__init__()
        hidden_channels = 16
        self.current_graph_encoder = GraphEncoder(-1, hidden_channels)
        self.extra_graph_encoder = GraphSetEncoder(-1, hidden_channels)

        self.use_extra_graphs = use_extra_graphs  # flag to choose whether to use the subgraphs
        self.use_n_graphs = use_n_graphs  # flag to choose whether to use the number of graphs as input

        self.unique_classes = original_classes
        self.hidden_size = 64

        # Define the output layers
        self.conv1d = Conv1d(hidden_channels, 32, 3)  # Adjust kernel size as needed
        #self.bn1d = BatchNorm1d(32)

        if not use_n_graphs:
            self.lin1 = Linear(160, self.hidden_size)
        else:
            self.lin1 = Linear(160+1, self.hidden_size)
        self.lin2 = Linear(self.hidden_size, len(self.unique_classes))


        if not use_n_graphs:
            self.lin_final1 = Linear(320, self.hidden_size)
        else:
            self.lin_final1 = Linear(320 + 1, self.hidden_size)
        self.lin_final2 = Linear(self.hidden_size, 1)  # Single output for regression

        # Output layers for extra graphs (classification)
        self.conv1d_extra = Conv1d(hidden_channels, 32, 3)  # Adjust kernel size as needed
        #self.bn1d_extra = BatchNorm1d(32)
        self.lin1_extra = Linear(32 * (10 - 2 + 1), self.hidden_size)
        self.lin2_extra = Linear(self.hidden_size, len(self.unique_classes))
        self.lin_final2 = Linear(self.hidden_size, len(self.unique_classes))

    def forward(self, x):
        if type(x) == dict:
            current_graph = x['current_graph']
            extra_graphs = x['additional_graphs']
            x_current = self.current_graph_encoder(Batch.from_data_list(current_graph))
        else:  # it means it is a DataTuple
            current_graph = x.current_graph
            extra_graphs = x.additional_graphs
            x_current = self.current_graph_encoder(Batch.from_data_list([current_graph]))

        if self.use_extra_graphs:
            if len(extra_graphs):
                if type(extra_graphs[0]) == list:
                    x_extra = self.extra_graph_encoder(extra_graphs)
                else:
                    x_extra = self.extra_graph_encoder([extra_graphs])
            else:
                x_extra = torch.zeros_like(x_current)

            # x_current
            x_current = x_current.view(len(x_current), 7, -1).permute(0, 2, 1)
            x_current = F.relu(self.conv1d(x_current))
            #x_current = self.bn1d(x_current)
            x_current = x_current.view(x_current.size(0), -1)  # Flatten

            # x_extra
            #x_extra = x_extra.unsqueeze(1)
            x_extra = x_extra.view(len(x_extra), 7, -1).permute(0, 2, 1)
            x_extra = F.relu(self.conv1d_extra(x_extra))
            #x_extra = self.bn1d_extra(x_extra)
            x_extra = x_extra.view(x_extra.size(0), -1)  # Flatten


            x = torch.cat([x_current, x_extra], dim=1)

            x = F.relu(self.lin_final1(x))
            #x = F.dropout(x, p=0.1, training=self.training)  # Increased dropout
            x = self.lin_final2(x)
            return F.log_softmax(x, dim=-1)
            #return F.softmax(x, dim=-1)
        else:
            x = x_current
            x = x.view(len(x), 7, -1).permute(0, 2, 1)
            x = F.relu(self.conv1d(x))
            #x = self.bn1d(x)
            x = x.view(len(x), -1)

            if self.use_n_graphs:
                # check if x is unidimensional
                if x.shape[0] != 1:
                    n_graphs = torch.tensor([[len(g)] for g in extra_graphs], dtype=torch.float32).to(x.device)
                else:
                    n_graphs = torch.tensor([[len(extra_graphs)]], dtype=torch.float32).to(x.device)

                x = torch.cat([x, n_graphs], dim=1)

            x = F.relu(self.lin1(x))
            #x = F.dropout(x, p=0.1, training=self.training)
            x = self.lin2(x)
            return F.log_softmax(x, dim=-1)
            #return F.softmax(x, dim=-1)


class GraphClassificationGated(torch.nn.Module):
    def __init__(self, original_classes, use_extra_graphs=True):
        super(GraphClassification, self).__init__()
        hidden_channels = 16
        self.current_graph_encoder = GraphEncoder(-1, hidden_channels)
        self.extra_graph_encoder = GraphSetEncoder(-1, hidden_channels)

        self.use_extra_graphs = use_extra_graphs  # flag to choose whether to use the subgraphs

        self.unique_classes = original_classes
        self.hidden_size = 64

        # Define the output layers
        self.conv1d = Conv1d(hidden_channels, 32, 3)  # Adjust kernel size as needed
        self.lin1 = Linear(160, self.hidden_size)
        self.lin2 = Linear(self.hidden_size, len(self.unique_classes))

        # Output layers for extra graphs (classification)
        self.conv1d_extra = Conv1d(hidden_channels, 32, 3)  # Adjust kernel size as needed
        self.lin1_extra = Linear(32 * (10 - 2 + 1), self.hidden_size)
        self.lin2_extra = Linear(self.hidden_size, len(self.unique_classes))

        self.lin_final1 = Linear(320, self.hidden_size)
        self.lin_final2 = Linear(self.hidden_size, len(self.unique_classes))

        # Gating mechanism
        self.gate = Sigmoid()
        self.gate_linear = Linear(320, 1)

    def forward(self, x):
        if type(x) == dict:
            current_graph = x['current_graph']
            extra_graphs = x['additional_graphs']
            x_current = self.current_graph_encoder(Batch.from_data_list(current_graph))
        else:  # it means it is a DataTuple
            current_graph = x.current_graph
            extra_graphs = x.additional_graphs
            x_current = self.current_graph_encoder(Batch.from_data_list([current_graph]))

        if self.use_extra_graphs:
            if len(extra_graphs):
                if type(extra_graphs[0]) == list:
                    x_extra = self.extra_graph_encoder(extra_graphs)
                else:
                    x_extra = self.extra_graph_encoder([extra_graphs])
            else:
                x_extra = torch.zeros_like(x_current)

            # x_current
            x_current = x_current.view(len(x_current), 7, -1).permute(0, 2, 1)
            x_current = F.relu(self.conv1d(x_current))
            x_current = self.dropout1d(x_current)
            x_current = x_current.view(x_current.size(0), -1)  # Flatten

            # x_extra
            x_extra = x_extra.view(len(x_extra), 7, -1).permute(0, 2, 1)
            x_extra = F.relu(self.conv1d_extra(x_extra))
            x_extra = self.dropout1d_extra(x_extra)
            x_extra = x_extra.view(x_extra.size(0), -1)  # Flatten

            x = torch.cat([x_current, x_extra], dim=1)
            gate_value = self.gate(self.gate_linear(x))
            x = gate_value * x_current + (1 - gate_value) * x_extra
            x = F.relu(self.lin_final1(x))
            x = self.dropout_final1(x)
            x = self.lin_final2(x)
            return F.log_softmax(x, dim=-1)
        else:
            x = x_current
            x = x.view(len(x), 7, -1).permute(0, 2, 1)
            x = F.relu(self.conv1d(x))
            x = self.dropout1d(x)
            x = x.view(len(x), -1)
            x = F.relu(self.lin1(x))
            x = self.dropout_lin1(x)
            x = self.lin2(x)
            return F.log_softmax(x, dim=-1)


class GraphClassificationM(torch.nn.Module):
    def __init__(self, original_classes, use_extra_graphs=True, use_n_graphs=False):
        super(GraphClassificationM, self).__init__()
        hidden_channels = 16
        self.current_graph_encoder = GraphEncoder(-1, hidden_channels)
        self.extra_graph_encoder = GraphSetEncoder(-1, hidden_channels)

        self.use_extra_graphs = use_extra_graphs  # flag to choose whether to use the subgraphs
        self.use_n_graphs = use_n_graphs  # flag to choose whether to use the number of graphs as input

        self.unique_classes = original_classes
        self.hidden_size = 64

        # Define the output layers
        self.conv1d = Conv1d(hidden_channels, 32, 3)  # Adjust kernel size as needed
        #self.bn1d = BatchNorm1d(32)

        if not use_n_graphs:
            self.lin1 = Linear(160, self.hidden_size)
        else:
            self.lin1 = Linear(160+1, self.hidden_size)
        self.lin2 = Linear(self.hidden_size, len(self.unique_classes))


        if not use_n_graphs:
            self.lin_final1 = Linear(320, self.hidden_size)
        else:
            self.lin_final1 = Linear(320 + 1, self.hidden_size)
        self.lin_final2 = Linear(self.hidden_size, 1)  # Single output for regression

        # Output layers for extra graphs (classification)
        self.conv1d_extra = Conv1d(hidden_channels, 32, 3)  # Adjust kernel size as needed
        #self.bn1d_extra = BatchNorm1d(32)
        self.lin1_extra = Linear(32 * (10 - 2 + 1), self.hidden_size)
        self.lin2_extra = Linear(self.hidden_size, len(self.unique_classes))
        self.lin_final2 = Linear(self.hidden_size, len(self.unique_classes))

    def forward(self, x):
        if type(x) == dict:
            current_graph = x['current_graph']
            extra_graphs = x['additional_graphs']
            x_current = self.current_graph_encoder(Batch.from_data_list(current_graph))
        else:  # it means it is a DataTuple
            current_graph = x.current_graph
            extra_graphs = x.additional_graphs
            x_current = self.current_graph_encoder(Batch.from_data_list([current_graph]))

        if self.use_extra_graphs:
            if len(extra_graphs):
                if type(extra_graphs[0]) == list:
                    x_extra = self.extra_graph_encoder(extra_graphs)
                else:
                    x_extra = self.extra_graph_encoder([extra_graphs])
            else:
                x_extra = torch.zeros_like(x_current)

            # x_current
            x_current = x_current.view(len(x_current), 7, -1).permute(0, 2, 1)
            x_current = F.relu(self.conv1d(x_current))
            #x_current = self.bn1d(x_current)
            x_current = x_current.view(x_current.size(0), -1)  # Flatten

            # x_extra
            #x_extra = x_extra.unsqueeze(1)
            x_extra = x_extra.view(len(x_extra), 7, -1).permute(0, 2, 1)
            x_extra = F.relu(self.conv1d_extra(x_extra))
            #x_extra = self.bn1d_extra(x_extra)
            x_extra = x_extra.view(x_extra.size(0), -1)  # Flatten


            x = torch.cat([x_current, x_extra], dim=1)

            #x = torch.rand_like(x)
            x = F.relu(self.lin_final1(x))
            #x = F.dropout(x, p=0.1, training=self.training)  # Increased dropout
            x = self.lin_final2(x)
            return F.log_softmax(x, dim=-1)
            #return F.softmax(x, dim=-1)
        else:
            x = x_current
            x = x.view(len(x), 7, -1).permute(0, 2, 1)
            x = F.relu(self.conv1d(x))
            #x = self.bn1d(x)
            x = x.view(len(x), -1)

            if self.use_n_graphs:
                # check if x is unidimensional
                if x.shape[0] != 1:
                    n_graphs = torch.tensor([[len(g)] for g in extra_graphs], dtype=torch.float32).to(x.device)
                else:
                    n_graphs = torch.tensor([[len(extra_graphs)]], dtype=torch.float32).to(x.device)

                x = torch.cat([x, n_graphs], dim=1)

            #torch.manual_seed(42)
            #x = torch.rand_like(x)
            x = F.relu(self.lin1(x))
            x = self.lin2(x)
            return F.log_softmax(x, dim=-1)
