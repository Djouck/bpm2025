import numpy as np
import torch
from sklearn.metrics import roc_auc_score, mean_squared_error, r2_score, f1_score


def calculate_accuracy(predictions, target):
    _, predicted_labels = torch.max(predictions, 1)
    correct = (predicted_labels == target).sum().item()
    accuracy = correct / target.size(0)
    return accuracy



def calculate_precision(predictions, target):
    _, predicted_labels = torch.max(predictions, 1)
    precision_per_class = []
    for class_label in torch.unique(target):
        true_positives = ((predicted_labels == class_label) & (target == class_label)).sum().item()
        false_positives = ((predicted_labels == class_label) & (target != class_label)).sum().item()
        if true_positives + false_positives == 0:
            precision_per_class.append(0)
        else:
            precision_per_class.append(true_positives / (true_positives + false_positives))
    return sum(precision_per_class) / len(precision_per_class)

def calculate_recall(predictions, target):
    _, predicted_labels = torch.max(predictions, 1)
    recall_per_class = []
    for class_label in torch.unique(target):
        true_positives = ((predicted_labels == class_label) & (target == class_label)).sum().item()
        false_negatives = ((predicted_labels != class_label) & (target == class_label)).sum().item()
        if true_positives + false_negatives == 0:
            recall_per_class.append(0)
        else:
            recall_per_class.append(true_positives / (true_positives + false_negatives))
    return sum(recall_per_class) / len(recall_per_class)

def calculate_f1_score(predictions, target):
    pred_label = torch.argmax(predictions, dim=1)
    return f1_score(target, pred_label, average="weighted")

#calculate the roc area under the curve
def calculate_roc_auc(predictions, target):
    #one_hot_predictions = torch.nn.functional.one_hot(torch.argmax(predictions, dim=1))
    #_, target_np = torch.max(target, 1)
    if target.dim() == 1 or target.size(1) == 1:
        target = torch.nn.functional.one_hot(target, num_classes=predictions.size(1))


    target_np = np.array(target)
    predictions_np = np.array(predictions)

    #if len(np.unique(target_np.argmax(axis=1))) == 1:
    #    raise ValueError("Only one class present in y_true. ROC AUC score is not defined in that case.")


    return roc_auc_score(target_np, predictions_np, multi_class='ovr', average="micro")


def calculate_mse(predictions, targets):
    return mean_squared_error(targets, predictions)

def calculate_r2_score(predictions, targets):
    return r2_score(targets, predictions)