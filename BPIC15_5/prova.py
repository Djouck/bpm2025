import pandas as pd
import re

# Function to compute Jaccard similarity between two sets
def jaccard_similarity(set1, set2):
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0

# Load the CSV dataset
csv_file_path = "BPIC15_5_Final_Data_File_Reg.csv"
df = pd.read_csv(csv_file_path, delimiter=';')

# Load the .g file (Instance Graphs)
g_file_path = "BPIC15_5.g"
with open(g_file_path, 'r', encoding='utf-8') as file:
    g_data = file.read()

# Ensure correct column names
df.rename(columns={'Case ID': 'case:concept:name'}, inplace=True)
df['case:concept:name'] = df['case:concept:name'].astype(str)  # Ensure IDs are strings

# Extract unique case IDs
unique_case_ids = df['case:concept:name'].unique()

# Extract instance graphs (split by "XP\n" separator)
instance_graphs = g_data.strip().split("\nXP\n")

# Store activities for each case ID from CSV
csv_case_activities = {}

for case_id in unique_case_ids:
    case_activities = df[df['case:concept:name'] == case_id]['Activity'].str.strip().tolist()
    case_activities_normalized = {activity.replace(" ", "").lower() for activity in case_activities}
    csv_case_activities[case_id] = case_activities_normalized

# Store the best matches for instance graphs
graph_case_matches = []
valid_graphs = []
valid_case_ids = set()
graph_index_to_case_id = {}
low_similarity_reports = []

# Compare each instance graph with all case IDs in the CSV
for i, instance_graph in enumerate(instance_graphs):
    # Extract activities from the instance graph
    graph_activities = re.findall(r"v \d+ (.+)", instance_graph)
    graph_activities_normalized = {activity.lower() for activity in graph_activities}

    best_match_case_id = None
    best_jaccard_score = 0

    for case_id, csv_activities in csv_case_activities.items():
        similarity_score = jaccard_similarity(graph_activities_normalized, csv_activities)

        if similarity_score > best_jaccard_score:
            best_jaccard_score = similarity_score
            best_match_case_id = case_id

    # Store the mapping
    graph_index_to_case_id[i] = best_match_case_id

    # Only keep cases and graphs where Jaccard similarity is exactly 1
    if best_jaccard_score == 1.0:
        valid_graphs.append(instance_graph)
        valid_case_ids.add(best_match_case_id)
    else:
        # Store cases where Jaccard similarity is less than 1
        low_similarity_reports.append({
            "Instance Graph Index": i,
            "Best Matching Case ID": best_match_case_id,
            "Jaccard Similarity Score": best_jaccard_score
        })

# Filter the CSV to keep only valid case IDs
df_filtered = df[df['case:concept:name'].isin(valid_case_ids)]

# Save the new filtered CSV
filtered_csv_path = "BPIC15_5_Filtered.csv"
df_filtered.to_csv(filtered_csv_path, index=False, sep=';')

# Modify the .g file format: Replace 'XP' with 'case id {case_id}'
filtered_instance_graphs = []

for i, instance_graph in enumerate(valid_graphs):
    case_id = graph_index_to_case_id[i]  # Get the corresponding case ID
    formatted_graph = f"case id {case_id}\n{instance_graph}"  # Replace 'XP' with 'case id {case_id}'
    filtered_instance_graphs.append(formatted_graph)

# Save the modified .g file
filtered_g_path = "BPIC15_5_Filtered.g"
with open(filtered_g_path, 'w', encoding='utf-8') as file:
    file.write("\n\n".join(filtered_instance_graphs))  # Separate instances with double newlines

# Convert low similarity reports to a DataFrame for display
low_similarity_results_df = pd.DataFrame(low_similarity_reports)

# Return updated file paths and dictionary
#filtered_csv_path, filtered_g_path, graph_index_to_case_id

