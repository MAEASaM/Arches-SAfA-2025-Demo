import pandas as pd
import json

resource_id_col = "MAEASaM ID"


def get_same_card_nodes(node_name, resource_model_file):
    """
    Find nodes that have the same card ID and are of geojson-feature-collection datatype.

    Args:
        node_name (str): Name of the node to find the group for
        resource_model_file (str): Path to the resource model JSON file

    Returns:
        list: List of node names that belong to the same card/group
    """
    with open(resource_model_file, "r") as f:
        resource_model = json.load(f)

    # Dictionary to store card_id -> list of node_ids with geojson-feature-collection
    node_names = []

    # Find group node id for the given node
    nodegroup_id = None
    for graph in resource_model.get("graph", []):
        for node in graph.get("nodes", []):
            if node.get("name") == node_name:
                nodegroup_id = node.get("nodegroup_id")
                break
        if nodegroup_id:
            break

    if not nodegroup_id:
        return node_names

    # find node names from the same group
    for graph in resource_model.get("graph", []):
        for node in graph.get("nodes", []):
            if node.get("nodegroup_id") == nodegroup_id:
                node_names.append(node.get("name"))

    return node_names


def modify_card_data(node_names, df):
    """
    Modify the card data for the given node names.

    Args:
        node_names (list): List of node names in the same card/group
        df (pd.DataFrame): DataFrame containing the data

    Returns:
        pd.DataFrame: Modified DataFrame
    """
    if len(df) == 1:
        return df

    # Create a copy to avoid modifying the original
    modified_df = df.copy()

    # Group by resource ID to process each group separately
    for resource_id, group in modified_df.groupby("MAEASaM ID"):
        if len(group) > 1:
            # Get the first row as reference
            first_row = group.iloc[0]

            # Update other rows in the group
            for idx in group.index[1:]:  # Skip the first row
                for node_name in node_names:
                    if node_name in modified_df.columns:
                        if (
                            pd.isna(modified_df.at[idx, node_name])
                            or modified_df.at[idx, node_name] == ""
                        ):
                            modified_df.at[idx, node_name] = first_row[node_name]

    return modified_df


def get_geometry_node_names(resource_model_file):
    """
    Get all node names that have geojson-feature-collection datatype.

    Args:
        resource_model_file (str): Path to the resource model JSON file

    Returns:
        list: List of node names with geojson-feature-collection datatype
    """
    with open(resource_model_file, "r") as f:
        resource_model = json.load(f)

    geo_node_names = []
    for graph in resource_model.get("graph", []):
        for node in graph.get("nodes", []):
            if node.get("datatype") == "geojson-feature-collection":
                geo_node_names.append(node.get("name"))

    return geo_node_names


def process_geometry_duplicates(df, resource_model_file):
    """
    Process geometry duplicates in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to process
        resource_model_file (str): Path to the resource model JSON file

    Returns:
        pd.DataFrame: Processed DataFrame
    """
    geo_node_names = get_geometry_node_names(resource_model_file)

    # Create a copy to avoid modifying the original
    processed_df = df.copy()

    # Group by resource ID to check for duplicates
    for resource_id, group in processed_df.groupby("MAEASaM ID"):
        if len(group) > 1:
            # Check if any geometry columns have multiple non-empty values
            geometry_has_multiple = False
            for node_name in geo_node_names:
                if node_name in processed_df.columns:
                    non_empty_geometries = group[node_name].dropna()
                    if len(non_empty_geometries) > 1:
                        geometry_has_multiple = True
                        break

            if geometry_has_multiple:
                # Copy data from first row to other rows
                first_row = group.iloc[0]
                for idx in group.index[1:]:  # Skip the first row
                    for col in processed_df.columns:
                        if col not in geo_node_names:  # Only copy non-geometry columns
                            current_value = processed_df.at[idx, col]
                            first_value = first_row[col]
                            if pd.isna(current_value) or current_value == "":
                                processed_df.at[idx, col] = first_value

    return processed_df


def validate_input_data(df):
    """
    Validate that the input DataFrame has the required columns.

    Args:
        df (pd.DataFrame): DataFrame to validate

    Returns:
        bool: True if valid, False otherwise
    """
    if resource_id_col not in df.columns:
        print(f"Warning: Resource ID column '{resource_id_col}' not found in CSV")
        print(f"Available columns: {list(df.columns)}")
        return False
    return True


def geo_duplicate(data_csv_file, output_cleaned_file, resource_model_file):
    """
    Process CSV data to handle geojson-feature-collection duplicates.

    Args:
        data_csv_file (str): Path to the input CSV file
        output_cleaned_file (str): Path to the output cleaned CSV file
        resource_model_file (str): Path to the resource model JSON file
    """
    # Read the CSV file
    df = pd.read_csv(data_csv_file)

    # Validate input data
    if not validate_input_data(df):
        # Write the original data without processing
        df.to_csv(output_cleaned_file, index=False)
        return

    # Process geometry duplicates
    processed_df = process_geometry_duplicates(df, resource_model_file)

    # Write to output file
    processed_df.to_csv(output_cleaned_file, index=False)

    print(f"Processing complete. Output written to {output_cleaned_file}")
    print(f"Original rows: {len(df)}, Processed rows: {len(processed_df)}")
