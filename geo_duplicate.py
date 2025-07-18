import pandas as pd
import json


def get_same_card_nodes(resource_model_file):
    """
    Find nodes that have the same card ID and are of geojson-feature-collection datatype.

    Args:
        resource_model_file (str): Path to the resource model JSON file

    Returns:
        dict: Dictionary mapping card IDs to lists of node IDs with geojson-feature-collection datatype
    """
    with open(resource_model_file, "r") as f:
        resource_model = json.load(f)

    # Dictionary to store card_id -> list of node_ids with geojson-feature-collection
    card_nodes = {}

    # Find all nodes in the resource model
    for graph in resource_model.get("graph", []):
        for node in graph.get("nodes", []):
            if node.get("datatype") == "geojson-feature-collection":
                nodegroup_id = node.get("nodegroup_id")
                node_id = node.get("nodeid")

                if nodegroup_id not in card_nodes:
                    card_nodes[nodegroup_id] = []
                card_nodes[nodegroup_id].append(node_id)

    return card_nodes


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

    # Get nodes with geojson-feature-collection datatype grouped by card ID
    card_nodes = get_same_card_nodes(resource_model_file)

    # Find the column that contains the card ID (nodegroup_id)
    # Based on the CSV structure, we need to identify which column represents the card/group
    # For now, we'll assume there's a column that indicates the card/group relationship

    # Group by resource ID and card ID to find duplicates
    # We need to identify the columns that represent:
    # 1. Resource ID (likely 'MAEASaM ID' based on the CSV)
    # 2. Card/Group ID (we need to identify this from the CSV structure)
    # 3. Geometry columns (columns that contain geojson-feature-collection data)

    # For now, let's identify geometry columns by looking for columns that might contain geometry data
    geometry_columns = []
    for col in df.columns:
        if "geometry" in col.lower() or "geom" in col.lower():
            geometry_columns.append(col)

    print(f"Found geometry columns: {geometry_columns}")

    # Group by resource ID to find rows with the same resource
    resource_id_col = "MAEASaM ID"  # Assuming this is the resource ID column

    if resource_id_col not in df.columns:
        print(f"Warning: Resource ID column '{resource_id_col}' not found in CSV")
        print(f"Available columns: {list(df.columns)}")
        # Write the original data without processing
        df.to_csv(output_cleaned_file, index=False)
        return

    # Group by resource ID
    grouped = df.groupby(resource_id_col)

    processed_rows = []

    for resource_id, group in grouped:
        if len(group) == 1:
            # Single row for this resource, no processing needed
            processed_rows.append(group.iloc[0])
        else:
            # Multiple rows for this resource, need to check for geometry duplicates
            print(f"Processing resource {resource_id} with {len(group)} rows")

            # Check if any of the geometry columns have multiple non-empty values
            geometry_has_multiple = False
            for geom_col in geometry_columns:
                non_empty_geometries = group[geom_col].dropna()
                if len(non_empty_geometries) > 1:
                    geometry_has_multiple = True
                    print(f"  Found multiple geometries in column {geom_col}")
                    break

            if geometry_has_multiple:
                # Copy values from the first row to other rows where geometry columns are empty
                first_row = group.iloc[0]

                for idx, row in group.iterrows():
                    new_row = row.copy()

                    # For rows after the first one, copy non-geometry values from the first row
                    if idx != group.index[0]:
                        for col in df.columns:
                            if (
                                col not in geometry_columns
                                and pd.isna(new_row[col])
                                and not pd.isna(first_row[col])
                            ):
                                new_row[col] = first_row[col]

                    processed_rows.append(new_row)
            else:
                # No geometry duplicates, keep all rows as is
                for idx, row in group.iterrows():
                    processed_rows.append(row)

    # Create new DataFrame with processed rows
    processed_df = pd.DataFrame(processed_rows)

    # Write to output file
    processed_df.to_csv(output_cleaned_file, index=False)

    print(f"Processing complete. Output written to {output_cleaned_file}")
    print(f"Original rows: {len(df)}, Processed rows: {len(processed_df)}")
