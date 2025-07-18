import pandas as pd
import json
import tempfile
import os
from geo_duplicate import get_same_card_nodes, geo_duplicate


def create_test_resource_model():
    """Create a test resource model JSON with geojson-feature-collection nodes."""
    return {
        "graph": [
            {
                "nodes": [
                    {
                        "nodeid": "node1",
                        "nodegroup_id": "card1",
                        "datatype": "geojson-feature-collection",
                        "name": "Site element geometry",
                    },
                    {
                        "nodeid": "node2",
                        "nodegroup_id": "card1",
                        "datatype": "geojson-feature-collection",
                        "name": "Legal boundary",
                    },
                    {
                        "nodeid": "node3",
                        "nodegroup_id": "card2",
                        "datatype": "string",
                        "name": "Site name",
                    },
                    {
                        "nodeid": "node4",
                        "nodegroup_id": "card3",
                        "datatype": "geojson-feature-collection",
                        "name": "Another geometry",
                    },
                ]
            }
        ]
    }


def create_test_csv_data():
    """Create test CSV data with geometry duplicates."""
    return pd.DataFrame(
        {
            "MAEASaM ID": ["SITE-001", "SITE-001", "SITE-001", "SITE-002", "SITE-003"],
            "Site element geometry": [
                "POINT (30.1 -20.1)",
                None,  # Empty geometry
                "POINT (30.2 -20.2)",  # Second geometry for same resource
                "POINT (30.3 -20.3)",
                "POINT (30.4 -20.4)",
            ],
            "Legal boundary": [
                "MULTIPOLYGON (((30.1 -20.1, 30.2 -20.1, 30.2 -20.2, 30.1 -20.2, 30.1 -20.1)))",
                None,
                "MULTIPOLYGON (((30.3 -20.3, 30.4 -20.3, 30.4 -20.4, 30.3 -20.4, 30.3 -20.3)))",
                None,
                None,
            ],
            "Site name": ["Test Site 1", None, None, "Test Site 2", "Test Site 3"],
            "Site description": [
                "Description 1",
                None,
                None,
                "Description 2",
                "Description 3",
            ],
            "Chronology": ["Stone Age", None, None, "Iron Age", "Modern"],
            "Another geometry": [None, None, None, "POINT (30.5 -20.5)", None],
        }
    )


def test_get_same_card_nodes():
    """Test the get_same_card_nodes function."""
    print("Testing get_same_card_nodes...")

    # Create test resource model
    resource_model = create_test_resource_model()

    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(resource_model, f)
        temp_file = f.name

    try:
        # Test the function
        result = get_same_card_nodes(temp_file)

        # Expected result: card1 should have 2 nodes, card3 should have 1 node
        expected = {"card1": ["node1", "node2"], "card3": ["node4"]}

        assert result == expected, f"Expected {expected}, got {result}"
        print("✅ get_same_card_nodes test passed!")

    finally:
        os.unlink(temp_file)


def test_geo_duplicate_basic():
    """Test basic geo_duplicate functionality."""
    print("Testing geo_duplicate basic functionality...")

    # Create test data
    test_df = create_test_csv_data()

    # Create temporary files
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as csv_f:
        test_df.to_csv(csv_f.name, index=False)
        input_csv = csv_f.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as json_f:
        resource_model = create_test_resource_model()
        json.dump(resource_model, json_f)
        resource_model_file = json_f.name

    output_csv = tempfile.mktemp(suffix=".csv")

    try:
        # Run geo_duplicate
        geo_duplicate(input_csv, output_csv, resource_model_file)

        # Read the result
        result_df = pd.read_csv(output_csv)

        # Check that SITE-001 rows have been processed correctly
        site_001_rows = result_df[result_df["MAEASaM ID"] == "SITE-001"]

        # Should have 3 rows for SITE-001
        assert len(site_001_rows) == 3, (
            f"Expected 3 rows for SITE-001, got {len(site_001_rows)}"
        )

        # Check that non-geometry values were copied from first row to other rows
        first_row = site_001_rows.iloc[0]
        for idx, row in site_001_rows.iterrows():
            if idx != site_001_rows.index[0]:  # Not the first row
                # Non-geometry columns should have values from first row
                assert row["Site name"] == "Test Site 1", (
                    f"Row {idx}: Expected 'Test Site 1', got '{row['Site name']}'"
                )
                assert row["Site description"] == "Description 1", (
                    f"Row {idx}: Expected 'Description 1', got '{row['Site description']}'"
                )
                assert row["Chronology"] == "Stone Age", (
                    f"Row {idx}: Expected 'Stone Age', got '{row['Chronology']}'"
                )

        print("✅ geo_duplicate basic test passed!")

    finally:
        # Clean up temporary files
        os.unlink(input_csv)
        os.unlink(resource_model_file)
        if os.path.exists(output_csv):
            os.unlink(output_csv)


def test_geo_duplicate_no_duplicates():
    """Test geo_duplicate when there are no geometry duplicates."""
    print("Testing geo_duplicate with no duplicates...")

    # Create test data with no geometry duplicates
    test_df = pd.DataFrame(
        {
            "MAEASaM ID": ["SITE-001", "SITE-002", "SITE-003"],
            "Site element geometry": [
                "POINT (30.1 -20.1)",
                "POINT (30.2 -20.2)",
                "POINT (30.3 -20.3)",
            ],
            "Site name": ["Site 1", "Site 2", "Site 3"],
            "Site description": ["Desc 1", "Desc 2", "Desc 3"],
        }
    )

    # Create temporary files
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as csv_f:
        test_df.to_csv(csv_f.name, index=False)
        input_csv = csv_f.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as json_f:
        resource_model = create_test_resource_model()
        json.dump(resource_model, json_f)
        resource_model_file = json_f.name

    output_csv = tempfile.mktemp(suffix=".csv")

    try:
        # Run geo_duplicate
        geo_duplicate(input_csv, output_csv, resource_model_file)

        # Read the result
        result_df = pd.read_csv(output_csv)

        # Should have same number of rows
        assert len(result_df) == len(test_df), (
            f"Expected {len(test_df)} rows, got {len(result_df)}"
        )

        # Data should be unchanged
        pd.testing.assert_frame_equal(result_df, test_df)

        print("✅ geo_duplicate no duplicates test passed!")

    finally:
        # Clean up temporary files
        os.unlink(input_csv)
        os.unlink(resource_model_file)
        if os.path.exists(output_csv):
            os.unlink(output_csv)


def test_geo_duplicate_missing_resource_id():
    """Test geo_duplicate when resource ID column is missing."""
    print("Testing geo_duplicate with missing resource ID...")

    # Create test data without MAEASaM ID column
    test_df = pd.DataFrame(
        {
            "Site name": ["Site 1", "Site 2"],
            "Site element geometry": ["POINT (30.1 -20.1)", "POINT (30.2 -20.2)"],
        }
    )

    # Create temporary files
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as csv_f:
        test_df.to_csv(csv_f.name, index=False)
        input_csv = csv_f.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as json_f:
        resource_model = create_test_resource_model()
        json.dump(resource_model, json_f)
        resource_model_file = json_f.name

    output_csv = tempfile.mktemp(suffix=".csv")

    try:
        # Run geo_duplicate
        geo_duplicate(input_csv, output_csv, resource_model_file)

        # Read the result
        result_df = pd.read_csv(output_csv)

        # Should have same data (no processing done)
        pd.testing.assert_frame_equal(result_df, test_df)

        print("✅ geo_duplicate missing resource ID test passed!")

    finally:
        # Clean up temporary files
        os.unlink(input_csv)
        os.unlink(resource_model_file)
        if os.path.exists(output_csv):
            os.unlink(output_csv)


def run_all_tests():
    """Run all tests."""
    print("Running geo_duplicate tests...\n")

    try:
        test_get_same_card_nodes()
        test_geo_duplicate_basic()
        test_geo_duplicate_no_duplicates()
        test_geo_duplicate_missing_resource_id()

        print("\n🎉 All tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    run_all_tests()
