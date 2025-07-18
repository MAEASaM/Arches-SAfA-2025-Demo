import json

# Load filters from the JSON file
with open("filters.json", "r") as f:
    FILTERS = json.load(f)


def data_filter(row: dict) -> dict:
    for field, mappings in FILTERS.items():
        if field in row and row[field] in mappings:
            row[field] = mappings[row[field]]
    return row
