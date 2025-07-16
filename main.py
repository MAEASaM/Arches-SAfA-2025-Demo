# This script is used to clean the CSV files for the Arches upload
# The script is written by Mahmoud Abdelrazek and Renier van der Merwe

# Data sheets

input_csv_file = "" #This will change depending on the csv file you want to clean
output_csv_file = "arches_modified.csv"
actor_csv_file = "Actor.csv"
admin_csv_file = "Administrative Model.csv"
chrono_csv_file = "Chronology.csv"
info_csv_file = "Information.csv"
concept_json_file = "Site_concepts.json"


import pandas as pd
import csv
from datetime import datetime
import pathlib
from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon, Point
import json


# find the path of the script
script_path = pathlib.Path(__file__).parent.absolute()

# add script path to the csv files
input_csv_file = script_path / input_csv_file
output_csv_file = script_path / output_csv_file
actor_csv_file = script_path / actor_csv_file
admin_csv_file = script_path / admin_csv_file
chrono_csv_file = script_path / chrono_csv_file
info_csv_file = script_path / info_csv_file
concept_json_file = script_path / concept_json_file


class CSV_cleaning_script:
    def __init__(self):
        pass

    def read_input_csv(self) -> pd.DataFrame:
        with open(input_csv_file, 'r') as input_csv_file_object:
            input_csv_file_object_reader = csv.DictReader(input_csv_file_object)
            write_output_csv(input_csv_file_object_reader)
            # return input_csv_file_object_reader


def read_input_csv() -> csv.DictReader:
    with open(input_csv_file, "r") as input_csv_file_object:
        input_csv_file_object_reader = csv.DictReader(input_csv_file_object)
        write_output_csv(input_csv_file_object_reader)
        # return input_csv_file_object_reader


def read_actor_uuid_csv() -> dict:
    actor_uuid_dict = {}
    with open(actor_csv_file, "r") as actor_uuid_file_object:
        actor_uuid_file_object_reader = csv.DictReader(actor_uuid_file_object)
        for uuid_row in actor_uuid_file_object_reader:
            actor_uuid_dict[uuid_row["Name value"]] = uuid_row["resourceid"]

    return actor_uuid_dict


def check_for_resource_id_column(file_reader: csv.DictReader) -> bool:
    return "resourceid" in file_reader.fieldnames


def write_output_csv(file_reader: csv.DictReader) -> None:
    with open(output_csv_file, "w") as zim_data_overwritten:
        # read actor uuid csv
        actor_uuid_dict = read_actor_uuid_csv()

        fieldnames = file_reader.fieldnames
        missing_resource_id = check_for_resource_id_column(file_reader)

        if not missing_resource_id:
            fieldnames = ["ResourceID"] + fieldnames

        writer = csv.DictWriter(zim_data_overwritten, fieldnames=fieldnames)

        writer.writeheader()
        for row in file_reader:
            if not missing_resource_id:
                row["ResourceID"] = row["MAEASaM ID"]
                row["Geometry type"] = row["Geometry type"]  # This is just a fix for my csv. We will have to change it to do this only if a WKT coloum is pressent
            row = data_filter(row)
            row = date_format_all_coloums(row)
            row = actor_uuid_format(row, actor_uuid_dict)
            row = clean_geomtry_based_on_type(row)
            writer.writerow(row)


def data_filter(row: dict) -> dict:
    # Implement your filtering logic here
    return row


def convert_date_format(date_str: str) -> str:
    try:
        # Parse the input date in the current format
        date_obj = datetime.strptime(date_str, "%Y/%m/%d")
        # Convert it to the desired format "%Y-%m-%d"
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        # Handle invalid date format gracefully
        return date_str  # Return the original date if it can't be parsed


def date_format_all_coloums(row: dict) -> dict:
    row["Survey date"] = convert_date_format(row["Survey date"])
    row["Date of imagery"] = convert_date_format(row["Date of imagery"])
    row["Date of imagery"] = row["Date of imagery"].replace("20XX", row["Survey date"])
    row["Date of imagery"] = row["Date of imagery"].replace("1900-01-00", row["Survey date"])
    row["Threat assessment date"] = convert_date_format(row["Threat assessment date"])
    row["Image used date"] = convert_date_format(row["Image used date"])
    return row


def actor_uuid_format(row: dict, actor_uuid_dict) -> dict:
    if row["Surveyor name"]:
        row["Surveyor name"] = (
            "[{'resourceId': '"
            + actor_uuid_dict[row["Surveyor name"]]
            + "','ontologyProperty': 'http://www.cidoc-crm.org/cidoc-crm/P11_had_participant', 'resourceXresourceId': '','inverseOntologyProperty': 'http://www.cidoc-crm.org/cidoc-crm/P140_assigned_attribute_to'}]"
        )
    if row["Threat assessor name"]:
        row["Threat assessor name"] = (
            "[{'resourceId': '"
            + actor_uuid_dict[row["Threat assessor name"]]
            + "','ontologyProperty': 'http://www.cidoc-crm.org/cidoc-crm/P11_had_participant', 'resourceXresourceId': '','inverseOntologyProperty': 'http://www.cidoc-crm.org/cidoc-crm/P140_assigned_attribute_to'}]"
        )
    if row["Assessor name"]:
        row["Assessor name"] = (
            "[{'resourceId': '"
            + actor_uuid_dict[row["Assessor name"]]
            + "','ontologyProperty': 'http://www.cidoc-crm.org/cidoc-crm/P11_had_participant', 'resourceXresourceId': '','inverseOntologyProperty': 'http://www.cidoc-crm.org/cidoc-crm/P140_assigned_attribute_to'}]"
        )
    return row


def clean_geomtry_based_on_type(row: dict) -> dict:
    geometry = row["Geometry type"]
    if geometry:
        geometry_type = geometry.split(" ")[0]
        if geometry_type == "POINT":
            return row
        if (
            geometry_type == "POLYGON"
        ):  # Add this to avoid the error but not sure if solved it
            return row
        if (
            geometry_type == "LINESTRING"
        ):  # Add this to avoid the error but not sure if solved it
            return row
        elif geometry_type == "MULTIPOLYGON":
            row["Geometry type"] = remove_duplicate_points(geometry)
            return row
        else:
            print("Unknown geometry type: " + geometry_type)
    else:
        return row


def remove_duplicate_points(geometry_wkt: str) -> str:
    if not geometry_wkt:
        return ""

    if type(geometry_wkt) == str:
        geometry = wkt.loads(geometry_wkt)
    else:
        geometry = geometry_wkt

    seen_points = set()

    if isinstance(geometry, MultiPolygon):
        new_polygons = []
        for polygon in geometry.geoms:
            new_polygon = remove_duplicate_points(polygon)
            new_polygons.append(new_polygon)
        return MultiPolygon(new_polygons)
    elif isinstance(geometry, Polygon):
        new_exterior = []
        for point in geometry.exterior.coords:
            if point not in seen_points:
                seen_points.add(point)
                new_exterior.append(point)
        new_interiors = []
        for interior in geometry.interiors:
            new_interior = []
            for point in interior.coords:
                if point not in seen_points:
                    seen_points.add(point)
                    new_interior.append(point)
            new_interiors.append(new_interior)

        return Polygon(shell=new_exterior, holes=new_interiors)
    else:
        return geometry


if __name__ == "__main__":
    read_input_csv()
