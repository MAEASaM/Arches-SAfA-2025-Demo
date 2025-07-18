# This script is used to clean the CSV files for the Arches upload
# The script is written by Mahmoud Abdelrazek and Renier van der Merwe

# TODO:
# 1. Refactor actor_uuid_format function to generate the actor dict only once # Done
# 2. Add check for the geomtry and fix duplicate point problem
# 3. split the filtering function to aliases and data fixes
# 4. generalise the script to work with other csv files
# 5. Allow for multiple date formates to be corrected to the require formate
# 6. Add section that orgnises the MAEASaM ID numericaly from the input csv


# Data sheets

input_csv_file = r"E:\MAEASaM\MAEASaM_desktop\Arches\Arches Git\Arches-ETL\Country files\Tanzania\RSRM Year 3 (Tanzania).xlsx - Akinbowale Akintayo_Remote Sensing Data_Corrected.csv"
output_csv_file = "arches_modified.csv"
actor_csv_file = "Actor."


import pandas as pd
import csv
from datetime import datetime
import pathlib
from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon, Point


# find the path of the script
script_path = pathlib.Path(__file__).parent.absolute()

# add script path to the csv files
input_csv_file = script_path / input_csv_file
output_csv_file = script_path / output_csv_file
actor_csv_file = script_path / actor_csv_file


class CSV_cleaning_script:
    def __init__(self):
        pass

    def read_input_csv(self) -> pd.DataFrame:
        with open(input_csv_file, "r") as input_csv_file_object:
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
    return "ResourceID" in file_reader.fieldnames


def write_output_csv(file_reader: csv.DictReader) -> None:
    with open(output_csv_file, "w") as zim_data_overwritten:
        # read actor uuid csv
        actor_uuid_dict = read_actor_uuid_csv()

        fieldnames = list(file_reader.fieldnames)
        missing_resource_id = check_for_resource_id_column(file_reader)

        if not missing_resource_id:
            fieldnames = ["ResourceID"] + fieldnames

        # Add new copyright/access fields to output if they don't exist
        if "Access Level" not in fieldnames:
            fieldnames.append("Access Level")
        if "Copyright Information" not in fieldnames:
            fieldnames.append("Copyright Information")

        writer = csv.DictWriter(zim_data_overwritten, fieldnames=fieldnames)

        writer.writeheader()
        for row in file_reader:
            if not missing_resource_id:
                row["ResourceID"] = row["MAEASaM ID"]
                row["Geometry type"] = row[
                    "Geometry type"
                ]  # This is just a fix for my csv. We will have to change it to do this only if a WKT column is present

            row = data_filter(row)
            row = date_format_all_coloums(row)
            row = actor_uuid_format(row, actor_uuid_dict)
            row = clean_geomtry_based_on_type(row)
            writer.writerow(row)


def data_filter(row: dict) -> dict:
    # Evidence field filters
    if "Evidence" in row:
        if row["Evidence"] == "Building":
            row["Evidence"] = "Structure"
        if row["Evidence"] == "Structures":
            row["Evidence"] = "Structure"
        if row["Evidence"] == "Unknown":
            row["Evidence"] = "Standing remains"
        if row["Evidence"] == "Enclosures":
            row["Evidence"] = "Enclosure"
        if row["Evidence"] == "Terracing":
            row["Evidence"] = "Terrace"
        if row["Evidence"] == "Wall":
            row["Evidence"] = "Walling"
        if row["Evidence"] == "Stone Mound":
            row["Evidence"] = "Stone mound"
        if row["Evidence"] == "Soil Mark":
            row["Evidence"] = "Soil mark"
        if row["Evidence"] == "Complex Enclosure":
            row["Evidence"] = "Complex enclosure"
        if row["Evidence"] == "Complex enclosures":
            row["Evidence"] = "Complex enclosure"
        if row["Evidence"] == "Stone Circle":
            row["Evidence"] = "Stone circle"
        if row["Evidence"] == "Complex Structure":
            row["Evidence"] = "Complex structure"
        if row["Evidence"] == "Earth":
            row["Evidence"] = "Terrace"
        if row["Evidence"] == "Soil discoloration":
            row["Evidence"] = "Discolouration"

    # Image type field filters
    if "Image type" in row:
        if row["Image type"] == "CNES / Airbus":
            row["Image type"] = "CNES Airbus"
        if row["Image type"] == "CNES/Airbus":
            row["Image type"] = "CNES Airbus"
        if row["Image type"] == "Bung":
            row["Image type"] = "Bing"

    # Climatic zone field filters
    if "Climatic zone" in row:
        if row["Climatic zone"] == "Dry Winter-Hot Summer (t)":
            row["Climatic zone"] = "Temperate Dry Winter Hot summer"
        if row["Climatic zone"] == "Dry Winter-Warm Summer (t)":
            row["Climatic zone"] = "Temperate Dry Winter Warm summer"
        if row["Climatic zone"] == "Cwb":
            row["Climatic zone"] = "Temperate Dry Winter Warm summer"
        if row["Climatic zone"] == "Cwa":
            row["Climatic zone"] = "Temperate Dry Winter Hot summer"
        if row["Climatic zone"] == "Bsh":
            row["Climatic zone"] = "Steppe Hot"

    # Surveyor name field filters
    if "Surveyor name" in row:
        if row["Surveyor name"] == "Ed Burnett":
            row["Surveyor name"] = "Ed Burnett, Edward Burnett"
        if row["Surveyor name"] == "Renier van der Merwe":
            row["Surveyor name"] = "Renier Hendrik van der Merwe"

    # Threat assessor name field filters
    if "Threat assessor name" in row:
        if row["Threat assessor name"] == "Ed Burnett":
            row["Threat assessor name"] = "Ed Burnett, Edward Burnett"
        if row["Threat assessor name"] == "Renier van der Merwe":
            row["Threat assessor name"] = "Renier Hendrik van der Merwe"

    # Measurement unit field filters
    if "Measurement unit" in row:
        if row["Measurement unit"] == "m2":
            row["Measurement unit"] = "square meter"
        if row["Measurement unit"] == "Square Meter":
            row["Measurement unit"] = "square meter"
        if row["Measurement unit"] == "m":
            row["Measurement unit"] = "Meter"
        if row["Measurement unit"] == "meter":
            row["Measurement unit"] = "Meter"
        if row["Measurement unit"] == "Hectares":
            row["Measurement unit"] = "Hectare"

    # Measurement type field filters
    if "Measurement type" in row:
        if row["Measurement type"] == "Perimiter":
            row["Measurement type"] = "Area"

    # Additional information field filters
    if "Additional information" in row:
        if row["Additional information"] == "M":
            row["Additional information"] = ""

    # Survey type field filters
    if "Survey type" in row:
        if row["Survey type"] == "Historic maps check":
            row["Survey type"] = "Historic map check"
        if row["Survey type"] == "Topographic":
            row["Survey type"] = "Topographic other"

    # Threat type field filters
    if "Threat type" in row:
        if row["Threat type"] == "Conflict":
            row["Threat type"] = "War"
        if row["Threat type"] == "Occupation expansion":
            row["Threat type"] = "Urbanisation"
        if row["Threat type"] == "Environmental":
            row["Threat type"] = "Episodic events"
        if row["Threat type"] == "Environmental ":
            row["Threat type"] = "Episodic events"
        if row["Threat type"] == "Anthopogenic":
            row["Threat type"] = "Agriculture"
        if row["Threat type"] == "Anthropogenic":
            row["Threat type"] = "Agriculture"
        if row["Threat type"] == "Antrhopgenic":
            row["Threat type"] = "Agriculture"
        if row["Threat type"] == "Antrhopogenic":
            row["Threat type"] = "Agriculture"
        if row["Threat type"] == "Development":
            row["Threat type"] = "Urbanisation"

    # Evidence shape field filters
    if "Evidence shape" in row:
        if row["Evidence shape"] == "Ring":
            row["Evidence shape"] = "Circular"

    # Ground truthed field filters
    if "Ground truthed" in row:
        if row["Ground truthed"] == "#REF!":
            row["Ground truthed"] = "No"

    # Land use land cover field filters
    if "Land use land cover" in row:
        if row["Land use land cover"] == "Built-up":
            row["Land use land cover"] = "Built up"
        if row["Land use land cover"] == "grassland":
            row["Land use land cover"] = "Grassland"
        if row["Land use land cover"] == "Cropland":
            row["Land use land cover"] = "Cultivated"
        if row["Land use land cover"] == "Tree Cover":
            row["Land use land cover"] = "Thicket"
        if row["Land use land cover"] == "Shurbland":
            row["Land use land cover"] = "Scrub"
        if row["Land use land cover"] == "Bare rock or Soil discoloration":
            row["Land use land cover"] = "Bare rock or soil"
        if row["Land use land cover"] == "Bare":
            row["Land use land cover"] = "Bare rock or soil"

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
    # Format date fields if they exist
    if "Survey date" in row:
        row["Survey date"] = convert_date_format(row["Survey date"])

    if "Date of imagery" in row:
        row["Date of imagery"] = convert_date_format(row["Date of imagery"])
        if "Survey date" in row:
            row["Date of imagery"] = row["Date of imagery"].replace(
                "20XX", row["Survey date"]
            )
            row["Date of imagery"] = row["Date of imagery"].replace(
                "1900-01-00", row["Survey date"]
            )

    if "Threat assessment date" in row:
        row["Threat assessment date"] = convert_date_format(
            row["Threat assessment date"]
        )

    if "Image used date" in row:
        row["Image used date"] = convert_date_format(row["Image used date"])

    return row


def actor_uuid_format(row: dict, actor_uuid_dict) -> dict:
    # List of actor fields that need UUID formatting
    actor_fields = [
        "Surveyor name",
        "Threat assessor name",
        "Assessor name",
        "Site data information Reference Institution",
    ]

    # Format actor fields with UUID relationships
    for field in actor_fields:
        if field in row and row[field] and row[field] in actor_uuid_dict:
            row[field] = (
                "[{'resourceId': '"
                + actor_uuid_dict[row[field]]
                + "','ontologyProperty': 'http://www.cidoc-crm.org/cidoc-crm/P11_had_participant', 'resourceXresourceId': '','inverseOntologyProperty': 'http://www.cidoc-crm.org/cidoc-crm/P140_assigned_attribute_to'}]"
            )

    # Add new copyright/access nodes with default values
    row["Access Level"] = "Public"
    row["Copyright Information"] = "CC BY-NC-SA"

    return row


def clean_geomtry_based_on_type(row: dict) -> dict:
    if "Geometry type" in row:
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
