# -*- coding: utf-8 -*-

# -----------------------------------------------------------------------------
# Author: Quentin Poterek
# Creation date: 12/06/2022
# Version: 1.1
#------------------------------------------------------------------------------


"""
My docstring
"""


# -----------------------------------------------------------------------------
# IMPORTS
# -----------------------------------------------------------------------------


import os.path as path
import sys
from getpass import getpass
from os import getcwd, mkdir

import pandas as pd
from sqlalchemy import create_engine

from lib import *
from managers import *


# -----------------------------------------------------------------------------
# CLASSES, CONSTANTS AND VARIABLES
# -----------------------------------------------------------------------------

# Associate variable name with PostGIS relation name

IN_RELATIONS = {
	"commune": "commune",
	"corine": "corine",
	"equipement_ign": "equipement_ign",
	"equipement_osm": "equipement_osm",
	"troncon_ign": "troncon_ign",
}

OUT_RELATIONS = {
	"departement": "departement",
	"equipement": "equipement",
	"espace_voiture": "espace_voiture",
	"espace_voiture_commune": "espace_voiture_commune",
	"espace_voiture_urbain": "espace_voiture_urbain",
	"tache_urbaine": "tache_urbaine",
	"tache_urbaine_commune": "tache_urbaine_commune",
	"troncon_ign_buffer": "troncon_ign_buffer",
	"troncon_ign_commune": "troncon_ign_commune",
	"troncon_ign_urbain": "troncon_ign_urbain",
}

ALL_RELATIONS = list(IN_RELATIONS.values()) + list(OUT_RELATIONS.values())

# Associate variable name with target GPKG layer name

GPKG_LAYERS = {
	"equipement_ign": "equipement_de_transport",
	"troncon_ign": "troncon_de_route",
}

# Associate data provider with columns for querying and creating subsets

QUERY_COLUMNS = {
	"corine": "CODE_18",
	"ign": "nature",
	"osm": "fclass",
	"insee": "INSEE_COM",
	"largeur": "largeur",
}

# Associate variable name with values for subsetting features

QUERY_CATEGORIES = {
	"corine": ["111", "112", "121", "122", "123", "124", 
	    "131", "132", "133", "141", "142"],
	"equipement_ign": ["Aire de repos ou de service", 
		"Autre équipement", "Carrefour",  "Parking", "Péage", 
		"Service dédié aux véhicules"],
	"equipement_osm": ["fuel", "parking", "parking_multistorey", 
		"parking_underground", "service"],
	"troncon_ign": ["Bretelle", "Rond-point", "Route à 1 chaussée",
		"Route à 2 chaussées", "Type autoroutier"],
}

# Input columns to subset

IN_COLUMNS = {
	"corine": ["ID", "CODE_18", "geometry"],
	"equipement_ign": ["cleabs", "nature", "geometry"],
	"equipement_osm": ["osm_id", "fclass", "geometry"],
	"troncon_ign": ["cleabs", "nature", "largeur_de_chaussee", 
		"geometry"],
}

# Output names for subsetted columns

OUT_COLUMNS = {
	"corine": ["id", "classe", "geometry"],
	"equipement_ign": ["cleabs", "nature", "geometry"],
	"equipement_osm": ["cleabs", "nature", "geometry"],
	"troncon_ign": ["cleabs", "nature", "largeur", "geometry"],
}

# Column names for output table

RESULT_COLUMNS = [
	# Commune identifier
	"num_insee",
	# Commune area (km²)
	"ArCom",
	# Transport infrastructures area (km²) at commune level
	"ArVoiCom",
	# Transport infrastructures perimeter (km) at commune level
	"PeVoiCom",
	# Total roadway length (km) at commune level
	"LeTroCom",
	# Urban footprint area (km²)
	"ArUrb",
	# Transport infrastructures area (km²) at urban footprint level
	"ArVoiUrb",
	# Transport infrastructures perimeter (km) at urban footprint level
	"PeVoiUrb",
	# Total roadway length (km) at urban footprint level
	"LeTroUrb",
	# Part of transport infrastructures in urban footprint (%)
	"PaArVoi",
	# Part of roadways in urban footprint (%)
	"PaLeTro",
	# Part of transport infrastructures area wrt commune area (%)
	"PaArVoiCom",
	# Part of transport infrastructures area wrt urban footprint area (%)
	"PaArVoiUrb",
	# Ratio of total roadway length (km) wrt commune area (km²)
	"RaLeArTroCom",
	# Ratio of total roadway length (km) wrt urban footprint area (km²)
	"RaLeArTroUrb",
	# Ratio between the perimeter and the square root area of
	# transport infrastructures at commune level
	"RaPeArVoiCom",
	# Ratio between the perimeter and the square root area of
	# transport infrastructures at urban footprint level
	"RaPeArVoiUrb",
]

# Get paths for directories

current_directory = path.abspath(getcwd())
if path.split(current_directory)[-1] != "src":
	current_directory = path.join(current_directory, "src")

SQL_DIRECTORY = path.join(path.dirname(current_directory),'sql')
INPUT_DIRECTORY = path.join(path.dirname(current_directory),'input')
OUTPUT_DIRECTORY = path.join(path.dirname(current_directory), 'output')

# Information for querying and processing data

DEPARTEMENT = int(sys.argv[1])
OFFSET_BUFFER_TRONCON = 2
TARGET_SRID = 2154


# -----------------------------------------------------------------------------
# DOWNLOAD AND PREPARE DATA
# -----------------------------------------------------------------------------

# Prepare information for downloading data ------------------------------------

# Get old region name for downloading from GeoFabrik (OSM)

insee_path = os.path.join(path.dirname(current_directory), "table_insee.csv")
insee = pd.read_csv(insee_path, sep='\t')
osm_region = insee[insee["CODE_DPT"]==str(DEPARTEMENT)].OSM_REG.item()
osm_region = osm_region.lower()

# Additionnal information

DEPARTEMENT_PAD = str(DEPARTEMENT).zfill(3)
IGN_RELEASE_DATE = "2022-03-15"

# Filenames

FILE_COMMUNE = "COMMUNE.shp"
FILE_CORINE = "CLC18_FR.shp"
FILE_IGN = f"BDT_3-0_GPKG_LAMB93_D{DEPARTEMENT_PAD}-ED{IGN_RELEASE_DATE}.gpkg"
FILE_OSM = "gis_osm_traffic_a_free_1.shp"
FILE_RESULT = f"analyse_d{DEPARTEMENT}.csv"

# Download data using FileManager ---------------------------------------------

fm = FileManager(INPUT_DIRECTORY)

for f, url, pattern in zip(
	# Filenames
	(FILE_COMMUNE, FILE_CORINE, FILE_IGN, FILE_OSM),
	# URLs
	(UrlManager.ADMIN.format(IGN_RELEASE_DATE),
	 UrlManager.CORINE,
	 UrlManager.BDTOPO.format(DEPARTEMENT_PAD, IGN_RELEASE_DATE),
	 UrlManager.OSM.format(osm_region)),
	# Patterns for extracting from archives
	(r".*[/]COMMUNE[.].*", 
	 r".*[/]CLC18_FR[.].*",
	 r".*gpkg$",
	 r".*traffic_a_free_1[.].*")
):
	f_path = os.path.join(INPUT_DIRECTORY, f)

	if exists(f_path):
		print(f"{f} already exists! It won't be downloaded.")
	else:
		archive_name = fm.download_file(url, INPUT_DIRECTORY)
		fm.extract(
			os.path.join(INPUT_DIRECTORY, archive_name),
			INPUT_DIRECTORY,
			pattern=pattern,
			delete_zip=True
		)


# -----------------------------------------------------------------------------
# PROCEDURE
# -----------------------------------------------------------------------------

# Setup environment -----------------------------------------------------------

# Prepare working directories and paths

for directory in (INPUT_DIRECTORY, OUTPUT_DIRECTORY):
	if not exists(directory):
		mkdir(directory)

PATH_COMMUNE = path.join(INPUT_DIRECTORY, FILE_COMMUNE)
PATH_CORINE = path.join(INPUT_DIRECTORY, FILE_CORINE)
PATH_IGN = path.join(INPUT_DIRECTORY, FILE_IGN)
PATH_OSM = path.join(INPUT_DIRECTORY, FILE_OSM)

check_files(PATH_COMMUNE, PATH_CORINE, PATH_IGN, PATH_OSM)

# Get credentials for connecting to PostGIS database

print("""
Please, provide information for creating a connection to 
your PostGIS database, where data will be stored and processed.
""")

credentials = Credentials()

for attribute, value in credentials.__dict__.items():
	_ = f"{attribute} [{value}]: "
	_ = getpass(_) if attribute == "password" else input(_)
	setattr(credentials, attribute, _ if _ else value)

# Create a PostgreSQL engine, setup data processor and clear potential residues

engine = create_engine(credentials.get_credentials("sqlalchemy"))
processor = SqlProcessor(engine, SQL_DIRECTORY)
processor.drop_relations("TABLE", ALL_RELATIONS)

# Add files to PostGIS database -----------------------------------------------

print(f"""
{Fmt.BOLD}Read input files and export to PostGIS database...{Fmt.END}
""")

# Read input files

files = IN_RELATIONS.copy()

layers = [
	None,
	None,
	GPKG_LAYERS["equipement_ign"],
	None,
	GPKG_LAYERS["troncon_ign"],
]

for key, path_data, layer in zip(
	list(files.keys()),
	(PATH_COMMUNE, PATH_CORINE, PATH_IGN, PATH_OSM, PATH_IGN),
	layers
):
	try:
		files[key] = gpd.read_file(path_data, layer=layer)
	except Exception:
		print(f"{Fmt.RED}✘ {path_data} could not be read!{Fmt.END}")
		sys.exit()

print(f"{Fmt.GREEN}✔ All files were read successfully!{Fmt.END}")

# Subset input files by rows and columns

for f, provider in zip(
	list(files.keys())[1:],
	("corine", "ign", "osm", "ign"),
):
#	Keep features used by cars (ign, osm) and artificial areas (corine)
	files[f] = extract_features(
		files[f],
		QUERY_COLUMNS[provider],
		QUERY_CATEGORIES[f]
	)
#	Keep relevant columns only
	files[f] = subset_columns(
		files[f],
		IN_COLUMNS[f],
		OUT_COLUMNS[f]
	)

print(f"{Fmt.GREEN}✔ All files were subsetted successfully!{Fmt.END}")

# Export files to PostGIS database

for key in files:
	try:
		files[key].to_postgis(IN_RELATIONS[key], engine, if_exists="replace")
	except Exception:
		print(f"{Fmt.RED}✘ {key} could not be exported to PostGIS!{Fmt.END}")
		processor.drop_relations("TABLE", ALL_RELATIONS)
		sys.exit()

del files

print(f"{Fmt.GREEN}✔ Export to PostGIS was successful!{Fmt.END}")

# Create spatial indexes for tables

for relation in (list(IN_RELATIONS.keys())):
	processor.create_spatial_index(relation)

# Prepare data for analysis ---------------------------------------------------

print(f"""
{Fmt.BOLD} Prepare data for analysis... {Fmt.END}
""")

print("Set proper SRID, geometry type and dimension.")

# Prepare administrative divisions

processor.execute_query(
	"file",
	path.join(SQL_DIRECTORY, "delete_where_like.sql"),
	IN_RELATIONS["commune"], QUERY_COLUMNS["insee"], "97%"
)

processor.singlepart_to_multipart(IN_RELATIONS["commune"])
processor.project_geometry(IN_RELATIONS["commune"], TARGET_SRID)

# Extract target department

processor.execute_query(
	"file",
	path.join(SQL_DIRECTORY, "create_union_where_like.sql"),
	"VIEW", OUT_RELATIONS["departement"], TARGET_SRID, 
	IN_RELATIONS["commune"], QUERY_COLUMNS["insee"], f"{DEPARTEMENT}%"
)

# Project OpenStreetMap data into target SRID

processor.singlepart_to_multipart(IN_RELATIONS["equipement_osm"])
processor.project_geometry(IN_RELATIONS["equipement_osm"], TARGET_SRID)

# Flatten 3D geometry to 2D

for relation in (
	IN_RELATIONS["equipement_ign"], 
	IN_RELATIONS["troncon_ign"]
):
	processor.project_3d_to_2d(relation, TARGET_SRID)

# Crop features to department geometry and keep relevant columns only

print("Perform geoprocessing to match desired analysis level.")

fields = [[OUT_COLUMNS["corine"][1]]]
fields += [values[:-1] for values in list(OUT_COLUMNS.values())[1:]]

for relation_a, relation_b, fields_b in zip(
#	Relations (a)
	[OUT_RELATIONS["departement"]] * 4,
#	Relations (b)
	[IN_RELATIONS["corine"], IN_RELATIONS["equipement_ign"],
	 IN_RELATIONS["equipement_osm"], IN_RELATIONS["troncon_ign"]],
#	Fields (b)
	fields
):
	processor.intersect_geometries(relation_a, relation_b, fields_b=fields_b)

# For roadways with missing width, fill with median value

query = f"""
UPDATE public."{IN_RELATIONS["troncon_ign"]}" AS t1
SET {QUERY_COLUMNS["largeur"]} = (
	SELECT PERCENTILE_CONT(0.5) 
		WITHIN GROUP (ORDER BY {QUERY_COLUMNS['largeur']})
	FROM public."{IN_RELATIONS["troncon_ign"]}" AS t2
	WHERE t2.nature = t1.nature
)
WHERE ({QUERY_COLUMNS['largeur']} IS NULL) 
	OR ({QUERY_COLUMNS['largeur']} = 0);
"""

processor.execute_query("string", query, IN_RELATIONS["troncon_ign"])

# Create buffer around roadways

processor.execute_query(
	"file",
	path.join(SQL_DIRECTORY, "create_union_buffer.sql"),
	"TABLE", OUT_RELATIONS["troncon_ign_buffer"], 
	f"{QUERY_COLUMNS['largeur']} + {OFFSET_BUFFER_TRONCON}", 
	TARGET_SRID, IN_RELATIONS["troncon_ign"]
)

# Dissolve roadways and transport infrastructures into car-dedicated surfaces

for relations in (
#	Transport infrastructures (parkings, roundabouts, etc.)
	(IN_RELATIONS["equipement_ign"], 
	 IN_RELATIONS["equipement_osm"],
	 OUT_RELATIONS["equipement"]),
#	Transport infrastructures and roadways
	(OUT_RELATIONS["troncon_ign_buffer"],
	 OUT_RELATIONS["equipement"],
	 OUT_RELATIONS["espace_voiture"])
):
	processor.dissolve_geometries(relations[0], relations[1], relations[2])

# Dissolve urban features from Corine Land Cover into urban footprint

processor.execute_query(
	"file",
	path.join(SQL_DIRECTORY, "create_union.sql"),
	"TABLE", OUT_RELATIONS["tache_urbaine"], TARGET_SRID, 
	IN_RELATIONS["corine"]
)

print("Intersect features with communes and urban footprint.")

# Intersect roadways and transport infrastructures with communes

for a, b, field, output in zip(
#	Relations (a)
	[IN_RELATIONS["commune"]] * 2,
#	Relations (b)
	[OUT_RELATIONS["espace_voiture"], 
	 IN_RELATIONS["troncon_ign"]],
#	Fields (a)
	[[QUERY_COLUMNS["insee"]]] * 2,
#	Output names
	[OUT_RELATIONS["espace_voiture_commune"], 
	 OUT_RELATIONS["troncon_ign_commune"]]
):
	processor.intersect_geometries(a, b, fields_a=field, out_name=output)

# Intersect urban footprint with communes

processor.intersect_geometries(
	IN_RELATIONS["commune"], 
	OUT_RELATIONS["tache_urbaine"],
	fields_a=[QUERY_COLUMNS["insee"]],
	out_name=OUT_RELATIONS["tache_urbaine_commune"]
)

# Intersect roadways and transport infrastructures with urban footprint

for a, b, field, output in zip(
#	Relations (a)
	[OUT_RELATIONS["tache_urbaine"]] * 2,
#	Relations (b)
	[OUT_RELATIONS["espace_voiture_commune"], 
	 OUT_RELATIONS["troncon_ign_commune"]],
#	Fields (a)
	[[QUERY_COLUMNS["insee"]]] * 2,
#	Output names
	[OUT_RELATIONS["espace_voiture_urbain"],
	 OUT_RELATIONS["troncon_ign_urbain"]]
):
	processor.intersect_geometries(a, b, fields_b=field, out_name=output)

print(f"{Fmt.GREEN}✔ Geoprocessing of data was successful!{Fmt.END}")

# Extract and export analysis results -----------------------------------------

print(f"""
{Fmt.BOLD} Extract analysis results and compute additional data...{Fmt.END}
""")

# Export data from database

result = processor.execute_query(
	"file",
	path.join(SQL_DIRECTORY, "get_result.sql"),
	IN_RELATIONS["commune"], 
	QUERY_COLUMNS["insee"], 
	OUT_RELATIONS["espace_voiture_commune"],
	DEPARTEMENT,
	OUT_RELATIONS["troncon_ign_commune"],
	OUT_RELATIONS["tache_urbaine_commune"],
	OUT_RELATIONS["espace_voiture_urbain"],
	OUT_RELATIONS["troncon_ign_urbain"],
)

result = [row for row in result]

# Format data

result = pd.DataFrame(result)
result.columns=RESULT_COLUMNS[:result.shape[1]]

# Compute additional features

result["PaArVoi"] = result.ArVoiUrb / result.ArVoiCom * 100
result["PaLeTro"] = result.LeTroUrb / result.LeTroCom * 100
result["PaArVoiCom"] = result.ArVoiCom / result.ArCom * 100
result["PaArVoiUrb"] = result.ArVoiUrb / result.ArUrb * 100
result["RaLeArTroCom"] = result.LeTroCom / result.ArCom
result["RaLeArTroUrb"] = result.LeTroUrb / result.ArUrb
result["RaPeArVoiCom"] = result.PeVoiCom / result.ArVoiCom ** 0.5
result["RaPeArVoiUrb"] = result.PeVoiUrb / result.ArVoiUrb ** 0.5

result = result.fillna(0)

# Export results

try:
	result.to_csv(
		path.join(OUTPUT_DIRECTORY, FILE_RESULT), 
		sep="\t", index=False
	)
except Exception:
	print(f"{Fmt.RED}✘ Results could not be saved!{Fmt.END}")
	processor.drop_relations("TABLE", ALL_RELATIONS)
	sys.exit()

print(f"{Fmt.GREEN}✔ Analysis results were saved successfully!{Fmt.END}")
print(f"Path: {path.join(OUTPUT_DIRECTORY, FILE_RESULT)}")

# Cleanup ---------------------------------------------------------------------

print(f"""
{Fmt.BOLD} Clear workspace...{Fmt.END}
""")

processor.drop_relations("TABLE", ALL_RELATIONS)
engine.dispose()

print(f"{Fmt.GREEN}✔ All done!{Fmt.END}\n")
