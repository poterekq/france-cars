# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------
# Author: Quentin Poterek
# Creation date: 12/06/2022
# Version: 1.0
#-----------------------------------------------------------------------


"""
My docstring
"""


# ----------------------------------------------------------------------
# IMPORTS
# ----------------------------------------------------------------------


import os.path as path
from getpass import getpass
from os import getcwd, mkdir

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from lib import *


# ----------------------------------------------------------------------
# DEFINITIONS
# ----------------------------------------------------------------------


# Associate variable name with PostGIS relation name 
RELATIONS = {
	"commune": "commune",
	"corine": "corine",
	"departement": "departement",
	"equipement": "equipement",
	"equipement_ign": "equipement_ign",
	"equipement_osm": "equipement_osm",
	"espace_voiture": "espace_voiture",
	"troncon": "troncon",
	"troncon_buffer": "troncon_buffer",
}

# Associate variable name with target GPKG layer name
GPKG_LAYERS = {
	"equipement_ign": "equipement_de_transport",
	"troncon_ign": "troncon_de_route",
}

# Associate data provider with columns for querying and creating subsets
QUERY_COLUMNS = {
	"corine": "CODE_12",
	"ign": "nature",
	"osm": "fclass",
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
INPUT_COLUMNS = {
	"corine": ["ID", "CODE_12", "geometry"],
	"equipement_ign": ["cleabs", "nature", "geometry"],
	"equipement_osm": ["osm_id", "fclass", "geometry"],
	"troncon_ign": ["cleabs", "nature", "largeur_de_chaussee", 
		"geometry"],
}

# Output names for subsetted columns
OUTPUT_COLUMNS = {
	"corine": ["id", "classe", "geometry"],
	"equipement_osm": ["cleabs", "nature", "geometry"],
	"troncon_ign": ["cleabs", "nature", "largeur", "geometry"],
}

# Spatial information for querying data
TARGET_SRID = 2154
DEPARTEMENT = 67

# Filenames
FILE_COMMUNE = "COMMUNE.shp"
FILE_CORINE = "CLC12_FR.shp"
FILE_IGN = "topo.gpkg"
FILE_OSM = "gis_osm_traffic_a_free_1.shp"


# ----------------------------------------------------------------------
# PROCEDURE
# ----------------------------------------------------------------------


# Setup working directories and paths

current_directory = path.abspath(getcwd())
if path.split(current_directory)[-1] != "src":
	current_directory = path.join(current_directory, "src")

INPUT_DIRECTORY = path.join(path.dirname(current_directory),'input')
OUTPUT_DIRECTORY = path.join(path.dirname(current_directory), 'output')

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
(1) Please, provide information for creating a connection to 
your PostGIS database, where data will be stored and processed.
""")

credentials = Credentials()

for attribute, value in credentials.__dict__.items():
	_ = f"{attribute} [{value}]: "
	_ = getpass(_) if attribute == "password" else input(_)
	setattr(credentials, attribute, _ if _ else value)
