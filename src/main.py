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


# Association between variable name and relation name 
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

# Association between variable name and target GPKG layer name
GPKG_LAYERS = {
	"equipement_ign": "equipement_de_transport",
	"troncon_ign": "troncon_de_route",
}

# Association between data provider and columns for creating subsets
QUERY_COLUMNS = {
	"corine": "CODE_12",
	"ign": "nature",
	"osm": "fclass",
}

# Association between variable name and values for subsetting features
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


# ----------------------------------------------------------------------
# PROCEDURE
# ----------------------------------------------------------------------


# Setup working directories

current_directory = path.abspath(getcwd())
if path.split(current_directory)[-1] != "src":
	current_directory = path.join(current_directory, "src")

INPUT_DIRECTORY = path.join(path.dirname(current_directory),'input')
OUTPUT_DIRECTORY = path.join(path.dirname(current_directory), 'output')

for directory in (INPUT_DIRECTORY, OUTPUT_DIRECTORY):
	if not exists(directory):
		mkdir(directory)

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
