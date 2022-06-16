# -*- coding: utf-8 -*-

# -----------------------------------------------------------------------------
# Author: Quentin Poterek
# Creation date: 08/06/2022
# Version: 1.1
#------------------------------------------------------------------------------


"""
Utilities for processing spatial data with a PostGIS backend.
"""


import itertools
from dataclasses import dataclass
from os.path import exists, join
from typing import Iterable, Optional, Union

import geopandas as gpd
import sqlalchemy


def flatten(iterable: Iterable) -> Iterable:
	return list(itertools.chain(*iterable))


def check_files(
	*args: str
) -> None:
	"""
	Check whether or not the files exist on the system.

	Parameters
	----------
		args : str or iterable of str
			Files to be looked for on the system.
	
	Raises
	------
		FileNotFoundError: If one of the provided files is not found.

	Examples
	--------
	If any of these files does not exist on the system, Python will
	raise an error.
	
	>>> check_files("a.shp", "b.gpkg", "./c.shp", "d.txt")
	"""
	for f in args:
		if not exists(f):
			raise FileNotFoundError(f"'{f}' does not exist!")


def extract_features(
	gdf: gpd.GeoDataFrame, 
	attribute: str, 
	values: Iterable[str]
) -> gpd.GeoDataFrame:
	"""
	Extract features whose attribute value is contained in the provided 
	values.

	Parameters
	----------
		gdf : GeoDataFrame
			GeoPandas dataframe from which features are to be extracted.
		attribute : str
			Attribute used for extracting features.
		values : iterable of str
			List of valid values that the features' attribute should 
			match.
	
	Returns
	-------
		GeoDataFrame
			GeoPandas dataframe with extracted features.
	
	Examples
	--------
	Create a geodataframe from a dictionary.

	>>> d = {"a": [...], "b": [...], "c": [...], "geometry": [...]}
	>>> data = gpd.GeoDataFrame(d, crs="EPSG:2154")
	>>> data
	 	a	b	c	geometry
	0	0	0	1	...
	1	1	2	1	...

	Extract rows from `data` where the value associated with 
	`query_column` is contained in the `categories` iterable.

	>>> query_column = "b"
	>>> categories = [1, 2, 3]
	>>> ways = extract_features(data, query_column, categories)
	>>> ways
	 	a	b	c	geometry
	1	1	2	1	...
	"""
	return gdf[gdf[attribute].isin(values)]


def subset_columns(
	gdf: gpd.GeoDataFrame, 
	src_columns: Iterable[str], 
	tgt_columns: Optional[Iterable[str]] = None
) -> gpd.GeoDataFrame:
	"""
	Select a subset of columns from a GeoPandas dataframe.

	Parameters
	----------
		gdf : GeoDataFrame
			GeoPandas dataframe to subset.
		src_columns : iterable of str
			Original names of the columns to be extracted.
		tgt_columns : iterable of str, optional
			Replacement names for those provided in `src_columns`.
			They should be in the same order as that of `src_columns`.
	
	Returns
	-------
		GeoDataFrame
			GeoPandas dataframe with the selected columns.
	
	Examples
	--------
	Create a geodataframe from a dictionary.

	>>> d = {"a": [...], "b": [...], "c": [...], "geometry": [...]}
	>>> data = gpd.GeoDataFrame(d, crs="EPSG:2154")
	>>> data
	 	a	b	c	geometry
	0	0	0	1	...
	1	1	2	1	...

	Create a subset of `data` by extracting columns specified in
	`src_columns`.

	>>> src_columns = ["a", "b", "geometry"]
	>>> ways = subset_columns(data, src_columns)
	>>> ways
	 	a	b	geometry
	0	0	0	...
	1	1	2	...

	It is also possible to rename columns from `src_columns` by 
	passing an iterable containing target names, and with a length equal
	to that of `src_columns`.

	>>> tgt_columns = ["y", "z", "geometry"]
	>>> ways = subset_columns(data, src_columns, tgt_columns)
	>>> ways
	 	y	z	geometry
	0	0	0	...
	1	1	2	...
	"""
	gdf = gdf[src_columns]

	if tgt_columns is not None:
		gdf.columns = tgt_columns
	
	return gdf


def has_single_geometry_type(
	geometry_type: Iterable[str]
) -> bool:
	"""
	Check whether or not the provided tuple contains a single geometry 
	type.

	Parameters
	----------
		geometry_type : iterable of str
			Tuple containing geometry type(s).
	
	Returns
	-------
		bool
			Whether or not the tuple contains a single geometry type.
	
	Examples
	--------
	Get geometry types from a spatial relation.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()
	>>> geometry_types = get_geometry_type(cursor, "communes")

	Check whether or not the spatial relation contains features with the
	same geometry type.

	>>> has_single_geometry_type(geometry_types)
	False
	"""
	return len(geometry_type) == 1


def convert_st_to_type(
	geometry: str, 
	allow_multi: bool = True
) -> str:
	"""
	Convert PostGIS geometry type to regular geometry type.

	Parameters
	----------
		geometry : str
			String containing a PostGIS geometry type. 
		allow_multi : bool, default True
			Whether or not to allow multipart geometry.
	
	Returns
	-------
		str
			A generic geometry type.
	
	Examples
	--------
	Get the geometry type of a spatial relation.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()
	>>> geometry_types = get_geometry_type(cursor, "communes")
	>>> geometry_types
	[('ST_MultiPolygon',)]

	Convert the PostGIS geometry type into a generic form.

	>>> convert_st_to_type(geometry_types[0][0])
	>>> 'MultiPolygon'

	It is also possible to convert a multipart geometry to its
	singlepart equivalent by setting `allow_multi` to False.

	>>> convert_st_to_type(geometry_types[0][0], false)
	>>> 'Polygon'	
	"""
	if geometry[:3] != "ST_":
		raise ValueError(
			"Input geometry type is not compatible with PostGIS!"
		)
	geometry = geometry[3:]
	if (geometry[:5] == "Multi" and not allow_multi):
		return geometry[5:]
	return geometry


@dataclass
class Credentials:
	"""
	A class used to store and format credentials for connecting to a
	PostgreSQL database.

	Attributes
	---------- 
		host : str, default "localhost"
			The host for the database.
		user : str, default "postgres"
			The user to connect to the database.
		password: str, default "postgres"
			The password used for connecting the user to the database.
		database : str, default "geodata"
			The database to connect to.
		port : str, default "5432"
			The port used for accessing the database.
	
	Methods
	-------
	get_psycopg2_string()
		Format credentials for psycopg2.
	get_sqlalchmy_string()
		Format credentials for sqlalchemy.
	get_credentials(backend)
		Format credentials for the input backend.
	"""
	host: str = "localhost"
	user: str = "postgres"
	password: str = "postgres"
	database: str = "geodata"
	port: str = "5432"


	def get_psycopg2_string(self) -> str:
		"""
		Return a formatted string for connecting to a PostgreSQL
		database using psycopg2.

		Returns
		-------
			str
				A formatted string ready to be passed to psycopg2.
		"""
		return (f"user={self.user} "
		        f"password={self.password} "
				f"host={self.host} "
				f"port={self.port} "
				f"dbname={self.database}")


	def get_sqlalchmy_string(self) -> str:
		"""
		Return a formatted string for connecting to a PostgreSQL
		database using sqlalchemy.

		Returns
		-------
			str
				A formatted string ready to be passed to sqlalchemy.
		"""
		return ("postgresql://"
		        f"{self.user}"
				f":{self.password}"
				f"@{self.host}"
				f":{self.port}"
				f"/{self.database}")


	def get_credentials(self, backend: str = "sqlalchemy") -> str:
		"""
		Return a formatted string for connecting to a PostgreSQL 
		database.

		Parameters
		----------
			backend : str, default "sqlalchemy"
				The backend technology used for getting acces to a 
				PostgreSQL session. Supported values are "psycopg2" and
				"sqlalchemy".

		Returns
		-------
			str
				A formatted string ready to be passed to a PostgreSQL
				session manager for connecting to a database.
		
		Raises
		------
			ValueError: If the provided backend is not supported. 
			Supported backends are psycopg2 and sqlalchemy.
		
		Examples
		--------
		Create an instance of the `Credentials` class.

		>>> credentials = Credentials()

		Return a formatted string from the instance's attributes.

		>>> credentials.get_credentials("sqlalchemy")
		postgresql://postgres:postgres@localhost:5432/geodata
		"""
		match backend:
			case "sqlalchemy":
				return self.get_sqlalchmy_string()
			case "psycopg2":
				return self.get_psycopg2_string()
			case default:
				raise ValueError((
					f"'{backend}' is not a supported backend! "
					"Use either 'psycopg2' or 'sqlalchemy'."
				))


class SqlProcessor:
	"""
	"""
	def __init__(
		self, 
		engine: sqlalchemy.engine.base.Engine,
		sql_directory: str
	) -> None:
		"""
		"""
		self.engine = engine
		self.sql_directory = sql_directory


	def execute_query(
		self,
		mode: str,
		query: str, 
		*parameters: Union[str, int, float]
	) -> None:
		"""
		Execute a query from an external SQL file, formatted with input
		parameters.

		Parameters
		----------
			engine : sqlalchemy.engine.base.Engine
				Engine bound to a PostgreSQL database.
			path : str
				Either a SQL query or a path to the SQL query file.
			mode : str
				Specify whether the query is distributed as a string
				(string mode) or as a file to read (file mode). This
				parameter can take the following values: file, string.
			parameters : iterable of str, int, float
				Parameters used for formatting the SQL query.
		
		Raises
		------
			ValueError: If the provided mode is not supported. Supported
				modes are "file" and "string".

		Examples
		--------
		Create an engine for connecting to a PostgreSQL database.

		>>> engine = sqlalchemy.create_engine(...)
		>>> cursor = connection.cursor()

		Execute a SQL query from an external file in file mode.

		>>> execute_query(
			engine, "./my_query.sql", 
			"commune", "INSEE_COM", "97%"
		)
		
		Execute a SQL query from a string in string mode.
		
		>>> query = '''
		DELETE FROM public."%s"
		WHERE "%s" LIKE '%s';
		'''
		>>> execute_query(
			engine, query, "string",
			"commune", "INSEE_COM", "97%"
		)
		"""
		
		if mode not in ("file", "string"):
			raise ValueError(f"{mode} is not a supported mode!")

		if mode == "file":
			with open(query) as f:
				query = f.read()

		query = sqlalchemy.text(query.format(*parameters))
		
		with self.engine.connect().execution_options(autocommit=True) as conn:
			result = conn.execute(query)
			
		return result


	def get_srid(
		self, 
		relation: str
	) -> int:
		"""
		"""
		srid = self.execute_query(
			"file",
			join(self.sql_directory, "select_srid.sql"),
			relation
		)

		return srid.first()[0]


	def is_same_srid(
		self, 
		relation_a: str, 
		relation_b: str
	) -> bool:
		"""
		"""
		srid_a = self.get_srid(relation_a)
		srid_b = self.get_srid(relation_b)

		return srid_a == srid_b


	def create_spatial_index(
		self,
		relation: str
	) -> None:
		"""
		"""
		self.execute_query(
			"file",
			join(self.sql_directory, "create_spatial_index.sql"),
			relation
		)


	def singlepart_to_multipart(
		self,
		relation: str
	) -> None:
		"""
		"""
		self.execute_query(
			"file",
			join(self.sql_directory, "singlepart_to_multipart.sql"),
			relation
		)


	def map_geometry_type(
		self,
		relation: str,
		allow_multi: bool = True
	) -> str:
		"""
		"""
		# Get geometry type(s) from relation
		geometry_types = self.execute_query(
			"file",
			join(self.sql_directory, "select_distinct_geometry_type.sql"),
			relation
		)

		geometry_types = flatten(geometry_types)

		# Map geometry type when possible
		if has_single_geometry_type(geometry_types):
			geometry_type = convert_st_to_type(geometry_types[0], allow_multi)
		else:
			raise ValueError(
				f"'{relation}' has more than one geometry type!"
			)
		
		return geometry_type


	def project_geometry(
		self,
		relation: str,
		srid: Union[int, str]
	) -> None:
		"""
		"""
		# Get standard geometry type from relation
		geometry_type = self.map_geometry_type(relation)

		# Project spatial relation into target SRID
		self.execute_query(
			"file",
			join(self.sql_directory, "alter_geometry_srid.sql"),
			relation, geometry_type, srid
		)
	

	def project_3d_to_2d(
		self,
		relation: str,
		srid: Union[int, str]
	) -> None:
		"""
		"""
		# Get standard geometry type from relation
		geometry_type = self.map_geometry_type(relation)

		# Remove third dimension from relation
		self.execute_query(
			"file",
			join(self.sql_directory, "alter_geometry_force_2d.sql"),
			relation, geometry_type, srid
		)


	def intersect_geometries(
		self,
		relation_a: str, 
		relation_b: str, 
		fields_a: Iterable[str] = [], 
		fields_b: Iterable[str] = [], 
		as_view: bool = False, 
		out_name: Optional[str] = None, 
		build_index: bool = True
	) -> None:
		"""
		Return a spatial relation representing the intersection of two 
		spatial relations provided as input.

		Parameters
		----------
			cursor : psycopg2.extensions.cursor
				Cursor bound to an open PostgreSQL connection.
			relation_a : str
				Relation containing geometries to intersect with
				`relation_b`.
			relation_b : str
				Relation containing geometries to intersect with
				`relation_a`.
			fields_a : iterable of str
				Fields of relation_a to pass to the intersection result.
			fields_b : iterable of str
				Fields of relation_b to pass to the intersection result.
			as_view : bool, default False
				Whether or not to create a view. When `False`, a table
				is created. When `True`, a view is created.
			out_name : str, optional
				Name of the relation to create. By default, `relation_b`
				is replaced with the intersection result.
			build_index : bool, default True
				Whether or not to build a spatial index for the
				intersection result. The option is not available for
				views.
		
		Examples
		--------
		Create a cursor from an open PostgreSQL connection.

		>>> connection = psycopg2.connect(...)
		>>> cursor = connection.cursor()

		Update the geometry of spatial relation `b` after intersecting
		it with spatial relation `a`. In this following example, the
		only information kept from both relations is the resulting
		geometry.

		>>> intersect_geometries(cursor, "a", "b")

		To create a new relation `c` instead of overwriting `b`, one may
		specify the `out_name` attribute.

		>>> intersect_geometries(cursor, "a", "b", out_name="c")

		A table is created by default. However, when `out_name` is
		provided, it is possible to set `as_view` to `True` in order to
		create a view instead.

		>>> intersect_geometries(
			cursor, "a", "b", 
			as_view=True, out_name="c"
		)

		In addition to the geometry resulting from the intersection of
		two relations, one can also specify fields to keep. In this
		case, it is preferable to remove the initial `geom` or
		`geometry` attributes from both `fields_a` and `fields_b`.
		Indeed, other functions or GIS softwares might be unsure whether
		to use the geometry resulting from the intersection or that of
		the initial spatial relations.

		Below, an example where the identifier from `a` and all
		attributes from `b` are kept. Note that the initial geometry of
		both relations was not included.

		>>> intersect_geometries(
			cursor, "a", "b", 
			fields_a=["id"], 
			fields_b=["x", "y", "z"]
		)
		"""
		if not self.is_same_srid(relation_a, relation_b):
			raise ValueError(f"Input geometries do not share the same SRID!")

		DIMENSION = {
			"Point": 0,
			"LineString": 1,
			"Polygon": 2,
		}

		fields_a = [f'a."{field}"' for field in fields_a]
		fields_b = [f'b."{field}"' for field in fields_b]
		fields = fields_a + fields_b
		fields = ", ".join(fields)

		geometry_type_b = self.map_geometry_type(relation_b, False)

		relation = f"{relation_b}_tmp" if out_name is None else out_name
		relation_type = "VIEW" if as_view else "TABLE"

		self.execute_query(
			"file",
			join(self.sql_directory, "create_intersection_geometries.sql"),
			relation_type, relation, fields, relation_a, relation_b,
			DIMENSION[geometry_type_b]
		)

		if out_name is None:
			query = f"""
			DROP TABLE public."{relation_b}";

			ALTER TABLE public."{relation}"
			RENAME TO "{relation_b}";
			"""
		
			self.execute_query("string", query)

		if build_index and relation_type == "TABLE":
			if out_name is None:
				self.create_spatial_index(relation_b)
			else:
				self.create_spatial_index(out_name)
	

	def dissolve_geometries(
		self,
		relation_a: str, 
		relation_b: str, 
		out_name: str, 
		as_view: bool = False, 
		build_index: bool = True
	) -> None:
		"""
		Return a spatial relation representing the union of two 
		spatial relations provided as input.

		Parameters
		----------
			relation_a : str
				Relation containing geometries to aggregate with relation_b.
			relation_b : str
				Relation containing geometries to aggregate with relation_a.
			out_name : str
				Name of the relation to create.
			as_view : bool, default False
				Whether or not to create a view. When False, a table is 
				created. When True, a view is created.
			build_index : bool, default True
				Whether or not to build a spatial index for the aggregation
				result. The option is not available for views.
		
		Examples
		--------
		Create a cursor from an open PostgreSQL connection.

		>>> connection = psycopg2.connect(...)
		>>> cursor = connection.cursor()

		Create a new spatial relation `c` from the union of two spatial
		relations `a` and `b`.

		>>> aggregate_relations(cursor, "a", "b", "c")

		By default, a new table is created. But one might also want to
		create a view. In this case, `as_view` must be set to `True`.

		>>> aggregate_relations(cursor, "a", "b", "c", as_view=True)
		"""

		if not self.is_same_srid(relation_a, relation_b):
			raise ValueError(f"Input geometries do not share the same SRID!")
		
		srid = self.get_srid(relation_a)

		relation_type = "VIEW" if as_view else "TABLE"

		self.execute_query(
			"file",
			join(self.sql_directory, "create_dissolve_geometries.sql"),
			relation_type, out_name, srid, relation_a, relation_b
		)

		if build_index and relation_type == "TABLE":
			self.create_spatial_index(out_name)


	def clear_relations(
		self,
		relation_type: str,
		relations: Iterable[str]
	) -> None:
		"""
		"""

		if relation_type.upper() not in ("TABLE", "VIEW"):
			raise ValueError(
				"{relation_type} is not a valid relation type! "
				"Valid values are 'TABLE' or 'VIEW'."
			)

		for relation in (relations):
			self.execute_query(
				"file",
				join(self.sql_directory, "drop_relation.sql"), 
				relation_type, relation, "CASCADE"
			)
