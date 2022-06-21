# -*- coding: utf-8 -*-

# -----------------------------------------------------------------------------
# Author: Quentin Poterek
# Creation date: 08/06/2022
# Version: 1.2
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


def flatten(lists: Iterable) -> Iterable:
	"""
	Flatten a list of lists into a single non-nested list.

	Parameters
	----------
		lists: iterable
			List of lists.
	
	Returns
	-------
		iterable
			Flattened non-nested list.
	
	Examples
	--------
	Create an irregularly shaped list with nested items.

	>>> my_list = [1, 2, [3, 4], [5, 6, 7], 8, 9]

	Flatten the list. 

	>>> flatten(my_list)
	[1, 2, 3, 4, 5, 6, 7, 8, 9]
	"""
	return list(itertools.chain(*lists))


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
	Create an instance of SqlProcessor.

	>>> processor = SqlProcessor(engine, sql_directory)

	Get geometry types for features contained in the provided 
	relation.

	>>> geometry_types = processor.get_geometry_types("communes")
	>>> geometry_types
	('ST_MultiPolygon', 'ST_Polygon')

	Check whether or not the spatial relation contains features only one
	geometry type.

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
	Create an instance of SqlProcessor.

	>>> processor = SqlProcessor(engine, sql_directory)

	Get geometry types for features contained in the provided 
	relation.

	>>> geometry_types = processor.get_geometry_types("communes")
	>>> geometry_types
	('ST_MultiPolygon',)

	Convert the PostGIS geometry type into a generic form.

	>>> convert_st_to_type(geometry_types[0])
	>>> 'MultiPolygon'

	It is also possible to convert a multipart geometry to its
	singlepart equivalent by setting `allow_multi` to False.

	>>> convert_st_to_type(geometry_types[0], false)
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
			case _:
				raise ValueError((
					f"'{backend}' is not a supported backend! "
					"Use either 'psycopg2' or 'sqlalchemy'."
				))


class SqlProcessor:
	"""
	A class used to do CRUD processing on data by executing SQL queries
	through an sqlalchemy engine.
	"""
	def __init__(
		self, 
		engine: sqlalchemy.engine.base.Engine,
		sql_directory: Optional[str] = None
	) -> None:
		"""
		Initialize the SQL processor.

		Parameters
		----------
			engine : sqlalchemy.engine.base.Engine
				Valid sqlalchemy engine for connecting to a PostGIS
				database.
			sql_directory : str, optional
				Absolute path where SQL queries are stored.
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
		Execute a query from a string or an external SQL file, 
		formatted with input parameters.

		Parameters
		----------
			mode : str
				Specify whether the query is distributed as a string
				(string mode) or as a file to read (file mode). This
				parameter can take the following values: file, string.
			query : str
			parameters : iterable of str, int, float
				Parameters used for formatting the SQL query.
		
		Raises
		------
			ValueError : If the provided mode is not supported. 
				Supported modes are "file" and "string".
			ValueError : If a query is executed in `file` mode while
				`sql_directory = None`.

		Examples
		--------

		Provide path where SQL queries are stored and create an engine 
		for connecting to a PostgreSQL database.

		>>> sql_directory = "/path/to/sql/directory/"
		>>> engine = sqlalchemy.create_engine(...)

		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Execute a SQL query from an external file in file mode. The
		corresponding file is stored in the `sql_directory`.

		>>> processor.execute_query(
			"file",                          # Mode
			"my_query.sql",                  # Query
			"commune", "INSEE_COM", "97%"    # Parameters
		)
		
		Execute a SQL query from a string in string mode. 
		
		Note: When using string mode exclusively, it is not necessary to
		provide a value for `sql_directory` when creating an instance of
		SqlProcessor.
		
		>>> query = '''
		DELETE FROM public."{0}"
		WHERE "{1}" LIKE '{2}';
		'''
		>>> processor.execute_query(
			"string",                        # Mode
			query,                           # Query
			"commune", "INSEE_COM", "97%"    # Parameters
		)
		"""
		
		if mode.lower() not in ("file", "string"):
			raise ValueError(f"{mode} is not a supported mode!")
		
		if mode.lower() == "file" and self.sql_directory is None:
			raise ValueError(
				"It is not possible to run a query in 'file' mode "
				"without specifying a SQL directory!"
			)

		if mode.lower() == "file":
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
		Get the SRID of a spatial relation.

		Parameters
		----------
			relation : str
				Name of the relation for which to get the SRID.
		
		Returns
		-------
			int
				SRID of the provided relation.

		Examples
		--------
		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Get the SRID from a spatial relation.

		>>> processor.get_srid(cursor, "communes")
		2154
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
		Check whether or not input relations share the same SRID.

		Parameters
		----------
			relation_a : str
				Name of a spatial relation.
			relation_b : str
				Name of a spatial relation.
		
		Returns
		-------
			bool
				Return `True` if both relations share the same SRID,
				`False` otherwise.
		"""
		srid_a = self.get_srid(relation_a)
		srid_b = self.get_srid(relation_b)

		return srid_a == srid_b


	def create_spatial_index(
		self,
		relation: str
	) -> None:
		"""
		Create a spatial index for the input spatial relation.

		Parameters
		----------
			relation : str
				Name of the relation for which to create an index.
		
		Examples
		--------
		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Create a spatial index for the provided relation.

		>>> processor.create_spatial_index("bd_topo")
		"""
		self.execute_query(
			"file",
			join(self.sql_directory, "create_spatial_index.sql"),
			relation
		)


	def singlepart_to_multipart(
		self,
		relation: str,
	) -> None:
		"""
		Convert the singlepart features of an input spatial relation to 
		multipart features.

		Parameters
		----------
			relation : str
				Name of the relation for which to convert singlepart 
				features to multipart features.
		
		Examples
		--------
		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Get the geometry type of a spatial relation.

		>>> processor.get_geometry_types("communes")
		('ST_Polygon',)

		Convert singleipart geometry to multipart geometry.

		>>> processor.single_to_multi_geometry("communes")

		Get the geometry type of the converted spatial relation.

		>>> processor.get_geometry_types("communes")
		('ST_MultiPolygon',)
		"""
		srid = self.get_srid(relation)

		self.execute_query(
			"file",
			join(self.sql_directory, "singlepart_to_multipart.sql"),
			relation, srid
		)


	def get_geometry_types(
		self,
		relation: str
	) -> Iterable[str]:
		"""
		Get distinct geometry types existing in a spatial relation.

		Parameters
		----------
			relation : str
				Name of the relation for which to get geometry types.

		Returns
		-------
			iterable of str
				Iterable storing the distinct geometry types found in a
				spatial relation.
		
		Examples
		--------
		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Get geometry types for features contained in the provided 
		relation.

		>>> processor.get_geometry_types("communes")
		('ST_MultiPolygon',)
		"""
		return self.execute_query(
			"file",
			join(self.sql_directory, "select_distinct_geometry_type.sql"),
			relation
		)	


	def map_geometry_type(
		self,
		relation: str,
		allow_multi: bool = True
	) -> str:
		"""
		Get geometry type in a spatial relation and convert it to a
		generic type.

		Parameters
		----------
			relation : str
				Spatial relation from which to get and convert the 
				geometry type.
			allow_multi : bool, default True
				Whether or not to allow multipart geometries. If `False`
				the multipart geometry type is converted to its
				singlepart equivalent.
		
		Returns
		-------
			str
				Generic geometric, converted from a PostGIS geometry
				type.
		
		Raises
		------
			ValueError : When a spatial relation has more than one
				distinct geometry type.
		
		Examples
		--------
		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Get geometry types for features contained in the provided 
		relation.

		>>> processor.get_geometry_types("communes")
		('ST_MultiPolygon',)

		Convert the PostGIS geometry type to a generic form.

		>>> processor.map_geometry_type("communes")
		"MultiPolygon"

		Convert the PostGIS geometry type to a generic singlepart form.

		>>> processor.map_geometry_type("communes", allow_multi=False)
		"Polygon"
		"""
		# Get geometry type(s) from relation
		geometry_types = self.get_geometry_types(relation)
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
		Project the geometries of an input spatial relation to the 
		provided SRID.

		Parameters
		----------
			relation : str
				Name of the relation for which to get the SRID.
			srid : str or int
				Target SRID.
		
		Examples
		--------
		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Get the SRID of a spatial relation.

		>>> processor.get_srid("communes")
		4326

		Reproject data from EPSG:4326 to EPSG:2154.

		>>> processor.project_geometry("communes", 2154)
		>>> processor.get_srid("communes")
		2154	
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
		Remove the third dimension of all features in a spatial 
		relation.

		Parameters
		----------
			relation : str
				Name of the relation for which to remove the third 
				dimension.
		
		Examples
		--------
		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Remove the third dimension from a spatial relation.

		>>> processor.transform_3d_to_2d("communes")
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
		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Update the geometry of spatial relation `b` after intersecting
		it with spatial relation `a`. In this following example, the
		only information kept from both relations is the resulting
		geometry.

		>>> processor.intersect_geometries("a", "b")

		To create a new relation `c` instead of overwriting `b`, one may
		specify the `out_name` attribute.

		>>> processor.intersect_geometries("a", "b", out_name="c")

		A table is created by default. However, when `out_name` is
		provided, it is possible to set `as_view` to `True` in order to
		create a view instead.

		>>> processor.intersect_geometries(
			"a", "b", as_view=True, out_name="c"
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

		>>> processor.intersect_geometries(
			"a", "b", 
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
				Relation containing geometries to aggregate with 
				`relation_b`.
			relation_b : str
				Relation containing geometries to aggregate with 
				`relation_a`.
			out_name : str
				Name of the relation to create.
			as_view : bool, default False
				Whether or not to create a view. When False, a table is 
				created. When True, a view is created.
			build_index : bool, default True
				Whether or not to build a spatial index for the 
				aggregation result. The option is not available for 
				views.
		
		Examples
		--------
		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Create a new spatial relation `c` from the union of two spatial
		relations `a` and `b`.

		>>> processor.aggregate_relations("a", "b", "c")

		By default, a new table is created. But one might also want to
		create a view. In this case, `as_view` must be set to `True`.

		>>> processor.aggregate_relations("a", "b", "c", as_view=True)
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


	def drop_relations(
		self,
		relation_type: str,
		relations: Iterable[str]
	) -> None:
		"""
		Drop input relations, whether tables or views.

		Parameters
		----------
			relation_type : str
				Specify whether the relations are `TABLE`s or `VIEW`s.
			relations : iterable of str
				Relations to drop.
		
		Raises
		------
			ValueError : The provided `relation_type` is invalid.
				Supported values are `TABLE` and `VIEW`.
			ValueError : A relation could not be dropped. In this case,
				sqlalchemy's error is caught and displayed to the user.

		Examples
		--------
		Create an instance of SqlProcessor.

		>>> processor = SqlProcessor(engine, sql_directory)

		Drop relations.

		>>> processor.drop_relations("TABLE", ["a", "b", "c"])
		"""
		if relation_type.upper() not in ("TABLE", "VIEW"):
			raise ValueError(
				f"{relation_type} is not a valid relation type! "
				"Valid values are 'TABLE' or 'VIEW'."
			)

		for relation in (relations):
			try:
				self.execute_query(
					"file",
					join(self.sql_directory, "drop_relation.sql"), 
					relation_type, relation, "CASCADE"
				)
			except Exception as e:
				print(e)
				raise ValueError(f"{relation} could not be dropped!")
