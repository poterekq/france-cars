# -*- coding: utf-8 -*-

# -----------------------------------------------------------------------------
# Author: Quentin Poterek
# Creation date: 08/06/2022
# Version: 1.1
#------------------------------------------------------------------------------


"""
Utilities for processing spatial data with a PostGIS backend.
"""


from dataclasses import dataclass
from os.path import exists
from typing import Iterable, Optional, Union

import geopandas as gpd
import psycopg2


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
					f"'{backend}' is an unsupported backend. "
					"Use either 'psycopg2' or 'sqlalchemy'."
				))


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


def execute_query(
	cursor: psycopg2.extensions.cursor,
	query: str, 
	parameters: Iterable[str] = (),
	mode: str = "file"
) -> None:
	"""
	Execute a query from an external SQL file, formatted with input
	parameters.

	Parameters
	----------
		cursor : psycopg2.extensions.cursor
			Cursor bound to an open PostgreSQL connection.
		path : str
			Either a SQL query or a path to the SQL query file.
		mode : str, default "file"
			Specify whether the query is distributing as a string
			(direct mode) or as a file to read (file mode). This
			parameter can take to following values: file, direct.			
		parameters : iterable of str, default ()
			Parameters used for formatting the SQL query.
	
	Raises
	------
		ValueError: If the provided mode is not supported. Supported
			modes are "file" and "direct".

	Examples
	--------
	Create a cursor from an open PostgreSQL connection.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()

	Execute a SQL query from an external file in file mode.

	>>> execute_query(
		cursor, 
		"./my_query.sql", 
		("commune", "INSEE_COM", "97%")
	)
	
	Execute a SQL query from a string in direct mode.
	
	>>> query = '''
	DELETE FROM public."%s"
	WHERE "%s" LIKE '%s';
	'''
	>>> execute_query(
		cursor, 
		query, 
		("commune", "INSEE_COM", "97%"),
		"direct"
	)
	"""
	
	if mode not in ("file", "direct"):
		raise ValueError(f"{mode} is not a supported mode!")

	if mode == "file":
		with open(query) as f:
			query = f.read()

	cursor.execute(query.format(*parameters))


def create_spatial_index(
	cursor: psycopg2.extensions.cursor, 
	relation: str
) -> None:
	"""
	Create a spatial index for the input spatial relation.

	Parameters
	----------
		cursor : psycopg2.extensions.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to create an index.
	
	Examples
	--------
	Create a cursor from an open PostgreSQL connection.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()

	Create a spatial index for the provided relation.

	>>> create_spatial_index(cursor, "bd_topo")
	"""
	query = f"""
	CREATE INDEX {relation}_geom_idx
	  ON public."{relation}"
	  USING GIST (geometry);
	"""
	
	cursor.execute(query)


def get_geometry_type(
	cursor: psycopg2.extensions.cursor, 
	relation: str
) -> Iterable[str]:
	"""
	Get the geometry types of a spatial relation.

	Parameters
	----------
		cursor : psycopg2.extensions.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to get the geometry type.
	
	Returns
	-------
		iterable of str
			Iterable storing the geometry types of each feature in the 
			relation.
	
	Examples
	--------
	Create a cursor from an open PostgreSQL connection.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()

	Get geometry types for features contained in the provided relation.

	>>> geometry_types = get_geometry_type(cursor, "communes")
	>>> geometry_types
	('ST_MultiPolygon',)
	"""
	query = f"""
	SELECT DISTINCT ST_GeometryType(geometry)
	FROM public."{relation}";
	"""

	cursor.execute(query)
	return cursor.fetchall()[0]


def has_single_geometry_type(
	geometry_type: Iterable[str]
) -> None:
	"""
	Check whether or not the provided tuple contains a single geometry 
	type.

	Parameters
	----------
		geometry_type : iterable of str
			Tuple containing geometry type(s).
	
	Raises
	------
		ValueError: If there is more than one geometry type in the
		provided iterable.
	
	Examples
	--------
	Get geometry types from a spatial relation.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()
	>>> geometry_types = get_geometry_type(cursor, "communes")

	Check whether or not the spatial relation contains features with the
	same geometry type.

	>>> has_single_geometry_type(geometry_types)

	If there is more than one geometry type, Python will raise an error.
	"""
	if len(geometry_type) > 1:
		raise ValueError(
			"Relations with more than a single geometry type"
			"are not allowed!"
		)


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


def get_srid(
	cursor: psycopg2.extensions.cursor, 
	relation: str
) -> Union[str, int]:
	"""
	Get the SRID of a spatial relation.

	Parameters
	----------
		cursor : psycopg2.extensions.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to get the SRID.
	
	Returns
	-------
		str
			SRID of the provided relation.

	Examples
	--------
	Create a cursor from an open PostgreSQL connection.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()

	Get the SRID from a spatial relation.

	>>> get_srid(cursor, "communes")
	2154
	"""
	query = f"""
	SELECT DISTINCT ST_SRID(geometry)
	FROM public."{relation}";
	"""

	cursor.execute(query)
	return cursor.fetchall()[0][0]


def single_to_multi_geometry(
	cursor: psycopg2.extensions.cursor, 
	relation: str
) -> None:
	"""
	Convert the singlepart features of an input spatial relation to 
	multipart features.

	Parameters
	----------
		cursor : psycopg2.extensions.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to convert singlepart 
			features to multipart features.
	
	Examples
	--------
	Create a cursor from an open PostgreSQL connection.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()

	Get the geometry type of a spatial relation.

	>>> get_geometry_type(cursor, "communes")
	('ST_Polygon',)

	Convert singleipart geometry to multipart geometry.

	>>> single_to_multi_geometry(cursor, "communes")

	Get the geometry type of the converted spatial relation.

	>>> get_geometry_type(cursor, "communes")
	('ST_MultiPolygon',)
	"""
	query = f"""
	UPDATE public."{relation}"
	SET geometry = ST_Multi(geometry);
	"""

	cursor.execute(query)


def project_geometry(
	cursor: psycopg2.extensions.cursor, 
	relation: str, 
	srid: Union[str, int]
) -> None:
	"""
	Project the geometries of an input spatial relation to the provided 
	SRID.

	Parameters
	----------
		cursor : psycopg2.extensions.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to get the SRID.
		srid : str or int
			Target SRID.
	
	Examples
	--------
	Create a cursor from an open PostgreSQL connection.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()

	Get the SRID of a spatial relation.

	>>> get_srid(cursor, "communes")
	4326

	Reproject data from EPSG:4326 to EPSG:2154.

	>>> project_geometry(cursor, "communes", 2154)
	>>> get_srid(cursor, "communes")
	2154
	"""
	geometry_types = get_geometry_type(cursor, relation)
	has_single_geometry_type(geometry_types)
	geometry_type = convert_st_to_type(geometry_types[0])

	query = f"""
	ALTER TABLE public."{relation}"
	  ALTER COLUMN geometry
	  TYPE Geometry({geometry_type}, {srid})
	  USING ST_Transform(geometry, {srid});
	"""

	cursor.execute(query)


def transform_3d_to_2d(
	cursor: psycopg2.extensions.cursor, 
	relation: str
) -> None:
	"""
	Remove the third dimension of all features in a spatial relation.

	Parameters
	----------
		cursor : psycopg2.extensions.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to remove the third 
			dimension.
	
	Examples
	--------
	Create a cursor from an open PostgreSQL connection.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()

	Remove the third dimension from a spatial relation.

	>>> transform_3d_to_2d(cursor, "communes")
	"""
	geometry_types = get_geometry_type(cursor, relation)
	has_single_geometry_type(geometry_types)
	geometry_type = convert_st_to_type(geometry_types[0])

	srid = get_srid(cursor, relation)
	
	query = f"""
	ALTER TABLE public."{relation}"
	  ALTER COLUMN geometry
	  TYPE Geometry({geometry_type}Z, {srid})
	  USING ST_Transform(geometry, {srid});
	
	ALTER TABLE public."{relation}"
	  ADD COLUMN geometry2d
	  geometry({geometry_type}, {srid});

	UPDATE public."{relation}"
	SET geometry2d = ST_Force2D(geometry);

	ALTER TABLE public."{relation}"
	  DROP geometry;

	ALTER TABLE public."{relation}"
	  RENAME geometry2d TO geometry;
	"""

	cursor.execute(query)


def intersect_geometries(
	cursor: psycopg2.extensions.cursor, 
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
			Relation containing geometries to intersect with relation_b.
		relation_b : str
			Relation containing geometries to intersect with relation_a.
		fields_a : iterable of str
			Fields of relation_a to pass to the intersection result.
		fields_b : iterable of str
			Fields of relation_b to pass to the intersection result.
		as_view : bool, default False
			Whether or not to create a view. When False, a table is 
			created. When True, a view is created.
		out_name : str, optional
			Name of the relation to create. By default, relation_b is
			replaced with the intersection result.
		build_index : bool, default True
			Whether or not to build a spatial index for the intersection
			result. The option is not available for views.
	
	Examples
	--------
	Create a cursor from an open PostgreSQL connection.

	>>> connection = psycopg2.connect(...)
	>>> cursor = connection.cursor()

	Update the geometry of spatial relation `b` after intersecting it
	with spatial relation `a`. In this following example, the only
	information kept from both relations is the resulting geometry.

	>>> intersect_geometries(cursor, "a", "b")

	To create a new relation `c` instead of overwriting `b`, one may
	specify the `out_name` attribute.

	>>> intersect_geometries(cursor, "a", "b", out_name="c")

	A table is created by default. However, when `out_name` is provided,
	it is possible to set `as_view` to `True` in order to create a view
	instead.

	>>> intersect_geometries(
		cursor, "a", "b", 
		as_view=True, out_name="c"
	)

	In addition to the geometry resulting from the intersection of two
	relations, one can also specify fields to keep. In this case, it is
	preferable to remove the initial `geom` or `geometry` attributes
	from both `fields_a` and `fields_b`. Indeed, other functions or GIS
	softwares might be unsure whether to use the geometry resulting from
	the intersection or that of the initial spatial relations.

	Below, an example where the identifier from `a` and all attributes
	from `b` are kept. Note that the initial geometry of both relations
	was not included.

	>>> intersect_geometries(
		cursor, "a", "b", 
		fields_a=["id"], 
		fields_b=["x", "y", "z"]
	)
	"""
	DIMENSION = {
		"Point": 0,
		"LineString": 1,
		"Polygon": 2,
	}

	fields_a = [f'a."{field}"' for field in fields_a]
	fields_b = [f'b."{field}"' for field in fields_b]
	fields = fields_a + fields_b
	fields = ", ".join(fields)

	geometry_types_b = get_geometry_type(cursor, relation_b)
	has_single_geometry_type(geometry_types_b)
	geometry_type_b = convert_st_to_type(geometry_types_b[0], False)

	relation = f"{relation_b}_tmp" if out_name is None else out_name
	relation_type = "VIEW" if as_view else "TABLE"

	query = f"""
	CREATE {relation_type} public."{relation}" AS (
	  SELECT clipped.*
	  FROM (
		SELECT ROW_NUMBER() OVER () AS id,
		  {fields},
		  (ST_Dump(ST_Intersection(a.geometry, b.geometry))).geom 
		    AS geometry
		FROM public."{relation_a}" AS a 
		  INNER JOIN public."{relation_b}" AS b
		    ON ST_Intersects(a.geometry, b.geometry)
	  ) AS clipped
	  WHERE 
	    ST_Dimension(clipped.geometry) = {DIMENSION[geometry_type_b]}
	);
	"""
	
	cursor.execute(query)

	if out_name is None:
		query = f"""
		DROP TABLE public."{relation_b}";

		ALTER TABLE public."{relation}"
		  RENAME TO "{relation_b}";
		"""
	
		cursor.execute(query)

	if build_index and relation_type == "TABLE":
		if out_name is None:
			create_spatial_index(cursor, relation_b)
		else:
			create_spatial_index(cursor, out_name)


def _intersect_geometries(
	cursor: psycopg2.extensions.cursor, 
	relation_a: str, 
	relation_b: str, 
	fields_a: Iterable[str] = [], 
	fields_b: Iterable[str] = [], 
	out_name: Optional[str] = None, 
	build_index: bool = True
) -> None:
	"""
	This function is deprecated. Use intersect_geometries instead.
	Return a spatial relation representing the intersection of two 
	spatial relations provided as input.

	Parameters
	----------
		cursor : psycopg2.extensions.cursor
			Cursor bound to an open PostgreSQL connection.
		relation_a : str
			Relation containing geometries to intersect with relation_b.
		relation_b : str
			Relation containing geometries to intersect with relation_a.
		fields_a : iterable of str
			Fields of relation_a to pass to the intersection result.
		fields_b : iterable of str
			Fields of relation_b to pass to the intersection result.
		out_name : str, optional
			Name of the relation to create. By default, relation_b is
			replaced with the intersection result.
		build_index : bool, default True
			Whether or not to build a spatial index for the intersection
			result. The option is not available for views.
	"""
	fields_a = [f"a.{field}" for field in fields_a]
	fields_b = [f"b.{field}" for field in fields_b]
	fields = fields_a + fields_b
	fields = ", ".join(fields)

	geometry_types_b = get_geometry_type(cursor, relation_b)
	has_single_geometry_type(geometry_types_b)
	geometry_type_b = convert_st_to_type(geometry_types_b[0], False)

	srid = get_srid(cursor, relation_b)

	table = f"{relation_b}_tmp" if out_name is None else out_name

	query = f"""
	CREATE TABLE public."{table}" AS (
	  SELECT {fields},
		ST_Multi(ST_Intersection(a.geometry, b.geometry))
		  ::geometry(Multi{geometry_type_b}, {srid}) AS geometry
	  FROM public."{relation_a}" AS a, public."{relation_b}" AS b
	);
	"""

	cursor.execute(query)

	if out_name is None:
		query = f"""
		DROP TABLE public."{relation_b}";

		ALTER TABLE public."{table}"
		  RENAME TO "{relation_b}";
		"""
	
		cursor.execute(query)

	if build_index:
		create_spatial_index(cursor, relation_b)


def aggregate_relations(
	cursor: psycopg2.extensions.cursor, 
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
		cursor : psycopg2.extensions.cursor
			Cursor bound to an open PostgreSQL connection.
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
	srid_a = get_srid(cursor, relation_a)
	srid_b = get_srid(cursor, relation_b)

	if srid_a != srid_b:
		raise ValueError(f"Input geometries do not share \
			the same SRID (a: {srid_a}, b: {srid_b})!")
	
	relation_type = "VIEW" if as_view else "TABLE"

	query = f"""
	CREATE {relation_type} public."{out_name}" AS (
	  SELECT ROW_NUMBER() OVER () AS id,
		(ST_Dump(ST_Union(geometry))).geom::geometry(Polygon, {srid_a}) 
		  AS geometry
	  FROM (
		SELECT geometry,
		  ST_ClusterDBSCAN(geometry, 0, 1) OVER () AS _clst
			FROM (
			  SELECT geometry
			  FROM public."{relation_a}"
			  UNION ALL
			  SELECT geometry
			  FROM public."{relation_b}"
			) AS table_union
		) AS geometric_clustering
	  GROUP BY _clst
	);
	"""

	cursor.execute(query)

	if build_index:
		create_spatial_index(cursor, out_name)
