# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------
# Author: Quentin Poterek
# Creation date: 08/06/2022
# Version: 1.0
#-----------------------------------------------------------------------

"""
Utilities for processing spatial data with PostGIS backend.
"""


from os.path import exists
from typing import Iterable, Optional, Union

import geopandas as gpd
import psycopg2


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
		    
	"""
	return gdf[gdf[attribute].isin(values)]


def subset_columns(
	gdf: gpd.GeoDataFrame, 
	src_columns: Iterable[str], 
	tgt_columns: Optional[Iterable[str]] = None
) -> gpd.GeoDataFrame:
	"""
	
	Parameters
	----------
		gdf : GeoDataFrame
			GeoPandas dataframe to subset.
		src_columns : iterable of str
			Original names of the columns to be extracted.
		tgt_columns : iterable of str, optional
			Replacement names for those provided in src_columns.
			They should be in the same order as that of src_columns.
	
	Returns
	-------
		GeoDataFrame
			GeoPandas dataframe with the selected columns.
	"""
	gdf = gdf[src_columns]

	if tgt_columns is not None:
		gdf.columns = tgt_columns
	
	return gdf


def check_files(
	*args: str
) -> None:
	"""
	Check whether or not the files exist on the system.

	Parameters
	----------
		args : str or iterable of str
			Files to be looked for on the system.
	"""
	for f in args:
		if not exists(f):
			raise FileNotFoundError(f"'{f}' does not exist!")


def create_spatial_index(
	cursor: psycopg2.cursor, 
	relation: str
) -> None:
	"""
	Create a spatial index for the input spatial relation.

	Parameters
	----------
		cursor : psycopg2.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to create an index.
	"""
	query = f"""
	CREATE INDEX {relation}_geom_idx
	  ON public."{relation}"
	  USING GIST (geometry);
	"""
	
	cursor.execute(query)


def get_geometry_type(
	cursor: psycopg2.cursor, 
	relation: str
) -> Iterable[str]:
	"""
	Get the geometry types of a spatial relation.

	Parameters
	----------
		cursor : psycopg2.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to get the geometry type.
	
	Returns
	-------
		iterable of str
			Iterable storing the geometry types of each feature in the 
			relation.
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

	"""
	if len(geometry_type) > 1:
		raise ValueError("Relations with more than a single geometry \
		type are not allowed!")


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
	"""
	if geometry[:3] != "ST_":
		raise ValueError("Input geometry type is not compatible with \
			PostGIS!")
	geometry = geometry[3:]
	if (geometry[:5] == "Multi" and not allow_multi):
		return geometry[5:]
	return geometry


def get_srid(
	cursor: psycopg2.cursor, 
	relation: str
) -> Union[str, int]:
	"""
	Get the SRID of a spatial relation.

	Parameters
	----------
		cursor : psycopg2.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to get the SRID.
	
	Returns
	-------
		str
			SRID of the provided relation.
	"""
	query = f"""
	SELECT DISTINCT ST_SRID(geometry)
	FROM public."{relation}";
	"""

	cursor.execute(query)
	return cursor.fetchall()[0][0]


def single_to_multi_geometry(
	cursor: psycopg2.cursor, 
	relation: str
) -> None:
	"""
	Convert the singlepart features of an input spatial relation to 
	multipart features.

	Parameters
	----------
		cursor : psycopg2.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to convert singlepart 
			features to multipart features.
	"""
	query = f"""
	UPDATE public."{relation}"
	SET geometry = ST_Multi(geometry);
	"""

	cursor.execute(query)


def project_geometry(
	cursor: psycopg2.cursor, 
	relation: str, 
	srid: Union[str, int]
) -> None:
	"""
	Project the geometries of an input spatial relation to the provided 
	SRID.

	Parameters
	----------
		cursor : psycopg2.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to get the SRID.
		srid : str or int
			Target SRID.
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
	cursor: psycopg2.cursor, 
	relation: str
) -> None:
	"""
	Remove the third dimension of all features in a spatial relation.

	Parameters
	----------
		cursor : psycopg2.cursor
			Cursor bound to an open PostgreSQL connection.
		relation : str
			Name of the relation for which to remove the third 
			dimension.
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
	cursor: psycopg2.cursor, 
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
		cursor : psycopg2.cursor
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
	cursor: psycopg2.cursor, 
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
		cursor : psycopg2.cursor
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
	cursor: psycopg2.cursor, 
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
		cursor : psycopg2.cursor
			Cursor bound to an open PostgreSQL connection.
		relation_a : str
			Relation containing geometries to intersect with relation_b.
		relation_b : str
			Relation containing geometries to intersect with relation_a.
		out_name : str, optional
			Name of the relation to create. By default, relation_b is
			replaced with the intersection result.
		as_view : bool, default False
			Whether or not to create a view. When False, a table is 
			created. When True, a view is created.
		build_index : bool, default True
			Whether or not to build a spatial index for the intersection
			result. The option is not available for views.
	"""
	srid_a = get_srid(cursor, relation_a)
	srid_b = get_srid(cursor, relation_b)

	if srid_a != srid_b:
		raise ValueError(f"Input geometries do not share \
			the same SRID ({srid_a}, {srid_b})!")
	
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
