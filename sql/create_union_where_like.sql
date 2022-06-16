CREATE {0} public."{1}" AS (
	SELECT ROW_NUMBER() OVER () AS id,
	       ST_Union(geometry)::geometry(Polygon, {2}) AS geometry
	FROM public."{3}"
	WHERE "{4}" LIKE '{5}'
);
