CREATE {0} public."{1}" AS (
	SELECT ROW_NUMBER() OVER () AS id,
	       ST_Multi(ST_Union(geometry))::geometry(MultiPolygon, {2}) AS geometry
	FROM public."{3}"
	WHERE "{4}" LIKE '{5}'
);
