CREATE {0} "{1}" AS (
	SELECT ROW_NUMBER() OVER () AS id,
	       ST_Union(ST_Multi(geometry))::geometry(MultiPolygon, {2}) AS geometry
	FROM public."{3}"
);