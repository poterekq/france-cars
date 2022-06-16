CREATE {0} public.{1} AS (
	SELECT ROW_NUMBER() OVER () AS id, 
	       ST_Union(ST_Multi(ST_Buffer(geometry, {2})))::geometry(MultiPolygon, {3}) AS geometry
	FROM public."{4}"
);