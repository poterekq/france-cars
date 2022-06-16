CREATE {0} public."{1}" AS (
SELECT ROW_NUMBER() OVER () AS id,
	(ST_Dump(ST_Union(geometry))).geom::geometry(Polygon, {2}) 
	AS geometry
FROM (
	SELECT geometry,
	ST_ClusterDBSCAN(geometry, 0, 1) OVER () AS _clst
		FROM (
		SELECT geometry
		FROM public."{3}"
		UNION ALL
		SELECT geometry
		FROM public."{4}"
		) AS table_union
	) AS geometric_clustering
GROUP BY _clst
);