CREATE {0} public."{1}" AS (
	SELECT clipped.*
	FROM (
		SELECT ROW_NUMBER() OVER () AS id, 
			{2},
			(ST_Dump(ST_Intersection(a.geometry, b.geometry))).geom AS geometry
		FROM public."{3}" AS a 
			INNER JOIN public."{4}" AS b
			ON ST_Intersects(a.geometry, b.geometry)
	) AS clipped
	WHERE ST_Dimension(clipped.geometry) = {5}
);
