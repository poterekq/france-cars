ALTER TABLE public."{0}"
	ALTER COLUMN geometry
	TYPE Geometry({1}Z, {2})
	USING ST_Transform(geometry, {2});
	
ALTER TABLE public."{0}"
	ADD COLUMN geometry2d
	geometry({1}, {2});

UPDATE public."{0}"
SET geometry2d = ST_Force2D(geometry);

ALTER TABLE public."{0}"
	DROP geometry;

ALTER TABLE public."{0}"
	RENAME geometry2d TO geometry;
