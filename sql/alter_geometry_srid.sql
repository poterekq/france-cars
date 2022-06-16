ALTER TABLE public."{0}"
	ALTER COLUMN geometry
	TYPE Geometry({1}, {2})
	USING ST_Transform(geometry, {2});
