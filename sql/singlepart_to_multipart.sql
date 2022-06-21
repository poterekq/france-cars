ALTER TABLE public."{0}"
	ALTER COLUMN geometry type geometry (Geometry, {1});
UPDATE public."{0}"
SET geometry = ST_Multi(geometry);
