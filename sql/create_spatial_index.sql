CREATE INDEX {0}_geom_idx
	ON public."{0}"
	USING GIST (geometry);
