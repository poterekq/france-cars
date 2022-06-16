SELECT "{0}"."{1}" AS "insee_com",
       ST_Area("{0}".geometry) * 1e-6 AS "aire_commune",
       csurface."aire_voiture",
	   csurface."perimetre_voiture",
       clineaire."longueur_voiture",
	   urbain."aire_urbain",
	   usurface."aire_voiture_urbain",
	   usurface."perimetre_voiture_urbain",
	   ulineaire."longueur_voiture_urbain"
FROM "{0}"
	FULL OUTER JOIN
	(
		SELECT "{1}",
			SUM(ST_Area(geometry)) * 1e-6 AS "aire_voiture",
			SUM(ST_Perimeter(geometry)) * 1e-3 AS "perimetre_voiture"
		FROM "{2}"
		WHERE "{1}" LIKE '{3}%'
		GROUP BY "{1}"
	) AS csurface
	ON "{0}"."{1}" = csurface."{1}"
	FULL OUTER JOIN (
		SELECT "{1}",
			SUM(ST_Length(geometry)) * 1e-3 AS "longueur_voiture"
		FROM "{4}"
		WHERE "{1}" LIKE '{3}%'
		GROUP BY "{1}"
	) AS clineaire
	ON "{0}"."{1}" = clineaire."{1}"
	FULL OUTER JOIN (
		SELECT "{1}",
		       SUM(ST_Area(geometry)) * 1e-6 AS "aire_urbain"
		FROM "{5}"
		WHERE "{1}" LIKE '{3}%'
		GROUP BY "{1}"
	) AS urbain
	ON "{0}"."{1}" = urbain."{1}"
	FULL OUTER JOIN
	(
		SELECT "{1}",
			SUM(ST_Area(geometry)) * 1e-6 AS "aire_voiture_urbain",
			SUM(ST_Perimeter(geometry)) * 1e-3 AS "perimetre_voiture_urbain"
		FROM "{6}"
		WHERE "{1}" LIKE '{3}%'
		GROUP BY "{1}"
	) AS usurface
	ON "{0}"."{1}" = usurface."{1}"
	FULL OUTER JOIN (
		SELECT "{1}",
		       SUM(ST_Length(geometry)) * 1e-3 AS "longueur_voiture_urbain"
		FROM "{7}"
		WHERE "{1}" LIKE '{3}%'
		GROUP BY "{1}"
	) AS ulineaire
	ON "{0}"."{1}" = ulineaire."{1}"
WHERE "{0}"."{1}" LIKE '{3}%';