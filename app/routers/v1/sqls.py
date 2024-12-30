series_sql = """
WITH series_base AS (
    SELECT *
    FROM series
    WHERE is_deleted=0
    AND schedule_end_time > UNIX_TIMESTAMP(NOW())
    AND series_id IN ({})
    ),
    
    ccs_sri_id_relation AS (
        SELECT DISTINCT ccs_series_id
        ,series_id
        FROM ccs_ott_series_relation
        WHERE series_id IN ({})
    ),
    
    -- actor
    actor_id_to_series_id_relation AS 
        (
            SELECT
            series_id
            ,tag_actor_id
            FROM series_actor_relation sar
            WHERE series_id IN ({})
        ),  

    actor_id_to_name AS
        (
            SELECT
            tag_actor_id
            ,name
            FROM tag_actor ta
            WHERE IS_DELETED=0
        ),
        
     ccs_series_id_to_actor_names AS 
     	(
		SELECT  ccs_series_id, GROUP_CONCAT(DISTINCT LOWER(name)) AS actor_names 
			FROM actor_id_to_series_id_relation
			INNER JOIN actor_id_to_name  
			ON actor_id_to_name.tag_actor_id = actor_id_to_series_id_relation.tag_actor_id
			INNER JOIN ccs_sri_id_relation
			ON ccs_sri_id_relation.series_id=actor_id_to_series_id_relation.series_id
		GROUP BY ccs_series_id
		),
		
	-- keywords: may contain duplicated values
	ccs_series_id_to_keyword AS (
		SELECT ccs_series_id,
		GROUP_CONCAT(DISTINCT LOWER(keyword)) AS keyword
		FROM series_base
		INNER JOIN ccs_sri_id_relation
		ON series_base.series_id=ccs_sri_id_relation.series_id
		GROUP BY ccs_series_id
	),
	
    -- alternative names
    ccs_series_id_to_alternative_names AS (
    	SELECT ccs_series_id,GROUP_CONCAT(DISTINCT LOWER(name))  AS alternative_names
        FROM series_base
        INNER JOIN ccs_sri_id_relation
        ON series_base.series_id=ccs_sri_id_relation.series_id
        GROUP BY ccs_series_id
    ),
    
    series_tag_relation AS (
    	SELECT series_id,
    	tag_id
    	FROM series_tag_relation
        WHERE series_id IN ({})
    ),

    tag_id_name AS (
        SELECT tag_id,
        LOWER(name) as tag_name
        FROM tag
        WHERE is_deleted =0
        AND name!='undefined'
        ),
    
    series_tag_names AS (
        SELECT series_id,
        GROUP_CONCAT(DISTINCT LOWER(tag_name)) AS tag_names
        FROM series_tag_relation
        INNER JOIN tag_id_name
        ON tag_id_name.tag_id = series_tag_relation.tag_id
        GROUP BY series_id
        )
    
SELECT

series_base.series_id AS _id,
series_base.series_id,
series_base.name,

series_base.cover_image_uri,
series_base.landscape_image,
series_base.portrait_image,

series_base.product_total,
series_base.released_product_total,

series_base.is_movie,
series_base.source_flag,
series_base.allow_tv,
series_base.allow_telstb,
series_base.description,
series_base.cover_image_uri,
series_base.landscape_image,
series_base.portrait_image,
series_base.release_time,
series_base.schedule_start_time,
series_base.schedule_end_time,
series_base.is_deleted,
series_base.country_ids,
series_base.area_id,
series_base.language_flag_id,
series_base.last_modified_time,
series_base.poster_logo,

LOWER(series_base.name) AS series_name_lower,
ccs_series_id_to_keyword.keyword,
ccs_sri_id_relation.ccs_series_id,
ccs_series_id_to_actor_names.actor_names,
ccs_series_id_to_alternative_names.alternative_names,
series_tag_names.tag_names


FROM series_base

LEFT JOIN ccs_sri_id_relation
ON series_base.series_id = ccs_sri_id_relation.series_id

LEFT JOIN ccs_series_id_to_keyword
ON ccs_sri_id_relation.ccs_series_id=ccs_series_id_to_keyword.ccs_series_id

LEFT JOIN ccs_series_id_to_actor_names
ON ccs_sri_id_relation.ccs_series_id=ccs_series_id_to_actor_names.ccs_series_id

LEFT JOIN ccs_series_id_to_alternative_names
ON ccs_sri_id_relation.ccs_series_id = ccs_series_id_to_alternative_names.ccs_series_id

LEFT JOIN series_tag_names
ON series_base.series_id = series_tag_names.series_id
"""


product_sql ="""
WITH product_base AS (
    SELECT *
    FROM product
    WHERE is_deleted = 0
    AND schedule_end_time > UNIX_TIMESTAMP(NOW())
    AND product_id in ({})
    ),
    product_tag_relation AS (
    SELECT product_id, tag_id
    FROM product_tag_relation
    WHERE product_id in ({})
    ),
    guest_tag_id_to_name AS (
    SELECT tag_guest_id AS tag_id, LOWER(name) AS guest_tag_name
    FROM tag_guest
    ),
    product_id_to_guest_tags AS (
    SELECT product_id, GROUP_CONCAT(LOWER(guest_tag_name)) AS guest_tag_names
    FROM product_tag_relation
    INNER JOIN guest_tag_id_to_name ON product_tag_relation.tag_id = guest_tag_id_to_name.tag_id
    WHERE product_id in ({})
    GROUP BY product_id
    )

SELECT 
    pb.product_id AS _id, 
    pb.product_id,
    pb.series_id,
    pb.number,
    pb.synopsis,
    pb.description,
    pb.cover_image_uri,
    pb.time_duration,
    pb.schedule_start_time,
    pb.schedule_end_time,
    pb.free_time,
    pb.premium_time,
    pb.is_free_premium_time,
    pb.allow_download,
    LOWER(pb.keyword) AS keyword,
    pb.is_produced,
    pb.is_deleted,
    pb.is_parental_lock_limited,
    pb.is_parental_lock_compulsory,
    pb.last_modified_time,
    pb.area_id,
    pb.language_flag_id,
    pb.censorship_ads_id,
    pb.allow_play_big_screen,
    pb.play_big_screen_start_time,
    pb.play_big_screen_end_time,
    pb.duration_start,
    pb.source_flag,
    pb.third_product_id,
    pb.seo_title,
    pb.seo_description,
    pb.chargingcp_id,
    pb.classification,
    pb.encryption_string,
    pb.multiple_image,
    pb.landscape_image,
    pb.portrait_image,
    pb.skip_intro_start_time,
    pb.skip_intro_end_time,
    pb.content_advisory,
    pb.drm,
    pb.dpr,
    pigt.guest_tag_names
FROM product_base pb
    LEFT JOIN product_id_to_guest_tags pigt ON pb.product_id = pigt.product_id
WHERE pb.product_id in ({})
"""

product_id_sql="""
SELECT product_id FROM product WHERE product_id > %s and is_deleted = 0
    AND schedule_end_time > UNIX_TIMESTAMP(NOW()) ORDER BY product_id ASC LIMIT %s
"""