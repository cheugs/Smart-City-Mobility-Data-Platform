-- GEOSPATIAL JOIN 2: for each bus/tram stop, its district and the closest traffic
-- sensor within 300 m (ST_DWithin on geography = metres). This links public
-- transport delays with road congestion around the stop.
DROP TABLE IF EXISTS analytics.stop_context;
CREATE TABLE analytics.stop_context AS
SELECT st.stop_id,
       st.line_id,
       st.stop_sequence,
       st.name          AS stop_name,
       d.district_id,
       d.name           AS district_name,
       near.sensor_id   AS nearest_sensor_id,
       near.distance_m  AS nearest_sensor_m,
       st.geom
FROM city.transit_stops st
LEFT JOIN city.districts d ON ST_Contains(d.geom, st.geom)
LEFT JOIN LATERAL (
    SELECT s.sensor_id,
           round(ST_Distance(s.geom::geography, st.geom::geography)::numeric, 1) AS distance_m
    FROM city.sensors s
    WHERE ST_DWithin(s.geom::geography, st.geom::geography, 300)
    ORDER BY s.geom <-> st.geom
    LIMIT 1
) near ON true;

ALTER TABLE analytics.stop_context ADD PRIMARY KEY (stop_id);
