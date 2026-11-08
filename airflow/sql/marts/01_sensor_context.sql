-- GEOSPATIAL JOIN 1: which road does each sensor measure, and in which district is it?
--   * nearest road segment: KNN search with the <-> operator (uses the GiST index)
--   * snap distance in metres: geography cast = real distance on the Earth
--   * district: ST_Contains(polygon, point)
DROP TABLE IF EXISTS analytics.sensor_context;
CREATE TABLE analytics.sensor_context AS
SELECT s.sensor_id,
       road.segment_id,
       road.name            AS road_name,
       road.road_class,
       road.speed_limit_kmh,
       round(ST_Distance(s.geom::geography, road.geom::geography)::numeric, 1) AS snap_distance_m,
       d.district_id,
       d.name               AS district_name,
       s.geom
FROM city.sensors s
CROSS JOIN LATERAL (
    SELECT r.segment_id, r.name, r.road_class, r.speed_limit_kmh, r.geom
    FROM city.road_segments r
    ORDER BY r.geom <-> s.geom
    LIMIT 1
) road
LEFT JOIN city.districts d ON ST_Contains(d.geom, s.geom);

ALTER TABLE analytics.sensor_context ADD PRIMARY KEY (sensor_id);
