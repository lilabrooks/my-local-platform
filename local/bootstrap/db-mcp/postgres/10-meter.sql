\getenv reader_pass METER_READER
CREATE TABLE meter_events AS
SELECT n AS sequence_no, 'station-' || (n % 4) AS station,
       CASE WHEN n % 2 = 0 THEN 'ready' ELSE 'charging' END AS state,
       n * 10 AS watts, 'synthetic maintenance note' AS service_note
FROM generate_series(1, 64) AS n;
CREATE VIEW readings AS SELECT sequence_no, station, state, watts FROM meter_events;
CREATE USER meter_reader PASSWORD :'reader_pass';
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE TEMPORARY ON DATABASE meter_demo FROM PUBLIC;
GRANT CONNECT ON DATABASE meter_demo TO meter_reader;
GRANT USAGE ON SCHEMA public TO meter_reader;
GRANT SELECT ON readings TO meter_reader;
