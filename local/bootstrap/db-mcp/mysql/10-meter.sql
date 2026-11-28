CREATE TABLE meter_events (sequence_no INT PRIMARY KEY, station VARCHAR(24), state VARCHAR(24),
                           watts INT, service_note VARCHAR(50));
INSERT INTO meter_events
WITH RECURSIVE series(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM series WHERE n < 64)
SELECT n, CONCAT('station-', MOD(n, 4)), IF(MOD(n, 2) = 0, 'ready', 'charging'),
       n * 10, 'synthetic maintenance note' FROM series;
CREATE DATABASE meter_private;
CREATE TABLE meter_private.calibration (revision INT);
INSERT INTO meter_private.calibration VALUES (1);
CREATE VIEW readings AS SELECT sequence_no, station, state, watts FROM meter_events;
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'meter_reader'@'%';
GRANT SELECT ON meter_demo.readings TO 'meter_reader'@'%';
