ALTER TABLE document
      ADD COLUMN intrinsic_time tstzmultirange; 

ALTER TABLE planning_assignment
      ADD COLUMN timezone text,
      ADD COLUMN timerange tstzrange;

---- create above / drop below ----

ALTER TABLE planning_assignment
      DROP COLUMN timezone,
      DROP COLUMN timerange;

ALTER TABLE document
      DROP COLUMN intrinsic_time;

