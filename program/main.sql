-- create
CREATE TABLE METADATA (
  id INTEGER AUTO_INCREMENT PRIMARY KEY,
  file_type TEXT NOT NULL,
  file_name TEXT,
  has_child BOOLEAN NOT NULL
);

CREATE TABLE PARENT_CHILD (
  parent_id INTEGER PRIMARY KEY,
  child_id TEXT 
);

CREATE TABLE SOURCE(
  file_id INTEGER PRIMARY KEY,
  location TEXT NOT NULL
);

-- insert
INSERT INTO METADATA (file_type, file_name, has_child) VALUES ('DICTECTORY','', TRUE);
INSERT INTO METADATA (file_type, file_name, has_child) VALUES ('DICTECTORY','user', TRUE);
INSERT INTO METADATA (file_type, file_name, has_child) VALUES ('DICTECTORY', 'ellachen', TRUE);
INSERT INTO METADATA (file_type, file_name, has_child) VALUES ('DICTECTORY', 'testdic', False);
INSERT INTO METADATA (file_type, file_name, has_child) VALUES ('DICTECTORY', '7777777', False);

-- /user/ellachen/testdic
-- 0/ 1  / 2      /  3
-- /7777777
-- 0/ 4
INSERT INTO PARENT_CHILD VALUES(0, '1, 4');
INSERT INTO PARENT_CHILD VALUES(1, '2');
INSERT INTO PARENT_CHILD VALUES(2, '3');
INSERT INTO PARENT_CHILD VALUES(3, '');
-- file only
INSERT INTO SOURCE VALUES(1, '1,2');
-- fetch 
SELECT * FROM METADATA;
SELECT * FROM PARENT_CHILD;
SELECT * FROM SOURCE;

-- SELECT * FROM METADATA WHERE id=1;

