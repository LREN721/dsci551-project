
-- create
CREATE TABLE METADATA (
  id INTEGER NOT NULL,
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
INSERT INTO METADATA (id, file_type, file_name, has_child) VALUES (1,'DICTECTORY','', TRUE);
INSERT INTO METADATA (id, file_type, file_name, has_child) VALUES (2,'DICTECTORY','user', TRUE);
INSERT INTO METADATA (id, file_type, file_name, has_child) VALUES (3,'DICTECTORY', 'ellachen', TRUE);
INSERT INTO METADATA (id, file_type, file_name, has_child) VALUES (4,'DICTECTORY', 'testdic', False);
INSERT INTO METADATA (id, file_type, file_name, has_child) VALUES (5,'DICTECTORY', '7777777', False);

-- /user/ellachen/testdic
-- 0/ 1  / 2      /  3
-- /7777777
-- 0/ 4
INSERT INTO PARENT_CHILD VALUES(0, '1, 4');
INSERT INTO PARENT_CHILD VALUES(1, '2');
INSERT INTO PARENT_CHILD VALUES(2, '3');
INSERT INTO PARENT_CHILD VALUES(3, '');
-- file only
--INSERT INTO SOURCE VALUES(1, '1,2');
-- fetch 
SELECT * FROM METADATA;
SELECT * FROM PARENT_CHILD;
SELECT * FROM SOURCE;

-- CREATE TABLE METADATA (
--     id INT AUTO_INCREMENT,
--     file_type TEXT NOT NULL,
--     has_child boolean NOT NULL;
--     );
    
-- INSER INTO METADATA VALUES （1， ‘DICTECTORY’，true）

-- SELECT * FROM METADATA;
