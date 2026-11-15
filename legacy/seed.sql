CREATE TABLE records(row_id TEXT PRIMARY KEY,region TEXT,score INTEGER,note TEXT);
INSERT INTO records VALUES('r1','A',10,'one'),('r2','A',10,'tie'),('r3','A',NULL,'null-score'),('r4',NULL,11,'null-region'),('r5','B',-2,'negative'),('r6','B',NULL,'null-B'),('r7','茶',5,'unicode'),('r8',NULL,NULL,'both-null'),('r9','A',-2,'low'),('r10','',0,'empty');
CREATE INDEX by_region_score ON records(region ASC,score DESC,row_id ASC);
