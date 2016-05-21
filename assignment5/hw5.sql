
#####
# 1.
create table KINGJAMES (freq INT, word STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' stored as textfile;

LOAD DATA INPATH "/user/cloudera/bible_frequency/part­-r­-00000" INTO TABLE KINGJAMES;

select count(*)
from kingjames
where LOWER(word) LIKE "w%" AND freq > 250
AND LENGTH(word) >= 4;

select *
from kingjames
where LOWER(word) LIKE "w%" AND freq > 250
AND LENGTH(word) >= 4;


#####
# 2.

create table SHAKE (freq INT, word STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' stored as textfile;
LOAD DATA INPATH "/user/cloudera/shake_freq/part­-r-00000" INTO TABLE shake;


create table merged (wordKJ STRING, wordS STRING, freqKJ INT, freqS INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' stored as textfile;

INSERT INTO merged
select K.word, S.word, K.freq, S.freq
from shake S FULL OUTER JOIN kingjames K ON S.word = K.word;

select count(*) from merged
where wordKJ is NULL AND wordS is NOT NULL;

select count(*) from merged
where wordKJ is NOT NULL AND wordS is NULL;

#####
# 3.


select count(*) from merged
where wordKJ is NULL AND wordS is NOT NULL;

select count(*) from merged
where wordKJ is NOT NULL AND wordS is NULL;


#####
# 4. 

CREATE TABLE apachelog ( host STRING,
identity STRING,
user STRING,
time STRING,
request STRING,
status STRING,
size STRING,
referer STRING,
agent STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe' WITH SERDEPROPERTIES ( "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (­|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (­|[0­9]*) (­|[0­9]*)(?: ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\"))?", "output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s" )
STORED AS TEXTFILE;

LOAD DATA INPATH "/user/cloudera/apache.access.log" INTO TABLE apachelog;

LOAD DATA INPATH "/user/cloudera/apache.access.2.log" INTO TABLE apachelog;

LOAD DATA INPATH "/user/cloudera/access_log_1.txt" INTO TABLE apachelog;

select count(*)
from apachelog;








