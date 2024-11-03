Line1 = LOAD 'hdfs:///user/mokshith/episodeIV_dialogues.txt' USING PigStorage('\t') AS (name:chararray,line_count:chararray);
Line2 = LOAD 'hdfs:///user/mokshith/episodeVI_dialogues.txt' USING PigStorage('\t') AS (name:chararray,line_count:chararray);
Line3 = LOAD 'hdfs:///user/mokshith/episodeV_dialogues.txt' USING PigStorage('\t') AS (name:chararray,line_count:chararray);
lines = Union Line1,Line2,Line3;

GroupByLines = GROUP lines BY name;
CountByLines = FOREACH GroupByLines GENERATE $0 AS name,COUNT($1) AS line_count;
orderbydesc = order CountByLines by line_count desc;
STORE orderbydesc INTO 'hdfs:///user/mokshith/pig_project_resultnew/LineOutputnew' USING PigStorage('\t');