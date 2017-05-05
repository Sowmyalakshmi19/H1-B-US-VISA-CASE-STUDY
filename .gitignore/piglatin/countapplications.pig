
h1b= LOAD '/home/sowmya/Desktop/mapreduce' using  PigStorage('\t')  AS (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage,year, worksite1:chararray,longitute, latitute);

groupbyyear= GROUP h1b by year;

countapp= foreach groupbyyear generate group as year, COUNT(h1b) as headcount;

dump countapp;

