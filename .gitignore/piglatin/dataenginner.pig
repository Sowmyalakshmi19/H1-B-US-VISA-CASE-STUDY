h1b= LOAD '/home/sowmya/Desktop/mapreduce' using  PigStorage('\t')  AS (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage,year, worksite1:chararray,longitute, latitute);

filter_bag= filter h1b by job_title== 'DATA ENGINEER';

group_all= group filter_bag by year;

count_all= foreach group_all generate $0, COUNT(filter_bag) as headcount;
