
h1b= LOAD '/home/sowmya/Desktop/mapreduce' using  PigStorage('\t')  AS (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage,year, worksite1:chararray,longitute, latitute);

filteration= filter h1b by job_title == 'DATA SCIENTIST';

groupbyemployer= group filteration by employer_name;

countbypos= foreach groupbyemployer generate $0, COUNT(filteration) as headcount;
     
dump countbypos;
