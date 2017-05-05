sqoop
------
hadoop fs -put /home/sowmya/visa.txt /niit

create table h1b_app(
     success_rate FLOAT NOT NULL,
      position VARCHAR(40) NOT NULL,
      status VARCHAR(40) NOT NULL,
PRIMARY KEY(success_rate));

sqoop export --connect jdbc:mysql://localhost/app --username root -P --table h1b_app --update-mode allowinsert --update-key success_rate --export-dir /niit/visa.txt --input-fields-terminated-by ','; 

