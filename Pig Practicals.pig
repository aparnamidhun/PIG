
start pig local
----------------------------
pig -x local

start pig HDFS mode
------------------------
pig or -x mapreduce

steps to load data
---------------------------------------
1.load the raw data in pig environment.
2.filter the data.
3.run the mapper on the filter data.
4.reducer on mapper output.
5.store result on fs.

tuple is one record of data

To calculate the count of customers for each profession
--------------------------------------------------------- 
cust= LOAD  '/home/hduser/niit/custs' using PigStorage(',') AS ( custid, firstname, lastname,age:long,profession);
groupbyprofession = group cust by $4;
describe groupbyprofession;
countbyprofession = FOREACH groupbyprofession GENERATE group as profession,COUNT (cust) as headcount;
describe countbyprofession
countbyprofession = order countbyprofession by $1 desc;
dump countbyprofession;
store countbyprofession into '/home/hduser/niit/niit/countbyprofession';



top 10 buyers
----------------
load sales/txns data.
group the data on custid
sum the amount for all the tupples for each custmer]
order desc
limit 10;
join this data with cust bag.


txn= LOAD '/home/hduser/niit/txns1.txt' USING PigStorage(',') AS (txnno:int,txndate:chararray,custno:int,amount:double,category:chararray,product:chararray,city:chararray,state:chararray,type:chararray);
 describe txn;
dump txn
groupbycustid = group txn by $2;
describe groupbycustid;
dump groupbycustid;
spentbycust = FOREACH groupbycustid GENERATE group as customer_id,ROUND_TO(SUM (txn.$3 ),2) as totalsales;
describe spentbycust;
spentbycust = order spentbycust by $1 desc;
top10cust = limit spentbycust 10;
dump top10cust;
top10join = join top10cust by $0,cust by $0;
dump top10join;
top10 = foreach top10join generate $0,$3,$4,$5,$6,$1;
dump top10;
top10order = order top10 by $5 desc;
dump top10order
store top10order into '/home/hduser/niit/niit/top10order';



cash and credit %
-----------------------

cash   total sales by cash          % on total sales
credit  total salesby creditcard     % on total sales 


load the txn data or make sure data available using dump.
group on type
sum(sales)   total paymentby type.
group all to find total sales;
run reducer
for each command to find %;



txn= LOAD '/home/hduser/niit/txns1.txt' USING PigStorage(',') AS (txnno:int,txndate:chararray,custno:int,amount:double,category:chararray,product:chararray,city:chararray,state:chararray,type:chararray);
or
dump txn;
groupbytype = group txn by $8;
describe groupbytype
dump groupbytype;
totalpaymentbytype = foreach groupbytype generate group as type,ROUND_TO(SUM (txn.$3 ),2) as totalsales; 
dump totalpaymentbytype;
groupall = group totalpaymentbytype all ;
describe groupall;
totalsale = foreach groupall generate ROUND_TO(SUM (totalpaymentbytype.$1 ),2) as totalsales;
dump totalsale
percentage_step1= foreach totalpaymentbytype generate $0,$1,totalsales.$0 as percentage;
percentage_final= foreach percentage_step1 generate $0,(($1*100)/$2);





book author
--------------------


  book = LOAD  '/home/hduser/Downloads/book-data.' using PigStorage(',') AS ( book_id, price:int, id);

 book_info_filter = FILTER book BY $1>=200;
 author = LOAD  '/home/hduser/Downloads/author-data.' using PigStorage(',') AS ( id:int, name:chararray );
 author_info_filter = FILTER author BY INDEXOF($1,'J',0) == 0;
joinbyauthorid = join book by $2,author by $0;
  joinandtabled = foreach joinbyauthorid generate $0,$1,$3;
  joinandtabled = foreach joinbyauthorid generate $1,$3;
  autherbookprice = order joinandtabled by $1 desc;
  store autherbookprice into '/home/hduser/niit/niit/autherbookprice';



analyse retail trade report
---------------------------------------
 



data2000 = LOAD  '/home/hduser/Downloads/2000.txt' using PigStorage(',') AS ( id:int, product, janprice:double,febprice:double,marprice:double,aprprice:double,mayprice:double,junprice:double,julprice:double,augprice:double,septprice:double,octprice:double,novprice:double,decprice:double);

data2001 = LOAD  '/home/hduser/Downloads/2001.txt' using PigStorage(',') AS ( id:int, product, janprice:double,febprice:double,marprice:double,aprprice:double,mayprice:double,junprice:double,julprice:double,augprice:double,septprice:double,octprice:double,novprice:double,decprice:double);

data2002 = LOAD  '/home/hduser/Downloads/2002.txt' using PigStorage(',') AS ( id:int, product, janprice:double,febprice:double,marprice:double,aprprice:double,mayprice:double,junprice:double,julprice:double,augprice:double,septprice:double,octprice:double,novprice:double,decprice:double);


sumdata2000 = foreach data2000 generate $0,$1,($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13) as totalsale;

sumdata2001 = foreach data2001 generate $0,$1,($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13) as totalsale;

sumdata2002 = foreach data2002 generate $0,$1,($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13) as totalsale;

joinsumdata = JOIN sumdata2000 by $0,sumdata2001 by $0,sumdata2002 by $0;

joindata = foreach joinsumdata generate $0,$1,$2,$5,$8;

 per1 = foreach joindata generate $0,$1,$2,$3,$4,($3-$2) as p1,($4-$3) as p2;
 per2 = foreach per1 generate $0,$1,($5*100)/$2 as p1,($6*100)/$3 as p2;
 per3 = foreach per2 generate $0,$1,ROUND_TO(($2+$3)/2,2) as avgpercent;
 finalpercentage = order per3 by $2 desc;


Analyze the entire data set and arrive at products that have experienced a consolidated yearly avg growth of 10% or more in sales since 2000.
----------------------------------------------------------------------------------------------------------------------------------------------

groath_morethan_10_percent = filter finalpercentage by $2 >= 10;

 store groath_morethan_10_percent into '/home/hduser/niit/niit/groath_morethan_10_percent';
 


 Analyze the entire data set and arrive at products that have experienced a consolidated yearly avg drop of 5% or more since 2000.
-----------------------------------------------------------------------------------------------------------------------------------

drop_morethan_5_percent = filter finalpercentage by $2 <= -5;

 
  

Find top 5 products and bottom 5 products of overall sales for 3 years.
------------------------------------------------------------------------
 overallsales = foreach joindata generate $0,$1,($2+$3+$4) as overalsales ;
top5 = order overallsales by $2 desc;
 top5sales = limit top5 5;
store top5sales into '/home/hduser/niit/niit/top5';
 bottom5 = order overallsales by $2 asc;
 bottom5sales = limit bottom5 5;


 store bottom5sales into '/home/hduser/niit/niit/bottom5';


monthly analising
---------------------------

groupall2000 = group data2000 all;
groupall2001 = group data2001 all;
groupall2002 = group data2002 all;

monthly2000 = foreach groupall2000 generate SUM(data2000.$2),SUM(data2000.$3),SUM(data2000.$4),SUM(data2000.$5),SUM(data2000.$6),SUM(data2000.$7),SUM(data2000.$8),SUM(data2000.$9),SUM(data2000.$10),SUM(data2000.$11),SUM(data2000.$12),SUM(data2000.$13);

monthly2001 = foreach groupall2001 generate SUM(data2001.$2),SUM(data2001.$3),SUM(data2001.$4),SUM(data2001.$5),SUM(data2001.$6),SUM(data2001.$7),SUM(data2001.$8),SUM(data2001.$9),SUM(data2001.$10),SUM(data2001.$11),SUM(data2001.$12),SUM(data2001.$13);

monthly2002 = foreach groupall2002 generate SUM(data2002.$2),SUM(data2002.$3),SUM(data2002.$4),SUM(data2002.$5),SUM(data2002.$6),SUM(data2002.$7),SUM(data2002.$8),SUM(data2002.$9),SUM(data2002.$10),SUM(data2002.$11),SUM(data2002.$12),SUM(data2002.$13);

monthly_2000 = foreach monthly2000 generate FLATTEN(TOBAG(*));

monthly_2001 = foreach monthly2001 generate FLATTEN(TOBAG(*));

monthly_2002 = foreach monthly2002 generate FLATTEN(TOBAG(*));

rank2000 = rank monthly_2000;
rank2001 = rank monthly_2001;
rank2002 = rank monthly_2002;
joinbymonths = join rank2000 by $0,rank2001 by $0,rank2002 by $0;
joinbymonths = foreach joinbymonths generate $0,$1,$3,$5;
per1 = foreach joinbymonths generate $0,$1,$2,$3,$2-$1 as p1,$3-$2 as p2;
per2 = foreach per1 generate $0,ROUND_TO(($4*100)/$1,2) as p1,ROUND_TO(($5*100)/$2,2) as p2;
per3 = foreach per2 generate $0,ROUND_TO(($1+$2)/2,2) as avgper;

dump per3;







WORD COUNT
-------------------------

file = LOAD '/home/hduser/niit/student1' using PigStorage() as(word:chararray);
 token = foreach file generate FLATTEN(TOKENIZE(word)) as word;
 groupbyword = group token by word;
 count = foreach groupbyword generate group,COUNT(token);
 count = order count by $1 desc;



medicaldata
---------------------
average claim
-------------

data = load '/home/hduser/Downloads/medical' using PigStorage() as (name:chararray,dept:chararray,claim:float);

groupdata = group data by $0;

dump groupdata;

describe groupdata;

countdata = foreach groupdata generate $0,$1,SUM(data.$2) as totalclaim,COUNT(data.$2);


avgdata = foreach countdata generate $0,$1,$2/$3 as avgclaim;


reliable
----------
usrdata = load '/home/hduser/Downloads/weblog' using PigStorage() as (name:chararray,bank:chararray,logtime:float);
bankdata = load '/home/hduser/Downloads/gateway' using PigStorage() as (bank:chararray,sucrate:float);
joindata = join usrdata by $1,bankdata by $0;
data1 = foreach joindata generate $0,$1,$4;
groupdata1 = group data1 by $0;
avg = foreach groupdata1 generate $0,AVG(data1.$2);
dump avg;
final = filter avg by $1 >90;
dump final;



to run different files with pig code
=---------------------------------------
pig -x local -p input=/home/hduser/book1.txt -f wordcount.pig;
pig -x local -p input=/home/hduser/book1.txt -p myword=is -f wordcount.pig;


book problem
-----------------
book1 = load '/home/hduser/book1.txt' using PigStorage() as (lines:chararray);
book2 = load '/home/hduser/book2.txt' using PigStorage() as (lines:chararray);
bookcombined = union book1,book2;
--dump bookcombined
split bookcombined into book3 if SUBSTRING(lines,5,7) == 'is',book4 if SUBSTRING (lines,19,24) == 'three',book5 if SUBSTRING (lines,0,4) == 'this';
--dump book3
--dump book4
--dump book5



















