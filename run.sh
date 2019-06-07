./2018201034.sh "select max(A) from table1;" > 1
./2018201034.sh "select min(B) from table2;" > 2
./2018201034.sh "select avg(C) from table1;" > 3
./2018201034.sh "select sum(D) from table2;" > 4
./2018201034.sh "select A,D from table1,table2;" > 5
./2018201034.sh "select distinct C from table1;" > 6
./2018201034.sh "select B,C from table1 where A=-900;" > 7
./2018201034.sh "select A,B from table1 where A=775 OR B=803;" > 8
./2018201034.sh "select * from table1,table2;" > 9
./2018201034.sh "select * from table1,table2 where table1.B=table2.B;" > 10
./2018201034.sh "select A,D from table1,table2 where table1.B=table2.B;" > 11
./2018201034.sh "select table1.C from table1,table2 where table1.A<table2.B;" > 12
./2018201034.sh "select A from table4;" > 13
./2018201034.sh "select Z from table1;" > 14
./2018201034.sh "select B from table1,table2;" > 15
./2018201034.sh "select distinct A,B from table1;" > 16
./2018201034.sh "select table1.C from table1,table2 where table1.A<table2.D OR table1.A>table2.B;" > 17
./2018201034.sh "select table1.C from table1,table2 where table1.A=table2.D;" > 18
./2018201034.sh "select table1.A from table1,table2 where table1.A<table2.B AND table1.A>table2.D;" > 19
./2018201034.sh "select sum(table1.A) from table1,table2;" > 20
