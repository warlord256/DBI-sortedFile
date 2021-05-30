
CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

a42.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o SortedDBFile.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o QueryPlan.o QueryOptimizer.o main.o
	$(CC) -o a42.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o SortedDBFile.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryPlan.o QueryOptimizer.o y.tab.o lex.yy.o main.o -lfl -lpthread
	
gtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o SortedDBFile.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryPlan.o QueryOptimizer.o y.tab.o lex.yy.o UnitTest.o RunTests.o
	$(CC) -o gtest.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o SortedDBFile.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryPlan.o QueryOptimizer.o y.tab.o lex.yy.o UnitTest.o RunTests.o -lfl -lpthread -lgtest

main.o : main.cc
	$(CC) -g -c main.cc 

UnitTest.o : UnitTest.cc
	$(CC) -g -c UnitTest.cc

RunTests.o : RunTests.cc
	$(CC) -g -c RunTests.cc

Schema.o : Schema.cc
	$(CC) -g -c Schema.cc


Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

HeapDBFile.o: HeapDBFile.cc
	$(CC) -g -c HeapDBFile.cc

SortedDBFile.o: SortedDBFile.cc
	$(CC) -g -c SortedDBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

Statistics.o : Statistics.cc
	$(CC) -g -c Statistics.cc

QueryOptimizer.o : QueryOptimizer.cc
	$(CC) -g -c QueryOptimizer.cc

QueryPlan.o : QueryPlan.cc
	$(CC) -g -c QueryPlan.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
