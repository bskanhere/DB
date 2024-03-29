CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o main.o -lfl

test1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o GenericDBFile.o HeapDBFile.o SortedDBFile.o y.tab.o lex.yy.o test1.o
	$(CC) -o test1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o GenericDBFile.o HeapDBFile.o SortedDBFile.o y.tab.o lex.yy.o test.o -lfl -lpthread

test2_1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o HeapDBFile.o Pipe.o y.tab.o lex.yy.o test2_1.o
	$(CC) -o test2_1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o HeapDBFile.o Pipe.o y.tab.o lex.yy.o test.o -lfl -lpthread

test2_2.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o y.tab.o lex.yy.o test2_2.o
	$(CC) -o test2_2.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o y.tab.o lex.yy.o test.o -lpthread #-lfl

test3.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test3.o
	$(CC) -o test3.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lpthread

test4_1.out: Record.o Comparison.o Schema.o File.o Statistics.o y.tab.o lex.yy.o test4_1.o
	$(CC) -o test4_1.out Record.o Comparison.o Schema.o File.o Statistics.o y.tab.o lex.yy.o test.o -ll

test.out: QueryPlan.o QueryPlanNodes.o Statistics.o Record.o Comparison.o Schema.o Function.o y.tab.o lex.yy.o test.o QueryPlan.h
	$(CC) -o test.out QueryPlan.o QueryPlanNodes.o Statistics.o Record.o Comparison.o Schema.o Function.o y.tab.o lex.yy.o test.o -ll

gtest1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o HeapDBFile.o y.tab.o lex.yy.o gtest1.o
	$(CC) -o gtest1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o HeapDBFile.o y.tab.o lex.yy.o gtest.o -lfl -l pthread -lgtest

gtest2_1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o HeapDBFile.o y.tab.o lex.yy.o Pipe.o gtest2_1.o
	$(CC) -o gtest2_1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o HeapDBFile.o y.tab.o lex.yy.o Pipe.o  gtest.o -lfl -l pthread -lgtest

gtest2_2.out: Record.o Comparison.o ComparisonEngine.o Schema.o BigQ.o File.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o y.tab.o lex.yy.o Pipe.o gtest2_2.o
	$(CC) -o gtest2_2.out Record.o Comparison.o ComparisonEngine.o Schema.o BigQ.o	 File.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o y.tab.o lex.yy.o Pipe.o  gtest.o -lfl -l pthread -lgtest

gtest4_1.out: Statistics.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gtest4_1.o
	$(CC) -o gtest4_1.out Statistics.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gtest4_1.o -lfl -l pthread -lgtest

gtest.out: QueryPlan.o QueryPlanNodes.o Statistics.o Record.o Comparison.o Schema.o Function.o gtest.o
	$(CC) -o gtest.out QueryPlan.o QueryPlanNodes.o Statistics.o Record.o Comparison.o Schema.o Function.o gtest.o -lfl -l pthread -lgtest

test1.o: tests/P1/test.cc
	$(CC) -g -c tests/P1/test.cc

test2_1.o: tests/P2.1/test.cc
	$(CC) -g -c tests/P2.1/test.cc

test2_2.o: tests/P2.2/test.cc
	$(CC) -g -c tests/P2.2/test.cc

test3.o: tests/P3/test.cc
	$(CC) -g -c tests/P3/test.cc

test4_1.o: tests/P4.1/test.cc
	$(CC) -g -c tests/P4.1/test.cc

test.o: test.cc
	$(CC) -g -c test.cc

gtest1.o: tests/P1/gtest.cc
	$(CC) -g -c tests/P1/gtest.cc

gtest2_1.o: tests/P2.1/gtest.cc
	$(CC) -g -c tests/P2.1/gtest.cc

gtest2_2.o: tests/P2.2/gtest.cc
	$(CC) -g -c tests/P2.2/gtest.cc

gtest3.o: tests/P3/gtest.cc
	$(CC) -g -c tests/P3/gtest.cc

gtest4_1.o: tests/P4.1/gtest.cc
	$(CC) -g -c tests/P4.1/gtest.cc

gtest.o: gtest.cc
	$(CC) -g -c gtest.cc

main.o: main.cc
	$(CC) -g -c main.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

GenericDBFile.o: GenericDBFile.cc
	$(CC) -g -c GenericDBFile.cc

HeapDBFile.o: HeapDBFile.cc
	$(CC) -g -c HeapDBFile.cc

SortedDBFile.o: SortedDBFile.cc
	$(CC) -g -c SortedDBFile.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
		
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

QueryPlanNodes.o : QueryPlanNodes.cc
	$(CC) -g -c QueryPlanNodes.cc

QueryPlan.o : QueryPlan.cc
	$(CC) -g -c QueryPlan.cc

	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c
		
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f *.bin
	rm -rf *.metadata
	rm -rf *.bigq
