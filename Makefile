CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

test1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o test1.o
	$(CC) -o test1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o test1.o -lfl

test2_1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o HeapDBFile.o Pipe.o y.tab.o lex.yy.o test2_1.o
	$(CC) -o test2_1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o HeapDBFile.o Pipe.o y.tab.o lex.yy.o test2_1.o -lfl -lpthread
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o main.o -lfl

gtest1: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o gtest1.o
	$(CC) -o gtest1 Record.o gtest1.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o -lfl -l pthread -lgtest

gtest2_1: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o gtest2_1.o Pipe.o
	$(CC) -o gtest2_1 Record.o gtest2_1.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o Pipe.o -lfl -l pthread -lgtest

test1.o: test1.cc
	$(CC) -g -c test1.cc

test2_1.o: test2_1.cc
	$(CC) -g -c test2_1.cc

gtest1.o:
	$(CC) -g -c gtest1.cc

gtest2_1.o:
	$(CC) -g -c gtest2_1.cc

main.o: main.cc
	$(CC) -g -c main.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
HeapDBFile.o: HeapDBFile.cc
	$(CC) -g -c HeapDBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f *.bin
