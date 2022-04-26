#include <iostream>

#include "ParseTree.h"
#include "QueryPlan.h"
#include "Statistics.h"

using namespace std;

// test settings file should have the
// catalog_path
const char *settings = "test.cat";

// donot change this information here
char *catalog_path = NULL;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

char *statisticsFileName = "Statistics42.txt";

int main() {

    // Loading Statistics from the file.
    Statistics statistics;
    statistics.Read(statisticsFileName);

    // Parse the query.
    yyparse();

    Query query = {finalFunction, tables, boolean, groupingAtts, attsToSelect, distinctAtts, distinctFunc};

    // Create query plan/
    QueryPlan queryPlan( &statistics, &query);

    // Print the query plan in post order.
    queryPlan.Print();
}


