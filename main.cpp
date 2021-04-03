#include "mapreduce.h"


string mpf(string x){
    return "1";
}


string rdf(vector<string> a){
    int res = 0;
    for(auto e: a){
        res+=stoi(e);
    }
    return to_string(res);
}


int main(){
    MapReduce mst("input.txt","output.txt", 10,2,2,mpf,rdf, rdf); //Input file name, [reducerID]output filename, linecount of input file, map task count, reduce task count, mapper function, combiner function, reducer function
    mst.startExecute();
    return 0;
}

