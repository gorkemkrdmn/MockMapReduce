#ifndef MAPREDUCE
#define MAPREDUCE

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <math.h> 
#include <algorithm>
#include <stdio.h>

using namespace std; 


using namespace std;

class MapReduce{
private:
    int reducer_count;
    int mapper_count;
    int* reducer_tasks;
    int* mapper_tasks;
    int mapper_index;
    int reducer_index;
    int open_inter_filec = 0;
    string (*map_func)(string);
    string (*combiner_func)(vector<string>);
    string (*reducer_func)(vector<string>);
    string out_file_name;
public:
    MapReduce(string ,string , int , int , int , string (*)(string), string(*) (vector<string>), string(*)(vector<string>));
    int clearFiles(int mapper_count, int reducer_count, string input_file); 
    void startExecute();
    void mapFinished();
};

class Mapper{
private:
    unordered_map<string, vector<string>> store;
public:
    Mapper(string , string , string (*)(string), string(*) (vector<string>), int);
};

class Reducer{
private:
public:
    Reducer(string , string , int , string(*reducer_func) (vector<string>), int ); 
};


#endif
