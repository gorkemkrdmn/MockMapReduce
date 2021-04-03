#include "mapreduce.h"


MapReduce::MapReduce(string inp_file_name, string out_file_name, int inp_filec, int mapc, int redc, string (*map_func)(string), string(*combiner_func) (vector<string>), string(*reducer_func)(vector<string>)): reducer_count(redc), mapper_count(mapc){
    this->reducer_count = redc;
    this->mapper_count = mapc;
    this->reducer_index = 0;
    this->mapper_index = 0;
    this->map_func = map_func;
    this->reducer_func = reducer_func;
    this->combiner_func = combiner_func;
    this->out_file_name = out_file_name;
    this->open_inter_filec = 0;
	int j = -1;
    ofstream output_file;
    ifstream input_file(inp_file_name);
    string line;
	for(int i =0; i<inp_filec; i++){
        getline(input_file, line);
        if(i%((int)ceil((double) inp_filec/mapc))==0){
            if(output_file.is_open())
                output_file.close();
            j++;
            output_file.open(to_string(j) +"minput.txt");
            (this->open_inter_filec)++;
        }
        output_file << line << endl;
    }
}


int MapReduce::clearFiles(int mapper_count, int reducer_count, string input_file){
    for(int i =0; i<mapper_count; i++)
        for(int j = 0; j<reducer_count; j++){
            const char* c =  (to_string(j) + to_string(i)+ input_file).c_str();
            if(remove(c) != 0 )
                perror( "Error deleting file" );
                //puts("File successfully deleted" );
        }
        for(int i =0; i<open_inter_filec; i++){
            const char* c =  (to_string(i) + "minput.txt").c_str();
            if(remove(c) != 0 )
                cout<<  "Error deleting file" <<endl;
       }
  return 0;
}

void MapReduce::startExecute(){
    for(int i = 0; i<mapper_count; i++)
        Mapper mtask(to_string(i) + "minput.txt", to_string(i) +"moutput.txt", map_func, combiner_func, reducer_count);
    for(int j = 0 ; j<reducer_count; j++)
        Reducer rtask("moutput.txt",  out_file_name, j , reducer_func, mapper_count);
    clearFiles(mapper_count, reducer_count, "moutput.txt");
}

void MapReduce::mapFinished(){
    mapper_index++;
  	cout<<"Map Finished"<< endl;
}

int mapperHashFunction(string key){
    return int(key[0]);
}

Mapper::Mapper(string in_file_name, string out_file_name, string (*map_func)(string), string(*combiner_func) (vector<string> ), int reducerCount){
    ifstream input_file(in_file_name);
    ofstream output_file;
    vector<vector<string>> shuffledKeys(reducerCount);
    string line;
    if (input_file.is_open()){
        while (getline(input_file,line)){
            if(combiner_func){
                if(store.find(line)==store.end()){
                    vector<string> init = {map_func(line)};
                    store[line] = init;
                    continue;
                }
                store[line].push_back(map_func(line));    
            }
            else
                shuffledKeys[mapperHashFunction(line)%reducerCount].push_back(line+","+map_func(line));
        }
        input_file.close();
    }
    if(combiner_func){
        for (auto& it: store) {
            shuffledKeys[mapperHashFunction(it.first)%reducerCount].push_back(it.first + "," + combiner_func(it.second));
        }
    }
    for(int i = 0; i<reducerCount; i++){
        output_file.open(to_string(i) + out_file_name);
        sort(shuffledKeys[i].begin(), shuffledKeys[i].end());
        for(auto&a : shuffledKeys[i])
            output_file << a << endl;
        output_file.close();
    }
}
Reducer::Reducer(string in_file_name, string out_file_name, int reducerID, string(*reducer_func) (vector<string>), int mapperCount){
    ifstream input_file;
    ofstream output_file;
    unordered_map<string, vector<string>>inputKeys;
    string line;
    for(int i = 0; i<mapperCount; i++){
        input_file.open(to_string(reducerID) + to_string(i) + in_file_name);
        if (input_file.is_open())
        {
        while (getline(input_file,line))
        {
            string delim = ",";
            auto start = 0U;
            auto end = line.find(delim);
            string key;
            string value;
            key = line.substr(start, end-start);
            value = line.substr(end+1, line.length()-(end+1));
            if(inputKeys.find(key)==inputKeys.end())
                inputKeys[key] = vector<string>{value};
            else
                inputKeys[key].push_back(value);
        }
        input_file.close();
        } 
    }
    output_file.open(to_string(reducerID) + out_file_name);
    for(auto& it: inputKeys){
        output_file<< it.first << "," << reducer_func(it.second)<<endl;
    }
}



