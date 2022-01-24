#!/bin/bash

# set query path parameter
data_set_folder="./data/queries/WatDiv/"
query_file_type=(C L SF S)
query_file_suffix=".in"
query_file_num=20

# specify RDF database name
database_name=watdiv100m

# binary executable for query
EXE=./bin/psoQuery

# set output folder
result_output_folder="./result_WatDiv100m/"
output_file_suffix=".txt"

if [ -d $result_output_folder ]; then
    rm -rf $result_output_folder
fi

mkdir -p $result_output_folder

for file_type in ${query_file_type[@]}
do
    echo 'Query Type: ' $file_type
    for (( i = 1; i <= query_file_num; i++ ))
    do
        file_path="${data_set_folder}${file_type}${i}${query_file_suffix}"
        rslt_path="${result_output_folder}${file_type}${i}${output_file_suffix}"
        if [ -f $file_path ]; then
            echo $EXE $database_name $file_path 
            # $EXE $database_name $file_path > $rslt_path
	          (time $EXE $database_name $file_path) > $rslt_path
        else
            echo "$file_path does not exist."
        fi
    done
done
