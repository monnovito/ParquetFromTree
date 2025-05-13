Repository which use uproot & **parallel** to read .root files and store the merged data into a one pandas dataframe saved in .parquet file.

The code is for now optimized for CYGNO experiment setup so beware change something for other purposes.

Grant permission to .sh file:
``` 
 chmode +x ParquetFromTree.sh 
 ```

Include your file .root in a folder at the same level of ParquetFromTree.sh named 'root'.
 
Execute it with :
``` 
 ./ParquetFromTree.sh START_RUN END_RUN  
 ```

the final .parquet file will be present in result/ directory.
have fun.
