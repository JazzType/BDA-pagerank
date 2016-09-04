# BDA-pagerank
Matrix-Vector Multiplication and PageRank computation

##commit d815a3f changes
- Tidy up
- Removed unnecessary imports
- Eigenvector calculation (according to [this](http://stackoverflow.com/a/774221) answer on SO)
- Final commit, merged master and dev

##commit fe14a2d changes
- Now using MultipleInputs class for input
- Added MultipleOutputs implementation
- Added base framework for iterative PageRank computation
- Added Mapper/Reducer for Vector comparison
- Added custom file output names

## Executing
Inside the folder where the .java files are present, run
```
$ javac *.java -classpath `hadoop classpath`
$ jar cf PageRank.jar *.class 
$ hadoop jar PageRank.jar MatrixVectorMultiplyMain <path-to-input-files> <temp_output_directory> <final_output_directory>
```
