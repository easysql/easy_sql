# Functions
## introduction
Pure query based ETL jobs sometimes cannot satisfy the requirement of the pipeline when there have complex transformation between different stage. To meed this requirement, we enable self-define function is easy sql to trigger python process to further handle the process. Currently we groups the different function into :
+ table function
+ column function
+ partition function
+ model function

Also for different backend, the function is implement seperatly. 
