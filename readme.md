# Programming Exercise using PySpark

## What tools were used
- Jupyter Notebook running in a docker container (jupyter/pyspark-notebook)

## What does this code do:
- Creates a spark session
- Takes two CSV datasets and joins them based on id field
- Contains a function for filtering the joined dataset by countries (accepts multiple values)
- Contains a function for filtering dropping / renaming columns
- Logs actions to logfile (with rotation policy in place)



## TO DO:
- Finish configuration of Travis CI pipeline
- Implement testing 

