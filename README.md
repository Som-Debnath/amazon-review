# DWH implementation based on AMAZON product review dataset
This repository contains the DWH model creation scripts as well as data pipelines to extract, transform and load the data into DWH. Finally, three insights (reports) about the data are prodcued from the DWH.

## Prerequisites and dependencies

- Install postgresql database or use already available postgresql db instance
- Python 3.7 or higher
- Clone this repository
- All the additional dependencies will be installed using the build script

___

## Build

- Install all the dependent packages and modules using the setup script located in the root folder:
```
    $ python setup.py
```

___

## Execution

- 1. Modify the ./config.ini files as per the target insfrastructure.
- 2. Execute the .sql file in the desired postgresql database instance.
- 2. Run the workflow.py (luigi orchestration)
```
    $python  workflow.py EndOfWorkflow --local-scheduler

```
