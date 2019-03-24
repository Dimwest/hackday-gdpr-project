# jsonymize

Python Spark job anonymizing JSON data. 
Can be used for large tasks such as cleaning up application 
events' data in S3 buckets for GDPR compliance.

## To-do list:

### Functionalities
- [x] Add config validation
- [x] Add processing steps logging
- [ ] Fetch source files from S3 bucket
- [ ] Create new S3 bucket to receive anonymized files
- [ ] Engineer an exception handling system preventing 
long-running jobs to stop
- [ ] Add support for parquet files (... then rename the project)
- [ ] Add bucket namespace handling per year/month/date/hour/minute
- [ ] Add in-place replacement functionality
- [ ] Add conversion JSON <--> parquet

### Refactoring & improvements
- [ ] Add type hints
- [ ] Track execution metrics (https://github.com/LucaCanali/sparkMeasure/blob/master/docs/Python_shell_and_Jupyter.md)

### Build & deployment
- [ ] Add Travis CI build
- [ ] Restructure as ready-to-use Spark job
- [ ] Add bash script for running
- [ ] Add Terraform module for spawning and provisioning 
EMR cluster, with task scheduling setup (in another repo)

### Tests 
- [ ] Add unit tests
- [ ] Add integration tests
- [ ] Add e2e tests
- [ ] Add coverage report
- [ ] Run large scale test and performance check