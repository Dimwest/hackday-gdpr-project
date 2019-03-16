# jsonymize

Python tool anonymizing JSON data using Spark. 
Can be used for large tasks such as cleaning up application 
events' data in S3 buckets for GDPR compliance.

## To-do list:

### Functionalities
- [ ] Fetch source files from S3 bucket
- [ ] Create new S3 bucket to receive anonymized files
- [ ] Engineer an exception handling system preventing 
long-running jobs to stop (e.g. flag failed events to store in
a specific bucket)
- [ ] Add support for parquet files (... then rename the project)
- [ ] Add bucket namespace handling per year/month/date/hour/minute
- [ ] Add in-place replacement functionality
- [ ] Add conversion JSON <--> parquet

### Refactoring & improvements
- [ ] Add type hints
- [ ] Track execution time

### Build & deployment
- [ ] Add Travis CI build
- [ ] Add Terraform module for spawning and provisioning 
EMR cluster, with task scheduling setup (in another repo)

### Tests 
- [ ] Add unit tests
- [ ] Add integration tests
- [ ] Add e2e tests
- [ ] Add coverage report
- [ ] Run large scale test and performance check