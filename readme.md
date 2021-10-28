TODO:
* DONE Set up ECR repos for both indexation images
* Set up indexation task (remeber to include the EFS mount)
* Test Travis build
* Test indexation
* Readme for linkliveslib (especially concerning pushing packages to nuget.org (right now done by hand))
* Check DataSync job and describe it somewhere

# Linklives-indexer-new
Dotnet application used to index person appearances, links and life courses from link-lives data (procedured by WP3) displayed on the website https://link-lives.dk (made by WP4).

The solution consists of a total of four projects: Two indexers, an indexer library and an indexer test project.

## Building
Indexers must be built individually. 
The project depends on LinkLivesLib which must be added as a local repository nuget package in order to be received when building the projects.

### Linklives-lifecourse-indexer
From the solution dir run: ``docker build -f linklives-lifecourse-indexer/Dockerfile.prod -t linklives-lifecourse-indexer  .``

### Linklives-transcribed-indexer
From the solution dir run: ``docker build -f linklives-transcribed-indexer/Dockerfile.prod -t linklives-transcribed-indexer .``

### LinkLivesLib nuget package
Both indexers depends on the LinkLivesLib package.

It is currently build in hand and published to nuget.org manually.

## Running in production
AWS Batch is used a basis for execution of the indexation.
Two tasks are setup in AWS Batch: linklives-lifecourse-indexer and linklives-transcribed-indexer. Each runs an individual indexation.

Each task has two variations: Full and limited-dev. Limited-dev is used for small scale tests.

When changes are made to the source code, a new Docker image must be uploaded to AWS ECR in order to run the newest code.

All this is done using Travis-CI.

## Running
### Linklives-lifecourse-indexer
``docker run linklives-lifecourse-indexer --es-host https://data-dev.link-lives.dk --path /app/link-lives/LL_data_v1.0.dev0/development --db-conn server=***REMOVED***;***REMOVED***;pwd=***REMOVED***;database=***REMOVED*** --max-entries 10``

### Data
The WP3 data is currently delivered by hand doing like this:
* Upload the data to S3://linklives-data
* Start a DataSync job. This will copy data from the linklives-data bucket to the EFS system used by AWS Batch