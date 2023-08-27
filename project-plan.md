# Project plan 

## Objective 
The objective of our project is to track, aggregate and provide analytical datasets from data engineer jobs fetched from one (or more) job board APIs and currency exchange rates API.

## Consumers 
The users of our datasets are job seekers, recruiters and hiring managers.

## Questions 
 - How many jobs are there for each job title?
 - What locations have the most jobs?
 - Monitor the number of jobs for each job title over time.
 - Track the number of applicants for each job title over time.
 - Identify the most popular companies.
 - What are the average salaries for each location in different currencies?

## Source datasets 

| Source name | Source type | Source documentation |
| - | - | - |
| Reed Jobseeker API| REST API | https://www.reed.co.uk/developers/Jobseeker | 
| Open Exchange Rates API | REST API | https://docs.openexchangerates.org/reference/api-introduction | 




## Solution architecture
How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution? 

- What data extraction patterns are you going to be using? 
- What data loading patterns are you going to be using? 
- What data transformation patterns are you going to be performing? 

We recommend using a diagramming tool like [draw.io](https://draw.io/) to create your architecture diagram. 

Here is a sample solution architecture diagram: 

![images/sample-solution-architecture-diagram.png](images/sample-solution-architecture-diagram.png)

## Breakdown of tasks 

### Extract and Load pipeline ~ Akos, Rihab
- Incremental extract/load
### Transform pipeline ~ Uzo
- Filtering, grouping, joins/merges
### Unit Tests ~ Rihab 
- Creating unit tests to cover the ETL Process

### stitching the ELT pipeline together, adding logging and creating the Dockerfile for the docker image ~ Uzo, Rihab
-  Metadata logs.
-  Build Docker image.
-  Deploy Docker container
### creating the required AWS services (e.g. RDS, ECR, S3, ECS) ~ Akos, Uzo

### Documentation and preparing slides for the presentation ~ Uzo, Rihab, Akos

### Deploying the solution to AWS ~ Rihab