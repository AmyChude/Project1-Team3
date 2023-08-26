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
How is your project broken down? Who is doing what?

We recommend using a free Task board such as [Trello](https://trello.com/). This makes it easy to assign and track tasks to each individual. 

Example: 

![images/kanban-task-board.png](images/kanban-task-board.png)