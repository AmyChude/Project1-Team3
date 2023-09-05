## Deploy docker container to Amazon Web Services (provide screenshot evidence of services configured/running):

### Elastic Container Service (ECS) - screenshot of scheduled task in ECS

Finished task in ECS:
![aws_02_ecs_finish.png](images/aws/aws_02_ecs_finish.png)
Logs of task in ECS:
![aws_03_ecs_log.png](images/aws/aws_03_ecs_log.png)

### Elastic Container Registry (ECR) - screenshot of image in ECR
Docker images in the ECR:
![aws_04_ecr_images.png](images/aws/aws_04_ecr_images.png)

### Relational Database Service (RDS) or Simple Storage Service (S3) depending on your choice of target storage - screenshot of dataset in target storage
Database in RDS:
![aws_05_rds_db.png](images/aws/aws_05_rds_db.png)
Query a table in RDS database:
![aws_06_rds_query.png](images/aws/aws_06_rds_query.png)

### IAM Role - screenshot of created role
Role in IAM:
![aws_07_iam_role.png](images/aws/aws_07_iam_role.png)

### S3 for `.env` file - screenshot of `.env` file in S3
.env file in S3:
![aws_08_s3_env.png](images/aws/aws_08_s3_env.png)