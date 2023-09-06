## Docker Build and Upload to AWS

### Login to AWS ECR
```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <your_IAM_id>.dkr.ecr.us-east-1.amazonaws.com/dec-p1-t3
```

### Build and Upload Docker Image
```bash
docker build --platform=linux/amd64 -t dec-p1-t3 .
```

### Local testing of Docker Image
```bash
docker run --env-file .\.env  dec-p1-t3:latest
```

### Push Docker Image to AWS ECR
```bash
docker tag dec-p1-t3:latest 282824242464.dkr.ecr.us-east-1.amazonaws.com/dec-p1-t3:latest
docker push 282824242464.dkr.ecr.us-east-1.amazonaws.com/dec-p1-t3:latest
```

### Upload .env file to AWS S3
Remove quotation marks from the .env file. Upload the .env file to the S3 bucket: arn:aws:s3:::dec-p1-t3