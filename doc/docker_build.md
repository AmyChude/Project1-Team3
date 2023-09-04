aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 282824242464.dkr.ecr.us-east-1.amazonaws.com/dec-p1-t3
docker build --platform=linux/amd64 -t dec-p1-t3 .
docker tag dec-p1-t3:latest 282824242464.dkr.ecr.us-east-1.amazonaws.com/dec-p1-t3:latest
docker push 282824242464.dkr.ecr.us-east-1.amazonaws.com/dec-p1-t3:latest