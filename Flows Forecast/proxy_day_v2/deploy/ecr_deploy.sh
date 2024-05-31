#SAVE CREDENTIALS
daccli cdh get_credential --project_uri uri:cart:337381:gemsbessus:gemsnabessdataloading_63447 --role_arn arn:aws:iam::046847230914:role/cdh_gemsnabessdataloading_63447 --save

#LOGIN TO ECR
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 046847230914.dkr.ecr.eu-west-1.amazonaws.com

cp -r ../../yesapi yesapi
cp ../src/proxy_day_v2.py ./
cp ../../writer.py ./
cp ../src/galaxy_vault_config.yaml ./
cp ../src/galaxy_vault_secrets.local.yaml ./
cp ../../../galaxy_vault-0.3.4.tar.gz ./

#BUILD DOCKER IMAGE
docker build --no-cache -t 046847230914.dkr.ecr.eu-west-1.amazonaws.com/cdh_gemsnabessdataloading_63447:proxy-day-v2-forecaster-v1.0.0 .

#PUSH DOCKER IMAGE TO ECRcon
docker push 046847230914.dkr.ecr.eu-west-1.amazonaws.com/cdh_gemsnabessdataloading_63447:proxy-day-v2-forecaster-v1.0.0

#DEPLOY BATCH JOB
daccli batch deploy_stack --config_file batch_job_config.json --project_uri uri:cart:337381:gemsbessus:gemsnabessdataloading_63447 --role_arn arn:aws:iam::046847230914:role/cdh_gemsnabessdataloading_63447 --stack_name proxy-day-v2-forecaster

rm -rd yesapi
rm proxy_day_v2.py
rm writer.py
rm galaxy_vault_config.yaml
rm galaxy_vault_secrets.local.yaml
rm galaxy_vault-0.3.4.tar.gz