::SAVE CREDENTIALS
daccli cdh get_credential --project_uri uri:cart:337381:gemsbessus:gemsnabessdataloading_63447 --role_arn arn:aws:iam::046847230914:role/cdh_gemsnabessdataloading_63447 --save

::LOGIN TO ECR
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 046847230914.dkr.ecr.eu-west-1.amazonaws.com

xcopy /E /I ..\..\yesapi yesapi
xcopy ..\src\proxy_day_v2.py .\
xcopy ..\..\writer.py .\
xcopy ..\src\galaxy_vault_config.yaml .\
xcopy ..\src\galaxy_vault_secrets.local.yaml .\
xcopy ..\..\..\galaxy_vault-0.3.4.tar.gz .\

::BUILD DOCKER IMAGE
docker build --no-cache -t 046847230914.dkr.ecr.eu-west-1.amazonaws.com/cdh_gemsnabessdataloading_63447:proxy-day-v2-forecaster-v1.0.0 .

::PUSH DOCKER IMAGE TO ECRcon
docker push 046847230914.dkr.ecr.eu-west-1.amazonaws.com/cdh_gemsnabessdataloading_63447:proxy-day-v2-forecaster-v1.0.0

:: DEPLOY BATCH
daccli batch deploy_stack --config_file batch_job_config.json --project_uri uri:cart:337381:gemsbessus:gemsnabessdataloading_63447 --role_arn arn:aws:iam::046847230914:role/cdh_gemsnabessdataloading_63447 --stack_name proxy-day-v2-forecaster

rmdir /S /Q yesapi
del proxy_day_v2.py
del writer.py
del galaxy_vault_config.yaml
del galaxy_vault_secrets.local.yaml
del galaxy_vault-0.3.4.tar.gz