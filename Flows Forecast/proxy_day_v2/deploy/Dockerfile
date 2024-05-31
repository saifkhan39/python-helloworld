FROM 046847230914.dkr.ecr.eu-west-1.amazonaws.com/python-base:3.10.12
ARG PY_VERSION="310"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root

COPY yesapi ./yesapi
COPY proxy_day_v2.py ./
COPY writer.py ./
COPY galaxy_vault_config.yaml ./
COPY galaxy_vault_secrets.local.yaml ./
COPY galaxy_vault-0.3.4.tar.gz ./
COPY requirements.txt ./

RUN python -m pip install pip --upgrade && pip install -r ./requirements.txt && rm -r ./requirements.txt

ENV http_proxy http://swag.proxy:8080
ENV https_proxy http://swag.proxy:8080
ENV HTTP_PROXY http://swag.proxy:8080
ENV HTTPS_PROXY http://swag.proxy:8080
ENV no_proxy 172.20.0.0/16,localhost,127.0.0.1,10.0.0.0/8,169.254.169.254,10.170.194.0/24,52.218.0.0/17,3.5.72.0/23,3.5.64.0/21,.internal,eu-west-1.eks.amazonaws.com,.api.ecr.eu-west-1.amazonaws.com,.dkr.ecr.eu-west-1.amazonaws.com,.ec2.eu-west-1.amazonaws.com
ENV NO_PROXY 172.20.0.0/16,localhost,127.0.0.1,10.0.0.0/8,169.254.169.254,10.170.194.0/24,52.218.0.0/17,3.5.72.0/23,3.5.64.0/21,.internal,eu-west-1.eks.amazonaws.com,.api.ecr.eu-west-1.amazonaws.com,.dkr.ecr.eu-west-1.amazonaws.com,.ec2.eu-west-1.amazonaws.com

USER $NB_USER