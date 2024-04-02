# Galaxy Credential Manager

-----

Internal package to fetch necessary crednetials and secrest to access various resources for Galaxy apps.
Currently utilizes Azure Key Vault + Local Config or pure Local Config to fetch secrets.

# User Guide
## Install/Update

-----

* With Poetry: `poetry add ..[Galaxy repo]\Utilities\galaxy-vault\dist\galaxy-vault.x.y.z-py3-none-any.whl`
* With pip: `pip install ..[Galaxy repo]\Utilities\galaxy-vault\dist\galaxy-vault-x.y.z.tar.gz`


## Usage

-----

### Azure Key Vault

-----

1. In `[current or parent directory]\galaxy_vault_config.yaml`, set `source: azure` and fill out the fields under `azure`. <br />
You can get the values from Matthew Nguyen
<br />
**DEVS**: For testing against development environments, change `env` to `dev` and point to the dev `key_vault_name`

```yaml
source: azure
source_options:
  - azure
  - local
source_options_comments: Non-supported option defaults to local

azure:
  key_vault_name: KEY_VAULT_NAME
  client_id: SERVICE_PRINCIPAL_CLIENT_ID
  client_secret: SERVICE_PRINCIPAL_SECRET
  tenant_id: SERVICE_PRINCIPAL_TENANT_ID
  env: prod
  env_options:
    - dev
    - prod
  env_comment: Allows reading from galaxy_vault_secrets.azure.yaml or galaxy_vault_secrets.azure.dev.yaml
```

2. In `C:\Users\[your username]\galaxy_vault_secrets.azure.yaml`, and fill out any individual account secrets needed for your scripts/app
<br />
Use the Galaxy Encryption Tool to populate the Encrypted credentials
<br />
**DEVS**: Use a `galaxy_vault_secrets.azure.dev.yaml` instead for your development environment.
<br />
**NOTE**: Galaxy Vault will look in several locations for the secrets file. Current directory > User Home > All parent directories

```yaml
comment: This should never be commmited with actual credentials! Only placeholders

individual-accounts-comment: 'Can be any format, just be consistent. Currently Accessed through dot notation for each level. Ex: yes_energy.username'
yes-energy-username: ENCRYPTED_YES_ENERGY_USERNAME
yes-energy-password: ENCRYPTED_YES_ENERGY_PASSWORD

metelogica-username: ENCRYPTED_GENSCAPE_USERNAME_PLACEHOLDER
metelogica-password: ENCRYPTED_GENSCAPE_PASSWORD_PLACEHOLDER

genscape-username: ENCRYPTED_GENSCAPE_USERNAME_PLACEHOLDER
genscape-password: ENCRYPTED_GENSCAPE_PASSWORD_PLACEHOLDER

certificate-comments: Needs to be absolute path to certificate file. Always unencrypted
ercot-certificate: UNENCRYPTED_PATH_TO_ERCOT_CERT_PLACEHOLDER
```

3. In your code, get the Vault
```python
from galaxy_vault import VaultFactory


factory = VaultFactory()
vault = factory.get_vault()
```

4. Get a service account secret with
```python
vault.get_secret('database-username')
```

5. Get an individual account secret with the path in the yaml file
```python
vault.get_secret_individual_account('yes-energy-username')
```

6. Get a certificate with
```Python
vault.get_certificate('ercot-certificate')
```

7. Get an individual certificate with
```Python
vault.get_certificate_individual_path('ercot-certificate-path')
```

8. (Optional) Helper methods have been made to get certain credentials, which returns an object with username, password, etc.

```python
db_credentials = vault.get_db_credentials()
# db_credentials.username gives you username
# db_credentials.password gives you password
# db_credentials.server gives you server
```

#### All Helper methods
```Python
vault.get_db_credentials()
vault.get_fleet_manager_read_credentials()
vault.get_fleet_manager_write_credentials()
vault.get_apx_credentials()
vault.get_ice_credentials()
```

### With Local Vault

-----

0. Local Vault has been created with the Azure Key Vault in mind. in most cases completing steps 1 and 2 are only needed to swap over to using it

1. In `[current or parent directory]\galaxy_vault_config.yaml` or swap `azure` to `local`

```yaml
source: local
source_options:
  - azure
  - local
source_options_comments: Non-supported option defaults to local
```

2. In `C:\Users\[your username]\galaxy_vault_secrets.local.yaml`, fill out any service and individual account secrets needed for your scripts/app
```yaml
comment: This is an example and should never be commmited with actual credentials! Only placeholders

service-accounts-comment: Must match values in Azure Key Vault
database-server: UNENCRYPTED_DATABASE_SERVER
database-username: UNENCRYPTED_DATABASE_SERVER_USERNAME
database-password: UNENCRYPTED_DATABASE_SERVER_PASSWORD

fleet-manager-base-url: UNENCRYPTED_FLEET_MANAGER_BASE_URL
fleet-manager-read-username: UNENCRYPTED_FLEET_MANAGER_READ_USERNAME
fleet-manager-read-password: UNENCRYPTED_FLEET_MANAGER_READ_PASSWORD
fleet-manager-write-username: UNENCRYPTED_FLEET_MANAGER_WRITE_USERNAME
fleet-manager-write-password: UNENCRYPTED_FLEET_MANAGER_WRITE_PASSWORD

apx-auth-url: UNENCRYPTED_APX_AUTH_URL
apx-base-url: UNENCRYPTED_APX_BASE_URL
apx-username: UNENCRYPTED_APX_USERNAME
apx-password: UNENCRYPTED_APX_PASSWORD
apx-client-id: UNENCRYPTED_APX_CLIENT_ID
apx-client-secret: UNENCRYPTED_APX_CLIENT_SECRET

ice-auth-url: UNENCRYPTED_ICE_AUTH_URL
ice-base-url: UNENCRYPTED_ICE_BASE_URL
ice-username: UNENCRYPTED_ICE_USERNAME
ice-password: UNENCRYPTED_ICE_PASSWORD
ice-api-key: UNENCRYPTED_ICE_API_KEY

yes-energy-base-url: UNENCRYPTED_YES_ENERGY_BASE_URL
metelogica-base-url: UNENCRYPTED_METELOGICA_BASE_URL

caiso-oasis-url: UNENCRYPTED_CAISO_OASIS_URL
caiso-base-url: UNENCRYPTED_CAISO_BASE_URL
caiso-mobile-url: UNENCRYPTED_MOBILE_URL


individual-accounts-comment: 'Azure/AWS/HashiCorp only certain characters for keys. follow-this-format'
yes-energy-username: UNENCRYPTED_YES_ENERGY_USERNAME
yes-energy-password: UNENCRYPTED_YES_ENERGY_PASSWORD

metelogica-username: UNENCRYPTED_GENSCAPE_USERNAME
metelogica-password: UNENCRYPTED_GENSCAPE_PASSWORD

genscape-username: UNENCRYPTED_GENSCAPE_USERNAME
genscape-password: UNENCRYPTED_GENSCAPE_PASSWORD

certificate-comment: Needs to be absolute path to certificate file. Always unencrypted
ercot-certificate: UNENCRYPTED_PATH_TO_ERCOT_CERT
```

Follow steps 3 onward in Azure Key Vault

## Notes about config files

-----

Config files:

1. `galaxy_vault_config.yaml` - determines which vendor's vault or secret manager to use, as well as settings to connect to said vendor <br />
Currently, can use Azure Key Vault or Local file (acting as a Vault)

2. `galaxy_vault_secrets.azure.yaml` -A local vault to hold individual account secrests that aren't stored in a provider's key vault, since they're not meant to be shared. <ins>This is a temporary solution until an Enterprise Application can be build, in which individual account credentials should not longer be stored in this file</ins> <br />
All values in this file should be ENCRYPTED

3. `galaxy_vault_secrets.local.yaml` - A local vault to simplify testing new secrets, or if a developer does not have access to Azure Key Vault.<br />
Should only be used by developers


# Developer Guide

-----

## Install Poetry

-----

1. Install pipx
2. `pipx install poetry`

## Testing

-----

`cd tests` then `poetry run pytest`

TODO: Can technically do this in VS code but the current working directory needs to be set to the correct test folder, otherwise additional yaml files won't be found

## Release New Package Version

-----

1. Update version with `poetry version x.y.z`
2. Run `poetry build`
3. Run `poetry export --without-hashes --format=requirements.txt > requirements.txt`


## Support

-----

Contact Matthew Nguyen