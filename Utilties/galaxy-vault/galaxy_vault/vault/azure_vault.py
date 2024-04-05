from azure.keyvault.certificates import CertificateClient
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from galaxy_vault.vault import Vault
from galaxy_vault.model import ApiCredentials, ApxCredentials, DatabaseCredentials, IceCredentials
from galaxy_vault.util import decryptor, file_util
import pydash
import yaml


class AzureVault(Vault):
    def __init__(self):
        self.__init_azure_clients()
        self.__init_encryption_key()
        self.__init_individual_account_secrets()


    def get_secret(self, path):
        return self.secret_client.get_secret(path).value


    def get_secret_individual_account(self, path):
        '''Returns secret from galaxy_vault_secrets.azure.yaml under user path'''
        value = pydash.get(self.individual_account_secrets, path)
        if value:
            return decryptor.decrypt(
                value,
                self.encryption_key
            )
        else:
            print("Error: Secret not found. Please check with the GEMS IT Team to ensure the value is in the Vault")
            return None

    def get_certificate(self, path):
        return self.certificate_client.get_certificate(path)
    
    
    def get_certificate_individual_path(self, path):
        '''Returns path to cert galaxy_vault_secrets.azure.yaml from file under user path'''
        return pydash.get(self.individual_account_secrets, path)


    # Helper methods below

    def get_db_credentials(self) -> DatabaseCredentials:
        return DatabaseCredentials(
            self.get_secret('database-server'),
            self.get_secret('database-username'),
            self.get_secret('database-password')
        )
    

    def get_fleet_manager_read_credentials(self) -> ApiCredentials:
        return ApiCredentials(
            username=self.get_secret('fleet-manager-read-username'),
            password=self.get_secret('fleet-manager-read-password'),
            base_url=self.get_secret('fleet-manager-base-url')
        )
    
    def get_fleet_manager_write_credentials(self) -> ApiCredentials:
        return ApiCredentials(
            username=self.get_secret('fleet-manager-write-username'),
            password=self.get_secret('fleet-manager-write-password'),
            base_url=self.get_secret('fleet-manager-base-url')
        )
    

    def get_apx_credentials(self) -> ApxCredentials:
        return ApxCredentials(
            self.get_secret('apx-username'),
            self.get_secret('apx-password'),
            self.get_secret('apx-base-url'),
            self.get_secret('apx-client-id'),
            self.get_secret('apx-client-secret'),
            self.get_secret('apx-auth-url')
        )
    

    def get_ice_credentials(self) -> IceCredentials:
        return IceCredentials(
            self.get_secret('ice-username'),
            self.get_secret('ice-password'),
            self.get_secret('ice-base-url'),
            self.get_secret('ice-auth-url'),
            self.get_secret('ice-api-key')
        )


    def __init_azure_clients(self):
        config_filepath = file_util.find_filepath('galaxy_vault_config.yaml')
        with open(config_filepath, 'r') as f:
            config = (yaml.safe_load(f))['azure']

        key_vault_name = config['key_vault_name']
        key_vault_uri = f"https://{key_vault_name}.vault.azure.net"
        
        credential = ClientSecretCredential(
            config['tenant_id'], 
            config['client_id'], 
            config['client_secret']
        )
        try:
            self.secret_client = SecretClient(
                vault_url=key_vault_uri,
                credential=credential
            )
        except Exception as e:
            print("Error: Could not connect to the Azure Key Vault: %s", str(e))

        self.certificate_client = CertificateClient(
            vault_url=key_vault_uri, 
            credential=credential,
        )


    def __init_individual_account_secrets(self):
        filepath = file_util.find_filepath('galaxy_vault_secrets.azure.yaml', True)
        with open(filepath, 'r') as f:
            self.individual_account_secrets = yaml.safe_load(f)
        

    def __init_encryption_key(self):
        self.encryption_key = self.get_secret('encryption-key')
