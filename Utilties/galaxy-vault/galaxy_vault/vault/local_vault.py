from galaxy_vault.vault import Vault
from galaxy_vault.model import ApiCredentials, ApxCredentials, DatabaseCredentials, IceCredentials
from galaxy_vault.util import file_util
from pykeepass import PyKeePass
import pydash
import yaml


class LocalVault(Vault):
    def __init__(self):
        self.__init_data()
        self.__init_encryption_key()


    def get_secret(self, path) -> str:
        return self.get_secret_individual_account(path)
    

    def get_secret_individual_account(self, path) -> str:
        if self.is_encrypted and path != "ercot-certificate":
            return decryptor.decrypt(
                pydash.get(self.secrets, path),
                self.encryption_key
            )
        else:
            return pydash.get(self.secrets, path)
    

    def get_certificate():
        raise NotImplementedError
    

    def get_certificate_individual_path(self, path):
        self.get_secret(path)


    def get_db_credentials(self) -> DatabaseCredentials:
        return DatabaseCredentials(
            self.get_secret('database-username'),
            self.get_secret('database-password'),
            self.get_secret('database-server')
        )


    def get_fleet_manager_read_credentials(self) -> ApiCredentials:
        return ApiCredentials(
            self.get_secret('fleet-manager-read-username'),
            self.get_secret('fleet-manager-read-password'),
            self.get_secret('fleet-manager-base-url')
        )
    

    def get_fleet_manager_write_credentials(self) -> ApiCredentials:
        return ApiCredentials(
            self.get_secret('fleet-manager-write-username'),
            self.get_secret('fleet-manager-write-password'),
            self.get_secret('fleet-manager-base-url')
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


    def __init_data(self):
        filepath = file_util.find_filepath('galaxy_vault_secrets.local.yaml', True)
        with open(filepath, 'r') as f:
            self.secrets = yaml.safe_load(f)


    def __init_encryption_key(self):
        config_filepath = file_util.find_filepath('galaxy_vault_config.yaml')
        with open(config_filepath, 'r') as f:
            config = (yaml.safe_load(f))['local']


        self.is_encrypted = config['is_encrypted']

        if self.is_encrypted:
            keepass = PyKeePass(config['keepass-database'], password=config['keepass-database-password'])
            entry_title = keepass.find_entries(title=config['keepass-title'], first=True)
            self.encryption_key = entry_title.password

