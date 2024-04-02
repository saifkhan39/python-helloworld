from galaxy_vault.vault import AzureVault, LocalVault
from galaxy_vault.util import file_util
import yaml


class VaultFactory:
    def __init__(self):
        self.secrets = self.__get_config()


    def get_vault(self):
        source = self.secrets['source']
        match source:
            case 'azure':
                return AzureVault()
            case _:
                return LocalVault()
            
    def __get_config(self):
        filepath = file_util.find_filepath('galaxy_vault_config.yaml')
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)