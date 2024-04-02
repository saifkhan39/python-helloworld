from abc import ABC, abstractmethod
from galaxy_vault.model import ApiCredentials, ApxCredentials, DatabaseCredentials, IceCredentials


class Vault(ABC):
    @abstractmethod
    def get_secret(self, path) -> str:
        pass


    @abstractmethod
    def get_secret_individual_account(self, path) -> str:
        pass


    @abstractmethod
    def get_db_credentials(self) -> DatabaseCredentials:
        pass


    @abstractmethod
    def get_fleet_manager_read_credentials(self) -> ApiCredentials:
        pass


    @abstractmethod
    def get_fleet_manager_write_credentials(self) -> ApiCredentials:
        pass


    @abstractmethod
    def get_apx_credentials(self) -> ApxCredentials:
        pass


    @abstractmethod
    def get_ice_credentials(self) -> IceCredentials:
        pass


    @abstractmethod
    def get_certificate(self, path):
        pass


    @abstractmethod
    def get_certificate_individual_path(self, path):
        pass