from .credentials import Credentials


class ApiCredentials(Credentials):
    def __init__(self, username, password, base_url):
        super().__init__(username, password)
        base_url = base_url