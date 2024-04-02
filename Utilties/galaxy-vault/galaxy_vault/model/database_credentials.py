from .credentials import Credentials


class DatabaseCredentials(Credentials):
    def __init__(self, username, password, server):
        super().__init__(username, password)
        self.server = server