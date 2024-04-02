from .api_credentials import ApiCredentials


class IceCredentials(ApiCredentials):
    def __init__(self, username, password, base_url, auth_url, api_key):
        super().__init__(username, password, base_url)
        self.auth_url = auth_url
        self.api_key = api_key