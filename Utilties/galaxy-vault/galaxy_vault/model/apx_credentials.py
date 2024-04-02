from .api_credentials import ApiCredentials


class ApxCredentials(ApiCredentials):
    def __init__(self, username, password, base_url, client_id, client_secret, auth_url):
        super().__init__(username, password, base_url)
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_url = auth_url