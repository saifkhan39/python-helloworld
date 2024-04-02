from galaxy_vault import LocalVault


# TODO: Update Tests to mock the .yaml file being read
def test_local_vault_get_db_credentials():
    vault = LocalVault()
    credentials = vault.get_db_credentials()
    assert credentials.server == 'example.server'
    assert credentials.username == 'Bob123'
    assert credentials.password == 'FakePassword'