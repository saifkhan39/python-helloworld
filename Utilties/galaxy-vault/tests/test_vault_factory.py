from galaxy_vault import LocalVault, VaultFactory

# TODO: Update Tests to mock the .yaml file being read
def test_factory_local_creates_correct_class():
    factory = VaultFactory()
    vault = factory.get_vault()
    assert type(vault) is LocalVault