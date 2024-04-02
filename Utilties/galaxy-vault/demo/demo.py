from galaxy_vault.factory import VaultFactory

factory = VaultFactory()
vault = factory.get_vault()
creds = vault.get_db_credentials()
apx_creds = vault.get_apx_credentials()

yes_energy_username = vault.get_secret_individual_account('yes-energy-username')
yes_energy_password = vault.get_secret_individual_account('yes-energy-password')

print(creds.username)
print(creds.password)
print(creds.server)
print(yes_energy_username)
print(apx_creds.client_id)
print(apx_creds.client_secret)
print(yes_energy_username)
print(yes_energy_password)