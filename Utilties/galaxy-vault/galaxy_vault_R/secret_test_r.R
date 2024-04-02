## install reticulate
# install.packages("reticulate")

library(reticulate)

## create R object
# x <- 'test1'

## access objects from python
# reticulate::py_run_string("print(r.x)")


py_run_string("from galaxy_vault.factory import VaultFactory")
py_run_string("factory = VaultFactory()")
print('test2')
vault = factory$get_vault()
test_url = vault$get_secret('apx-base-url')
# py_run_string("vault = factory.get_vault()")
# py_run_string("test_url = vault.get_secret('apx-base-url')")



# py_run_string("from pack import mod")
# mod$func()

