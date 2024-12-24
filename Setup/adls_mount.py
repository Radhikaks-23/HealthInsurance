# Databricks notebook source
# Databricks notebook source
storageAccountName = "datalakehousegen23"
storageAccountAccessKey = dbutils.secrets.get('tt-hc-kv', 'tt-adls-access-key-dev')
mountPoints=["landing","configs"]
for mountPoint in mountPoints:
    if not any(mount.mountPoint == f"/mnt/datalakehousegen23/{mountPoint}" for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
            source = 'abfss://{}@{}.dfs.core.windows.net/'.format(mountPoint, storageAccountName),
            mount_point = f"/mnt/datalakehousegen23/{mountPoint}",
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.dfs.core.windows.net': storageAccountAccessKey}
            )
            print(f"{mountPoint} mount succeeded!")
        except Exception as e:
            print("mount exception", e)

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.mounts())