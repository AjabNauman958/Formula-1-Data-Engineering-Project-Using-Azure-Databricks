# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake countainers for the project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):

    # get secrets from azure key vault
    client_id = dbutils.secrets.get(scope = "", key = "")
    tenant_id = dbutils.secrets.get(scope = "", key = "")
    client_secret = dbutils.secrets.get(scope = "", key = "")

    # Spark configuration for mounting ADLS
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # umount if already mounted
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # mount the storage container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("storageAccountName", "demo")

# COMMAND ----------

mount_adls("storageAccountName", "raw")

# COMMAND ----------

mount_adls("storageAccountName", "processed")

# COMMAND ----------

mount_adls("storageAccountName", "presentation")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/storageAccountName/"))

# COMMAND ----------

