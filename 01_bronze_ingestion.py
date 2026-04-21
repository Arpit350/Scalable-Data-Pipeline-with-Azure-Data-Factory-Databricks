-- Create storage credential (done via UI or Terraform typically, but can verify)
SHOW STORAGE CREDENTIALS;

-- Create external location pointing to your ADLS container
CREATE EXTERNAL LOCATION IF NOT EXISTS my_adls_location
  URL 'abfss://<container>@<storage_account>.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL <your_storage_credential>);

-- Verify access
SHOW EXTERNAL LOCATIONS;
