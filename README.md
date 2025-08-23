Beside main ones I am using few additional notebooks:<br>
<br>
- configuration -> variables used in several notebooks<br>
- encryption_helper -> helper for encrypting and decrypting PII fields<br>
- transformation_helper -> helper for cleaning data in silver layer<br>
<br>
I'm also using databricks based secrets, for keepeing safely storage account's key:<br>
<img width="470" height="67" alt="476645359-b3d1c0bd-d96b-44d9-a295-5b9bf8f9438e" src="https://github.com/user-attachments/assets/2e781e3b-30e6-425e-9e10-31d462a4fd18" /><br>
_____

# Setup of infrastructure
1. Created a resource group with <br>
`az group create --name sparkhm3 --location polandcentral`<br>
2. Created storage account with<br>
`az storage account create --name sasparkhm3 --resource-group sparkhm3 --location polandcentral --sku Standard_LRS`<br>
3. Created storage container with<br>
`az storage container create --name contsparkhm3 --account-name sasparkhm3`<br>
4. Updated terraform configuration in main.tf<br>
5. Deployed infrastructure with<br>
`terraform init && terraform plan -out terraform.plan && terraform apply terraform.plan`<br>
6. From that point I worked mostly with Databricks GUI hosted by my Azure subscription. One more thing was setting secrets scope with databricks CLI<br>
`create-scope --scope <SCOPE_NAME>`
`databricks secrets put-secret --scope my-scope --key <STORAGE_ACCOUNT_KEY> --string-value "<KEY_VALUE>"`
`databricks secrets list-secrets --scope my-scope`
_____
# 01_create_metadata
Schemas are created:<br>
<img width="392" height="439" alt="image" src="https://github.com/user-attachments/assets/316acbbe-ed4d-4340-b693-b5d6aabc65e3" /><br>
# 02_load_bronze_data
Raw data with encrypted PII fields<br>
<img width="868" height="766" alt="image" src="https://github.com/user-attachments/assets/f4e4b9ed-a134-4ec5-8256-e48667e9c73e" /><br>
<img width="1846" height="783" alt="image" src="https://github.com/user-attachments/assets/36d78027-e771-4763-95ab-a06c0a669304" /><br>
<img width="1781" height="660" alt="image" src="https://github.com/user-attachments/assets/db627b1e-fab0-478a-a10c-59168475a788" /><br>
# 03_bronze_to_silver
Data with:<br>
- trimmed string values<br>
- columns casted to correct types<br>
- null values instead of empty ones<br>
- removed duplicates<br>
- encrypted data<br>
<img width="813" height="805" alt="image" src="https://github.com/user-attachments/assets/fda32029-6dde-43c5-8db1-060f0d1168ea" /><br>
<img width="1852" height="505" alt="image" src="https://github.com/user-attachments/assets/6f4d3dd2-cea9-42c1-bb8b-195937f48dbd" /><br>
<img width="1823" height="747" alt="image" src="https://github.com/user-attachments/assets/2acfd051-a7c5-4bf8-8e67-cd79fe24d721" /><br>
<img width="1870" height="792" alt="image" src="https://github.com/user-attachments/assets/9f2faff2-494d-4006-af30-b95a78cceaa7" /><br>
# 04_silver_to_gold
Data prepared for analisys:<br>
<img width="1049" height="784" alt="image" src="https://github.com/user-attachments/assets/cca4077e-5d45-4048-b16f-1a69b2028bb8" /><br>
<img width="1748" height="785" alt="image" src="https://github.com/user-attachments/assets/21b73a13-2026-4f2d-ac32-dfcdc98cdc3a" /><br>

# Checpoints:
<img width="1046" height="499" alt="image" src="https://github.com/user-attachments/assets/59b1fcd8-e3d9-4872-b82e-cc2542b1d8f1" />
<img width="883" height="603" alt="image" src="https://github.com/user-attachments/assets/bdf44b8d-85bd-4015-a54f-86defdd6a9cf" />


