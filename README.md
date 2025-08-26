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

# Checkpoints:
<img width="1046" height="499" alt="image" src="https://github.com/user-attachments/assets/59b1fcd8-e3d9-4872-b82e-cc2542b1d8f1" /><br>
<img width="883" height="603" alt="image" src="https://github.com/user-attachments/assets/bdf44b8d-85bd-4015-a54f-86defdd6a9cf" /><br>

# Manually running notebooks:
01_create_metadata:<br>
<img width="1749" height="828" alt="image" src="https://github.com/user-attachments/assets/e7ae0aff-96c5-4ef1-aa1c-7fd7043aecec" /><br>
<img width="417" height="571" alt="image" src="https://github.com/user-attachments/assets/b882c1ea-6319-4860-915b-5c4e72b4b471" /><br>
_____
02_load_bronze_data: <br>
Data for 2016 only:<br>
<img width="1395" height="836" alt="image" src="https://github.com/user-attachments/assets/64b30bf7-df96-4756-8f36-3734c0f09ebd" /><br>
Data for both 2016 & 2017:<br>
<img width="1512" height="668" alt="image" src="https://github.com/user-attachments/assets/ceb6bf76-74f2-403c-92ea-b5935e80826b" /><br>
_____
03_bronze_to_silver: <br>
Data for 2016 only:<br>
<img width="775" height="509" alt="image" src="https://github.com/user-attachments/assets/7add3295-6fd6-464a-9422-b3f10f79ec81" /><br>
<img width="875" height="521" alt="image" src="https://github.com/user-attachments/assets/d62124a2-6f03-4953-b463-411a63201ebc" /><br>
Data for both 2016 & 2017:<br>
<img width="878" height="506" alt="image" src="https://github.com/user-attachments/assets/fbc94433-e672-42db-83ac-75d997a7ce22" /><br>
_____
04_silver_to_gold:<br>
Data for 2016 only:<br>
<img width="568" height="314" alt="image" src="https://github.com/user-attachments/assets/7c9ec054-0cb6-430a-bcdd-430be3552a1f" /><br>
<img width="1450" height="539" alt="image" src="https://github.com/user-attachments/assets/92c8be90-2256-46ec-ad79-0f43f8cf6525" /><br>

After that uploading data for 2017:<br>
<img width="1629" height="817" alt="image" src="https://github.com/user-attachments/assets/834df262-a097-4136-957b-d7a97b57802e" /><br>
Data for both 2016 & 2017:<br>
<img width="854" height="512" alt="image" src="https://github.com/user-attachments/assets/30ce2e46-f594-42c9-9ad7-4b9965b9bfcb" /><br>

# Visualizations:
Datasets for this dashboards have been created in notebook 05_build_dashboard_datasets.There are only a few of records for each, but I'm asumming that's just the data:<br>
<img width="1341" height="723" alt="image" src="https://github.com/user-attachments/assets/7ce0f3f1-5d0c-4a5f-9f06-90ae1f8a850e" /><br>
<img width="1348" height="712" alt="image" src="https://github.com/user-attachments/assets/f7d3ad10-a60c-41c4-b970-93f7386c44fc" /><br>
<img width="1371" height="735" alt="image" src="https://github.com/user-attachments/assets/f3067636-3b5f-456a-92d6-f64aaa0cc283" /><br>
<img width="1328" height="731" alt="image" src="https://github.com/user-attachments/assets/c73852ea-7de3-452e-bbaf-a4040cf6a1e9" /><br>
<img width="1369" height="713" alt="image" src="https://github.com/user-attachments/assets/0caf688b-9bbd-40db-8633-8702fd459758" /><br>



