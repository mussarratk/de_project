# de_project
## DP-750
---

<details>
    

# RIDESHARE PROJECT - 

- Ingestion Design
- Bronze Layer - PySpark - All Table
- Silver Layer - LDP - Lakeflow Declarative Pipeline - Trips Table
- Gold Layer - OBT append - LDP  - Trips Table for joins add from dimension table
- Star Schema - Dimensional Data Model - All facts and dims


<img width="1303" height="634" alt="image" src="https://github.com/user-attachments/assets/f5484741-d324-4751-b2b9-0c0bb022680e" />

# Project
  
<img width="562" height="577" alt="image" src="https://github.com/user-attachments/assets/d18dc42e-9b10-44a6-b621-882d89844b94" />

<img width="1156" height="434" alt="image" src="https://github.com/user-attachments/assets/60e0c00c-afcc-4a09-8531-f45e0f181a11" />

- External Location to save the table

<img width="1309" height="431" alt="image" src="https://github.com/user-attachments/assets/2055df33-74ae-4590-9cb0-eb8418898baa" />
<img width="1342" height="421" alt="image" src="https://github.com/user-attachments/assets/1a18d510-1817-4364-894d-b8f80196b4e1" />

# Bronze Layer 

<img width="1364" height="596" alt="image" src="https://github.com/user-attachments/assets/6915fb8a-d5b0-4311-b75e-c4063f10234c" />
<img width="1291" height="453" alt="image" src="https://github.com/user-attachments/assets/0ada2898-234f-4d72-9360-d4105c0fcacc" />


## Pipeline - LDP - Fully Managed by Databricks
- Use Autoloader - drag everything
- No Need Checkpoint Location and Schema Location - Auto done

<img width="1325" height="517" alt="image" src="https://github.com/user-attachments/assets/cf9327f4-61d7-4546-ba78-9909ca78647a" />
<img width="1325" height="475" alt="image" src="https://github.com/user-attachments/assets/b56d01a9-b39c-4f93-b9ea-b6aede4d91fd" />

<img width="978" height="400" alt="image" src="https://github.com/user-attachments/assets/7143664f-a547-484b-a5cc-fa298c7e899d" />

##
explore
<img width="900" height="574" alt="image" src="https://github.com/user-attachments/assets/426a8f62-2451-4962-8997-6a34330feb4d" />


## Silver
<img width="1314" height="586" alt="image" src="https://github.com/user-attachments/assets/9193f0b8-2636-458a-94c0-6972af56f30d" />
<img width="1343" height="460" alt="image" src="https://github.com/user-attachments/assets/41beb9c8-fd72-40c4-b4ea-2540ee0abadb" />

## OBT_Gold

<img width="1300" height="590" alt="image" src="https://github.com/user-attachments/assets/049326a1-cbbd-4d7f-8c89-17a249f19447" />
<img width="1313" height="625" alt="image" src="https://github.com/user-attachments/assets/8e816608-f441-44d5-bcd7-a5ad425e2fe6" />
<img width="1333" height="441" alt="image" src="https://github.com/user-attachments/assets/201869b9-eb2f-4889-a177-d281c98fa7a0" />

- exploration

<img width="1009" height="452" alt="image" src="https://github.com/user-attachments/assets/3c595757-79a6-4a1e-b4eb-65c0c2b10c0c" />
<img width="1262" height="216" alt="image" src="https://github.com/user-attachments/assets/15612231-2635-4f3e-85c0-c6f661c5feb0" />
<img width="1296" height="550" alt="image" src="https://github.com/user-attachments/assets/02ec63ed-3cf9-43e0-99ee-efc4e93c4c0b" />


- Stateful transformation like join with the streaming data (state is not persisted) apply this watermark in data - treat this data - process late arrival data 10 mins (come within 10 mins not process the data - Spark Structured Streaming handle all the data which is compatible) data depending on our need and do not keep this data in your state before that. 


## Star Schema - SCD - DIM Stagging View - Dims tables
- In stagging layer we decide what columns we need for our dims.py file as OBT has all (either can create view or table ) here incremental streaming view (recommended to create view - no need store the stagging) - It is only select no transformation
  
<img width="1361" height="575" alt="image" src="https://github.com/user-attachments/assets/0172942d-7e4a-4b1a-973b-634d07bd7d86" />

<img width="1312" height="630" alt="image" src="https://github.com/user-attachments/assets/143dcfff-8635-4ba0-850e-6bd2a6cd3008" />


- external context dim and added internal context fact table some dim columns
  
<img width="1349" height="574" alt="image" src="https://github.com/user-attachments/assets/07fdae3c-0703-471d-92df-81dfa5f9ade0" />

<img width="1303" height="634" alt="image" src="https://github.com/user-attachments/assets/084dbaa8-ad98-40d9-95ea-a08e88ff55ec" />

<img width="1292" height="575" alt="image" src="https://github.com/user-attachments/assets/37195605-aba4-4549-a014-060850a312ca" />

## Fact Tables

<img width="1307" height="594" alt="image" src="https://github.com/user-attachments/assets/56536b60-13cc-42cc-92b2-c039481632bf" />
<img width="1247" height="536" alt="image" src="https://github.com/user-attachments/assets/1d6e0c5b-f087-46e7-b4a0-5a4a3c46d6d3" />

- fetching the data used join fact n cust dim

<img width="1252" height="582" alt="image" src="https://github.com/user-attachments/assets/cc1a747d-2fee-438d-8659-3800e083fd76" />

- ## Incremental Update

<img width="560" height="433" alt="image" src="https://github.com/user-attachments/assets/a66101b3-94c3-4d93-bb5b-aae7439335b7" />

<img width="1258" height="518" alt="image" src="https://github.com/user-attachments/assets/5e84879e-e27c-42d7-a5d7-726f95c967cd" />

<img width="1026" height="385" alt="image" src="https://github.com/user-attachments/assets/c3c3acbe-9c16-4e91-8bf8-de402e853807" />
<img width="1256" height="525" alt="image" src="https://github.com/user-attachments/assets/7d1bae1f-064b-4721-beb2-ee36d606dc3e" />

-- updated in streaming ingestion -- for customer id 2








</details>


---


<details>


## AZURE SETUP

<img width="1361" height="545" alt="image" src="https://github.com/user-attachments/assets/0befb2f6-8e80-4654-914d-a77d75782134" />

<img width="609" height="617" alt="image" src="https://github.com/user-attachments/assets/f5464fac-a868-4563-9f4a-4ec7ad4bebfd" />
<img width="1354" height="546" alt="image" src="https://github.com/user-attachments/assets/b45904b0-82a7-4e48-be0c-4fecfde8d48d" />
<img width="1359" height="611" alt="image" src="https://github.com/user-attachments/assets/c2915952-3388-418c-b513-af16283d93df" />
<img width="1043" height="593" alt="image" src="https://github.com/user-attachments/assets/616ab8c2-6a18-408c-a2ac-a18fee7f4441" />
<img width="1340" height="594" alt="image" src="https://github.com/user-attachments/assets/6ef4380b-f648-4c37-b292-7c3c6e40b81a" />
<img width="1358" height="444" alt="image" src="https://github.com/user-attachments/assets/dc069395-2047-4085-a81d-2aa326313383" />


<details>
  

----

## DATABRICKS SETUP

- from account console - Set UP - user management - with admin account

  <img width="1355" height="599" alt="image" src="https://github.com/user-attachments/assets/e987f0dc-7aff-4f54-a96a-7e17f1f397a3" />
<img width="1354" height="482" alt="image" src="https://github.com/user-attachments/assets/b0301c8e-fa1f-4ef7-989c-d018a71276d0" />
<img width="979" height="185" alt="image" src="https://github.com/user-attachments/assets/c317c424-0256-4935-8a25-f0288e5dae95" />
<img width="1350" height="567" alt="image" src="https://github.com/user-attachments/assets/8b1897fe-d277-4bad-a6ce-a2a5b08b6b94" />
<img width="1359" height="455" alt="image" src="https://github.com/user-attachments/assets/994c3114-c795-4e69-8296-6fad4d303e7b" />

- New metastore created with adls path and access connector after role assignments

- ## EXTERNAL LOCATIONS

<img width="1332" height="590" alt="image" src="https://github.com/user-attachments/assets/4736df9f-e26a-49e3-ad8f-69ee8d4967c8" />
<img width="1322" height="570" alt="image" src="https://github.com/user-attachments/assets/34415d5e-2fb7-4b00-9a24-2e3ac5a7643d" />
<img width="1304" height="520" alt="image" src="https://github.com/user-attachments/assets/4389d7f1-1825-4209-907c-da62aba9abd8" />
<img width="1324" height="542" alt="image" src="https://github.com/user-attachments/assets/9de0b1e1-a423-48f3-906c-ee22c4aeced1" />

</details>


# Compute



# 




</details>













