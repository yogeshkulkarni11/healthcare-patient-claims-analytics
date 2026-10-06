# Data Dictionary

## Patients

| Column | Description |
|---|---|
| patient_id | Unique patient identifier |
| gender | Patient gender |
| age | Patient age |
| city | Patient city |
| insurance_type | Government or private insurance |
| chronic_condition | Known chronic condition |

## Claims

| Column | Description |
|---|---|
| claim_id | Unique claim identifier |
| patient_id | Patient reference |
| claim_date | Date of claim |
| provider | Healthcare provider |
| diagnosis | Diagnosis associated with claim |
| procedure | Procedure/service |
| claim_amount | Claim amount |
| claim_status | Paid, Approved or Rejected |
