# odap_framework_demo

## How to use it

### Step1

Clone this repo to Databricks Workspace of your liking.

### Step 2

Add environment variables to your Databricks cluster

```bash
READ_ENV=dev
WRITE_ENV=dev
```

### Step3

In Databricks run `_demo/_setup/init` notebook which will create raw example tables (customer, card_transactions, web_visits).

### Step4

Run `orchestrator` notebook. It will create `odap_features.customer` table with features defined in `features/` folder.

### Step5

Check the segment `segments/customer_account_investment_interest` by running this notebook.
