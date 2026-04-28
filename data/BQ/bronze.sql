-- Description: Create external tables for bronze dataset in BigQuery

CREATE EXTERNAL TABLE IF NOT EXISTS `project-00e61840-f4f1-4e1d-ac8.bronze_dataset.departments_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-24/landing/hosp-a/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-00e61840-f4f1-4e1d-ac8.bronze_dataset.encounters_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-24/landing/hosp-a/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-00e61840-f4f1-4e1d-ac8.bronze_dataset.patients_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-24/landing/hosp-a/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-00e61840-f4f1-4e1d-ac8.bronze_dataset.providers_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-24/landing/hosp-a/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-00e61840-f4f1-4e1d-ac8.bronze_dataset.transactions_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-24/landing/hosp-a/transactions/*.json']
);

---------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS `project-00e61840-f4f1-4e1d-ac8.bronze_dataset.departments_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-24/landing/hosp-b/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-00e61840-f4f1-4e1d-ac8.bronze_dataset.encounters_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-24/landing/hosp-b/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-00e61840-f4f1-4e1d-ac8.bronze_dataset.patients_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-24/landing/hosp-b/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-00e61840-f4f1-4e1d-ac8.bronze_dataset.providers_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-24/landing/hosp-b/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-00e61840-f4f1-4e1d-ac8.bronze_dataset.transactions_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-24/landing/hosp-b/transactions/*.json']
);

---------------------------------------------------------------------------------------------------------------------------