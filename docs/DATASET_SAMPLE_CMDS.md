# VM INSTANCE  
```
su - dis
mkdir data-ingestion-sample
cd data-ingestion-sample
```

## Lấy mẫu MIMIC-IV Hosp
```
hdfs dfs -cat data/raw_data/mimic/hosp/admissions.csv | head -n 100 > admissions.csv
hdfs dfs -cat data/raw_data/mimic/hosp/patients.csv | head -n 100 > patients.csv
hdfs dfs -cat data/raw_data/mimic/hosp/diagnoses_icd.csv | head -n 100 > diagnoses_icd.csv
hdfs dfs -cat data/raw_data/mimic/hosp/labevents.csv | head -n 100 > labevents.csv
```

## Lấy mẫu MIMIC-IV (ICU)
```
hdfs dfs -cat data/raw_data/mimic/icu/d_items.csv | head -n 100 > d_items.csv
```

## Lấy mẫu eICU
```
hdfs dfs -cat data/raw_data/eICU/patient.csv | head -n 100 > patient.csv
hdfs dfs -cat data/raw_data/eICU/vitalPeriodic.csv | head -n 100 > vitalPeriodic.csv
hdfs dfs -cat data/raw_data/eICU/diagnosis.csv | head -n 100 > diagnosis.csv
hdfs dfs -cat data/raw_data/eICU/medication.csv | head -n 100 > medication.csv
```

# LOCAL
## Tải dữ liệu MIMIC-IV Hosp
```
gcloud compute scp dis@bigdata2:/home/dis/data-ingestion-sample/admissions.csv data/raw/hosp/ --project=bigdata-490002 --zone=asia-southeast1-b
gcloud compute scp dis@bigdata2:/home/dis/data-ingestion-sample/patients.csv data/raw/hosp/ --project=bigdata-490002 --zone=asia-southeast1-b
gcloud compute scp dis@bigdata2:/home/dis/data-ingestion-sample/diagnoses_icd.csv data/raw/hosp/ --project=bigdata-490002 --zone=asia-southeast1-b
gcloud compute scp dis@bigdata2:/home/dis/data-ingestion-sample/labevents.csv data/raw/hosp/ --project=bigdata-490002 --zone=asia-southeast1-b
## Tải dữ liệu MIMIC-IV ICU
gcloud compute scp dis@bigdata2:/home/dis/data-ingestion-sample/d_items.csv data/raw/icu/ --project=bigdata-490002 --zone=asia-southeast1-b
## Tải dữ liệu eICU
gcloud compute scp dis@bigdata2:/home/dis/data-ingestion-sample/patient.csv data/raw/ --project=bigdata-490002 --zone=asia-southeast1-b
gcloud compute scp dis@bigdata2:/home/dis/data-ingestion-sample/vitalPeriodic.csv data/raw/ --project=bigdata-490002 --zone=asia-southeast1-b
gcloud compute scp dis@bigdata2:/home/dis/data-ingestion-sample/diagnosis.csv data/raw/ --project=bigdata-490002 --zone=asia-southeast1-b
gcloud compute scp dis@bigdata2:/home/dis/data-ingestion-sample/medication.csv data/raw/ --project=bigdata-490002 --zone=asia-southeast1-b
## Tải dữ liệu Note
gcloud compute scp dis@bigdata2:/home/dis/data-ingestion-sample/discharge.csv data/raw/ --project=bigdata-490002 --zone=asia-southeast1-b
```
