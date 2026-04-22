# h360tk_ethiopia_medisoft


## Context

The goal of this development is to illustrate how we can integrate data from Medisoft into the Hearts360Tookit to generate dashboards easily

We are working from the data presented here (test data, containing no PII, publicly available):
- https://hearts360.medisoft.rw/data_sharing_export.php


## H360tk in action
### Starting the system

```
git pull git@github.com:simpledotorg/h360tk_ethiopia_medisoft.git
cd h360tk_ethiopia_medisoft
docker compose up -d
```

### Loading the data
```
 docker exec -it python_processing sh -c "bash /scripts/do_all.sh"
```

This script will 
- iterate on facilities and download relevant files (Patients, Blood pressures, Blood sugars)
- load that data into the h360tk database

### Accessing the dashboards
