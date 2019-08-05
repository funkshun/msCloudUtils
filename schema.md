# msBWT Cloud Database Schema

## Abbreviated

### Tables

- datasets
  - id (integer primary key)
  - name (text)
  - grouping (text)
- hosts
  - id (integer primary key)
  - url (text)
- local
  - id (integer primary key)
  - direc (text)
  - grouping (text)
- dataset_hosts
  - id (integer primary key)
  - data_id (foreign key references datasets (id))
  - host_id (foreign key references hosts (id))
- dataset_local
  - id (integer primary key)
  - data_id (foreign key references datasets (id))
  - local_id (foreign key references local (id))
  