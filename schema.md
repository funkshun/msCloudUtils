# msBWT Cloud Database Schema

## Abbreviated

### Tables

- datasets
  - id (integer primary key)
  - name (text)
  - group (text)
- hosts
  - id (integer primary key)
  - url (text)
- local
  - id (integer primary key)
  - direc (text)
- dataset_hosts
  - data_id (foreign key references datasets (id))
  - host_id (foreign key references hosts (id))
- dataset_local
  - data_id (foreign key references datasets (id))
  - local_id (foreign key references local (id))
  
  
