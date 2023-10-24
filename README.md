# Sieve 

![Sieve Logo](images/logo.png)

SIEVE is a general purpose middleware to support access control in DBMS that enables them to scale query processing with very large number of access control policies. Full version of the paper can be seen at [arXiv](https://arxiv.org/abs/2004.07498). 


## Setup

1. Download [MySQL](https://dev.mysql.com/downloads/installer/) or [PostgreSQL](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)
2. Create a user "sieve" and grant privileges of DBA role.
3. Create a Schema.
4. Populate the schema found inside the data directory (data/wifi_dataset.tar.xz for MySQL and data/mall_dataset.tar.xz for PostgreSQL)
   1. Extracted files are sql dump files.
   2. Import the table definition (wifi_defn.sql/ mall_defn.sql)
   3. Import the data (wifi_data.sql/ mall_data.sql)
5. Update the sample.properties file with the DBMS properties inside the src/main/resources/credential.sample directory

## Usage

1. Set the dbms and table_name options in resources/config/general.properties
2. Set true for the experiments that you wish to run (Options: Query Performance, Policy Scale up)
3. Compile the code
```
mvn clean install

```
4. Execute it with
```
mvn exec:java 
```

## License
[Apache 2.0](https://choosealicense.com/licenses/apache-2.0/)

