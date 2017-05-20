# jdchive

An idempotent tool to easily create and maintain Hive table.

When deploying an application in the Hadoop world, a common issue is to create the required Hive table.

This is usually achieved using scripts. But, this can quickly become cumbersome and hard to maintain. And a nightmare when it come to updating a running application.

'jdc...' stand for 'Just DesCribe'. You define all the Hive database and table of your application in a simple YAML file and **jdchive** will take care of deploying all theses object on your cluster.

**jdchive** is a convergence tool. Its aim is to align the real configuration to what is described on the source file, while applying only strictly necessary modification.

This make **jdchive** a fully idempotent tool, as all modern DevOps tools should be.

Regarding schema evolution, **jdchive** will do its best to perform required modification using ALTER commands. For most involving evolution, requiring data manipulation, jdchive will act as an innovating Database Migration framework.

## Usage

**jdchive** is provided as rpm packages (Sorry, only this packaging is currently provided. Contribution welcome), on the [release pages](https://github.com/BROADSoftware/jdchive/releases).

**jdchive** MUST be used on properly configured Hadoop client node. (i.e `hive` shell must be functional)

Once installed, usage is the following:

```bash
# jdchive --inputFile yourDescription.yml
```
 
Where `yourDescription.yml` is a file containing your target Hive database and table description. **jdchive** will then perform all required operation to reach this target state.

Note than if `yourDescription.yml` content match the current configuration, no operation will be performed.

Here is a sample of such `description.yml` file:

```yaml
databases:
- name: jdctest1
  comment: "For jdchive table testing"
    
tables:
- name: testSimple
  database: jdctest1
  comment: "A first, simple test table"
  fields:
  - name: fname
    type: string
    comment: "First name"
  - name: lname
    type: string
    comment: "Last name"
```

Of course, database and table description may include several other attributes, to represent most of the option on Hive table creation. This is described later in this document.  

### Other launch option

When launching the **jdchive** command you may provide some optional command line options:


| Option| Description | 
| --- | --- |
|`--configFile`| This parameter allow an Hadoop properties configuration file to be added to the default set. This parameters can occur several times on the command line|
|`--defaultState`|`present` or `absent`. This parameter allow setting of all `state` value which are not explicitly defined. |
|`--dryRun`|This switch will prevent **jdchive** to perform any effective operation.|
|`--dumpConfigFile`|For Debuging purpose: All Hadoop Configuration will be dumped in the provided file parameter|
|`--keytab`| This parameter specify a keytab for Kerberos authentication. If present, `--principal` parameter must also be defined.|
|`--principal`| This parameter specify a principal for Kerberos authentication. If present, `--keytab` parameter must also be defined.|

## Database definition

> To have a good understanding of all Hive database option, please refer to the [Hive DDL documentation](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL).  

Here is a description of all attributes for a Database definition.

| Attribute | Req. | Description |
| ---: | :---: | --- |
|name        |yes|The name of the database|
| properties |no | A map of properties. Equivalent to WITH DBPROPERTIES Hive DDL clause|
| location   |no | Equivalent to LOCATION Hive DDL clause|
| owner      |no | The owner of the database. May be a user account or a group.|
| owner_type |no | USER or ROLE. Specify what represent the owner attribute|
| comment    |no | Equivalent to the COMMENT Hive DDL close|
| state      |no | `present`or `absent`. Define the target state to reach for this database. Refer to **Table and database deletion**|

### Ownership

Database owner can be explicitly set by the attribute defined above. If this attribute is not present, then the owner will be the account launching the command.

Once created, one may change owner by setting the corresponding attribute. Launching **jdchive** under another account will have no impact.

### Example:

```yaml
databases:
- name: jdctest1
```
Will internally generate the command:

```sql
CREATE DATABASE jdctest1
```
And

```yaml
databases:
- name: jdctest2
  location: /user/apphive/db/testtables1
  owner_type: USER
  owner: apphive
  comment: "For jdchive table testing"
```
Will internally generate the commands:

```sql
CREATE DATABASE jdctest2 COMMENT 'For jdchive table testing' LOCATION 'hdfs://clusterid/user/apphive/db/testtables1'
ALTER DATABASE jdctest2 SET OWNER USER apphive
```

## Table definition

> To have a good understanding of all Hive table option, please refer to the [Hive DDL documentation](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL).  

Here is a description of all attributes for a Database definition.

| Attribute       | Req. | Description |
| ---:            | :---: | --- |
|name             |yes|The name of the table|
|database         |yes|The database this table belong to|
|external         |no |Boolean. Equivalent to the EXTERNAL Hive DDL clause|
|fields           |no |A list of fields definition describing the table's column. Refer to **Field definition** below|
|comment          |no |An optional comment associated to the table|
|location         |no |Equivalent to the LOCATION Hive DDL clause|
|properties       |no |A map of properties. Equivalent to TBLPROPERTIES Hive DDL clause|
|stored_as        |no |Specify the file format, such as SEQUENCEFILE, TEXTEFILE, RCFILE, ....<br/>Equivalent to STORED AS Hive DDL clause [1]|
|input_format     |no |Equivalent to STORED AS INPUT FORMAT '....' Hive DDL clause [1][2]|
|output_format    |no |Equivalent to STORED AS OUTPUT FORMAT '....' Hive DDL clause [1][2]|
|delimited        |no |A map of delimiter character. Equivalent to ROW FORMAT DELIMITED Hive DDL clause. Refer to **Delimited row format** below [3]|
|serde            |no |Allow explicit definition of a `serde`'. Equivalent to ROW FORMAT SERDE Hive DDL clause [3]|
|serde_properties |no |A map of properties associated to the `serde`. Equivalent to WITH SERDEPROPERTIES Hive DDL clause|
|storage_handler  |no |Allow definition of the storage handler. Equivalent to STORED BY Hive DDL clause|
|partitions       |no |A list of fields definition describing the table's partitioning. Refer to **Field definition** below|
|clustered_by     |no |Allow definition of a CLUSTERED BY Hive DDL clause. Refer to **Table clustering** below|
|skewed_by        |no |Allow definition of a SKEWED BY Hive DDL Clause. Refer to **Skewed values**|
|alterable        |no |Boolean. Default `no`. Allow most of ALTER TABLE commands to be automatically issued for table modification. Refer to **Altering table** below|
|droppable        |no |Boolean. Allow this table to be dropped and recreated if definition is modified. Default value is `yes` if the table is external, `no`for all other cases|
|state            |no |`present`or `absent`. Define the target state to reach for this table. Refer to **Table and database deletion**|

[1]: Storage format can be defined using two methods:

* Use `stored_by`. This will define implicitly `input_format`, `output_format` and, for some value the `serde`.
* Explictly define `input_format`, `output_format` and eventually `serde`.

The two approaches are exclusive. Defining both `stored_by` and `input/output_format` will generate an error.

[2] `input_format` and `output_format` must be defined both if used.

[3] `delimited` and `serde` are exclusive and can't be defined both.

### Altering table

The must tricky operation with tools like **jdchive** is not table creation, but how it must behave on existing table evolution, specially if theses table already contains some data. 

In case of table schema update, operation can be classified in several categories:

* Modification which can be performed by issuing one or several ALTER TABLE command and which are independent of data layout. For example, changing a comment. These operations are automatically performed.

* Modification which can be performed by issuing one or several ALTER TABLE command, but may introduce a shift between the effective stored data and the new table definition definition. Such operation need to be allowed by setting the `alterable`flag to yes. 

> Most if not all ALTER TABLE commands will only modify Hive's metadata, and will not modify data. Users should make sure the actual data layout of the table/partition conforms with the new metadata definition.

* Modification which can't be performed as too complex or there is no corresponding ALTER TABLE command. Such operation should be performed by an external, user defined script. Refer to **Database migration** for more information.

* Modification which occurs on table which can be freely dropped without deleting the data. This is the case for example for EXTERNAL tables. In such case, the table is dropped and recreated in case of schema modification. This can be controlled using the `droppable` flag.

### Field definition:

Here is the definition of a `field` element:

| Field Attribute | Req. | Description |
| ---:  | :---: | --- |
|name   |yes|The name of the field|
|type   |yes|The type of the field|
|comment|no |An optional comment|

### Delimited row format 

The `delimited` map can hold the following values:

```yaml
fields_terminated_by:
fields_escaped_by:
collection_items_terminated_by:
map_keys_terminated_by:
lines_terminated_by:
null_defined_as:
``` 

The characters must be expressed between single quote, and can be a regular character, an usual backslash escape sequence, or a unicode value. For example:

```yaml
...
delimited:
  fields_terminated_by: ','
  map_keys_terminated_by: '\u0009'  # Same as '\t'
  lines_terminated_by: '\n'
  null_defined_as: '\u0001'
``` 
> using octal notation (i.e. '\001') is not supported.

### Table clustering

Here is the definition of a `clustered_by` element:

| Attribute  | Req. | Description |
| ---:       | :---: | --- |
|columns     |yes|This list of columns to CLUSTER BY |
|nbr_buckets |yes|The number of buckets|
|sorted_by   |no |A list of sort item element, as defined just below|

Inner sort item element:

|Attribute | Req. | Description |
| ---:     | :---: | --- |
|columns   |yes|A list of column |
|direction |no |The direction: `ASC` or `DESC`. Default is `ASC`|

Example:

```yaml
  ...
  clustered_by:
    columns:
    - userid
    - page_url
    sorted_by:
    - { column: userid, direction: asc }
    - { column: page_url, direction: desc }
    nbr_buckets: 16
```

Will be interpreted as:

```sql
 CLUSTERED BY(userid,page_url) SORTED BY (userid asc, page_url desc) INTO 16 BUCKETS
```

### Skewed values

Here is the definition of the `skewed_by` element:

| Attribute | Req. | Description |
| ---: | :---: | --- |
|columns                |yes|A list of column |
|values                 |yes|A list of list of values|
|stored\_as_directories |no |Boolean. Is skewed value stored as directory. Default `no`|

Example:

```yaml
  ...
  skewed_by:
    columns:
    - key
    values:
    - [ 1 ]
    - [ 5 ]
    - [ 6 ]
    stored_as_directories: true
```

will be interpreted as:

```sql
SKEWED BY(key) ON(('1'),('5'),('6')) STORED AS DIRECTORIES
```

And:

```yaml
  skewed_by:
    columns:
    - col1
    - col2
    values:
    - [ "s1", 1 ]
    - [ "s3", 3 ]
    - [ "s13", 13 ]
    - [ "s78", 78 ]
```

will be interpreted as:

```sql
SKEWED BY(col1,col2) ON(('s1', 1),('s3', 3),('s13', 13),('s78',78))
```

### Table ownership

As there is no command such as 'ALTER TABLE SET USER...' the database owner will be the account under which the `jdchive` command was launched during database creation.

Once created, there is no way to change table ownership.

### Table creation examples

```yaml
tables:
- name: testSimple
  database: jdctest1
  comment: "A first, simple test table"
  location: "/tmp/xxx"
  fields:
  - name: fname
    type: string
    comment: "First name"
  - name: lname
    type: string
    comment: "Last name"
- name: testRcFile
  database: jdctest1
  comment: "A RCFILE table"
  fields: [ { name: fname, type: string, comment: 'The first name' }, { name: lname, type: string } ]
  stored_as: RCFILE
````

Will be interpreted, for creation as:

```sql
CREATE  TABLE jdctest1.testSimple ( fname string COMMENT 'First name', lname string COMMENT 'Last name' ) COMMENT 'A first, simple test table' LOCATION 'hdfs://mycluster/tmp/xxx'
CREATE  TABLE jdctest1.testRcFile ( fname string COMMENT 'The first name', lname string ) COMMENT 'A RCFILE table' STORED AS RCFILE
```

```yaml
tables:
- name: testPartitions
  database: jdctest1
  comment: "A table with partitions"
  fields:
  - name: viewTime
    type: INT
  - name: userid
    type: BIGINT
  - name: page_url
    type: STRING
  - name: referrer_url
    type: STRING
  - name: ip
    type: STRING
    comment: "IP Address of the User"
  partitions:
  - name: dt
    type: STRING
  - name: country
    type: STRING
  stored_as: SEQUENCEFILE
  state: present
````

Will be interpreted, for creation as:

```sql
CREATE  TABLE jdctest1.testPartitions ( viewTime INT, userid BIGINT, page_url STRING, referrer_url STRING, ip STRING COMMENT 'IP Address of the User' ) COMMENT 'A table with partitions' PARTITIONED BY ( dt STRING, country STRING ) STORED AS SEQUENCEFILE

```

```yaml
tables:
- name: testSerde
  database: jdctest1
  comment: "Serde test"
  fields:
  - { name: host, type: STRING }
  - { name: identity, type: STRING }
  - { name: theuser, type: STRING }
  serde: "org.apache.hadoop.hive.serde2.RegexSerDe"
  serde_properties:
    input.regex: "([^ ]*) ([^ ]*) ([^ ]*)"
  state: present
  alterable: true
````

Will be interpreted, for creation as:

```sql
CREATE  TABLE jdctest1.testSerde ( host STRING, identity STRING, theuser STRING) COMMENT 'Serde test' ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES ( 'input.regex'='([^ ]*) ([^ ]*) ([^ ]*)')
```

```yaml
# A mapping on Hbase table (External)    
table:
- name: testHBase
  database: jdctest1
  fields:
  - { name: rowkey, type: string, comment: "The row key" }
  - { name: number, type: int }
  - { name: prefix, type: string }
  - { name: fname, type: string }
  - { name: lname, type: string }
  - { name: company, type: string }
  - { name: title, type: string }
  external: true
  storage_handler: "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
  properties:
    hbase.table.name: "test2:test2a"
  serde_properties:
    hbase.columns.mapping: ":key,id:reg,id:prefix,id:fname,id:lname,job:cpny,job:title"
  state: present
````

Will be interpreted, for creation as:

```sql
CREATE EXTERNAL TABLE jdctest1.testHBase ( rowkey string COMMENT 'The row key', number int, prefix string, fname string, lname string, company string, title string ) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ( 'hbase.columns.mapping'=':key,id:reg,id:prefix,id:fname,id:lname,job:cpny,job:title') TBLPROPERTIES ( 'hbase.table.name'='test2:test2a')
```

## Table and database deletion

All databases or tables not described in the `description.yml` file will be left untouched.

To allow deletion to be performed, theses objects got a `state:` attribute. When not defined, it default to  `present`, or to the value provided by the `--defaultState` parameter. But it could be set to `absent` to trigger the deletion of the corresponding entity.

For example: 

```yaml

databases:
- name: jdctest1
  state: absent
    
tables:
- name: testSimple
  database: jdctest1
  state: absent
```

Will remove all object created by our previous example.

> Note, as a security, no cascading deletion from database to table will be performed. Deletion of a database can only be effective if all hosted table are explicitly deleted. 

## Logs

In case of problem, generated log messages can be helpful. 

**jdchive** will generate such message in the file `/tmp/jdchive-<user>.log`

The reason logs file is not stored in `/var/log` is it will be generally executed as a regular user, and, as such, will not have write access in `/var/log`

This is for the same reason the file name include the user name.

All this can easily be modified by editing the `/usr/bin/jdchive` launch script.

## Advanced configuration

After installation on the target system, **jdchive** files layout is the following:

```bash
/opt/jdchive/jdchive_uber-X.X.X.jar
/usr/bin/jdchive
/etc/jdchive/log4j.xml
/etc/jdchive/setenv.sh
```

**jdchive** behavior can be configured by altering the `setenv.sh` file:

```bash
# Set JAVA_HOME. Must be at least 1.7.
# If not set, will try to lookup a correct version.
# JAVA_HOME=/some/place/where/to/find/java

# Set the log configuration file
JOPTS="$JOPTS -Dlog4j.configuration=file:/etc/jdchive/log4j.xml"

# You may have to edit this line to adjust to your environement. Provide a space separated list of folders.
# The launching script will try to use one of these folder to find the Hive jars.
CANDIDATE_HIVE_LIBS="/usr/hdp/current/hive-client/lib"

# We need to explicitly add the hive-site config file. Location may vary per configuration
HIVE_CONFIG_FILE='/etc/hive/conf/hive-site.xml'
OPTS="$OPTS --configFile $HIVE_CONFIG_FILE"

# Dump configuration, for debugging
#OPTS="$OPTS --dumpConfigFile /tmp/jdchive-conf.txt"

# Report performed opération
OPTS="$OPTS --reportFile /tmp/jdchive-report-$(date +%y%m%d-%H%M%S).yml"

# Set kerberos principal and keytab (If kerberos is activated and you don't like 'kinit')
# OPTS="$OPTS --principal hiveUser --keytab /home/hiveUser/.keytabs/hiveUser.keytab
```

In this file, you can for example notice the parameter `--reportFile` for report file generation with the date-time pattern.

Of course, you can also modify the log4j.xml configuration file according to your preferences.
          
## Kerberos secured cluster

In the case your Hadoop cluster is protected by Kerberos, you have two methods to provide authentication.

* Using the `--principal` and `--keytab` parameters.

* Issue a `kinit` command before launching **jdchive**. (You then can check your ticket with the `klist` command).

In both case, the operation will be performed on behalf of the owner of the ticket. Ensure this user has got sufficient access privileges for Hive.

This also means created Database and Table will belong to the principal owner of the ticket.

## Ansible integration

With its idempotence property, **jdchive** is very easy to be orchestrated by usual DevOps tools like Chef, Puppet or Ansible.

You will find an Ansible role [at this location](http://github.com/BROADSoftware/bsx-roles/tree/master/hadoop/jdchive).

This role can be used as following;

```yaml	
- hosts: edge_node1
  vars:
    jdchive_rpm_url: https://github.com/BROADSoftware/jdchive/releases/download/v0.1.0/jdchive-0.1.0-1.noarch.rpm  
    jdchive_user: "apphive"
    jdchive_description:
      databases:
      - name: jdctest1
        comment: "For jdchive table testing"
      tables:
      - name: testSimple
        database: jdctest1
        comment: "A first, simple test table"
        fields:
        - name: fname
          type: string
          comment: "First name"
        - name: lname
          type: string
          comment: "Last name"
  roles:
  - hadoop/jdchive
```
  
### Kerberos support

This role can also support kerberos. For this, one need just to add a `jdchive_keytab` variable.

```yaml
- hosts: edge_node1
  vars:
    jdchive_rpm_url: https://github.com/BROADSoftware/jdchive/releases/download/v0.1.0/jdchive-0.1.0-1.noarch.rpm  
    jdchive_user: "apphive"
    jdchive_keytab: /etc/security/keytabs/apphive.keytab
    jdchive_description:
      databases:
      - name: jdctest1
    ...
```

The kerberos principal will be the value of `jdchive_user`

## Report file

On each run, **jdchive** generate a report file in YAML format, under the name `/tmp/jdchive-report-YYMMDD-HHMMSS.yml`.

Here is a sample of such report:

```yaml
---
done:
  commands:
  - "ALTER TABLE jdctest1.testsimple CHANGE COLUMN lname lname string COMMENT 'Last name'"
  - "ALTER TABLE jdctest1.testsimple SET TBLPROPERTIES ('comment' = 'A first, simple test table')"
todo:
  tableMigrations:
  - name: "testsimple"
    database: "jdctest1"
    existing:
      external: false
      fields:
      - name: "fname"
        type: "string"
        comment: "First name"
      - name: "lname"
        type: "string"
        comment: "Last name + MODIF"
      - name: "title"
        type: "string"
        comment: "Added field"
      - name: "age"
        type: "int"
      comment: "A first, simple test table + MODIF"
      location: "hdfs://mycluster/tmp/xxx"
      properties:
        last_modified_time: "1492763318"
        totalSize: "0"
        numFiles: "0"
        transient_lastDdlTime: "1492763317"
        comment: "A first, simple test table + MODIF"
        last_modified_by: "sa"
      input_format: "org.apache.hadoop.mapred.TextInputFormat"
      output_format: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
      serde: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      serde_properties:
        serialization.format: "1"
      partitions: []
    target:
      external: false
      fields:
      - name: "fname"
        type: "string"
        comment: "First name"
      - name: "lname"
        type: "string"
        comment: "Last name"
      comment: "A first, simple test table"
      location: "hdfs://mycluster/tmp/xxx"
      properties: {}
      stored_as: "TEXTFILE"
      input_format: "org.apache.hadoop.mapred.TextInputFormat"
      output_format: "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"
      serde_properties: {}
      partitions: []
      alterable: false
      droppable: false
      state: "present"
    diff:
      fields:
      - name: "title"
        type: "string"
        comment: "Added field"
      - name: "age"
        type: "int"
    existingFingerprint: 8250757764985273983
    targetFingerprint: 6718616937512399292
    diffFingerprint: 4205480441032907571  
```

The first section (`done.commands`) list the command which was performed by the run. Here two ALTER TABLE commands.

Then there is the section `todo` which list all migrations. A migration is defined by an operation which must be performed to reach the target state, but **jdchive** can't do himself.

Each migration is defined by 6 attributes:

| Attribute           | Description |
| ---:                | --- |
| existing            | The current state of the table (Or database) as it exist on the target system |
| target              | The target state, as it is defined in the description yaml file |
| diff                | The difference between the two previous definition|
| existingFingerprint | A fingerprint (checksum) of the `existing` element. Intended to identify a given state. Refer to **Database migration** below |
| targetFingerprint   | Same as above for `target` element |
| diffFingerprint     | Same as obove for `diff` element.|

## Database migration

Based on the [reporting feature](#report-file) of **jdchive**, one may conceive to build a simple Database migration tool.

The base principle of such tools is to apply migration step based on a initial and target database schema version. Of course, coding the migration steps is up to the user in all case. There is no magic here.

But, aim of such tool is to allow automation of these migration step for a typical deployment workflow (i.e. Dev/Integration/PrepPoduction/Production).  

Database schema version are typically stored in a specific table, managed by the migration tool. In **jdchive** case, table definition fingerprinting offer a more flexible and error free solution to determine which migration step must be applied.

Another help for database migration is the return code of the **jdchive** command.

* 0 means everything is OK and all database/target state has been reached. In other words, the `todo` element in the report file is empty.

* 1 means there is some migration to perform to reach the target state. In other words, the `todo` element in the report file contains some migration to perform.

* All other values means an unexpected error occurred. 

## Build

Just clone this repository and then:

```bash
$ gradlew rpm
```

This should build everything. You should be able to find generated packages in `build/distribution` folder.

## License

    Copyright (C) 2017 BROADSoftware

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
