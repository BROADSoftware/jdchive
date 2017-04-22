# jdchive

An idempotent tool to easily create and maintain Hive table.

When deploying an application in the Hadoop world, a common issue is to create the required Hive table.

This is usually achieved using scripts. But, this can quickly become cumbersome and hard to maintain. And a nightmare when it come to updating a running application.

'jdc...' stand for 'Just DesCribe'. You define all the Hive database, table, column, properties of your application in a simple YAML file and jdchive will take care of deploying all theses object on your cluster.

jdchive is a convergence tool. Its aim is to align the real configuration to what is described on the source file, while applying only strictly necessary modification.

This make jdchive a fully idempotent tool, as all modern DevOps tools should be.

Regarding schema evolution, jdchive will do its best to perform required modification using ALTER commands. For most involving evolution, requiring data manipulation, jdchive will act as an inovating Database Migration framework.

## Usage

jdchive is provided as rpm packages (Sorry, only this packaging is currently provided. Contribution welcome), on the [release pages](https://github.com/BROADSoftware/jdchive/releases).

jdchive MUST be used on properly configured Hadoop client node. (i.e `hive` shell must be functional)

Once installed, usage is the following:

~~~bash
# jdchive --inputFile yourDescription.yml
~~~
 
Where `yourDescription.yml` is a file containing your target Hive database and table description. jdchive will then perform all required operation to reach this target state.

Note than if `yourDescription.yml` content match the current configuration, no operation will be performed.

Here is a sample of such `description.yml` file:

~~~yaml
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
~~~

Of course, database and table description may include several other attributes, to represent most of the option on Hive table creation. This is described later in this document.  

### Other launch option

When launching the jdchive command you may provide some optional command line options:


| Option| Description | 
| --- | --- |
|`--configFile`| This parameter allow an Hadoop properties configuration file to be added to the default set. This parameters can occur several times on the command line|
|`--defaultState`|`present` or `absent`. This parameter allow setting of all `state` value which are not explicitly defined. |
|`--dryRun`|This switch will prevent jdchive to perform any effective operation.|
|`--dumpConfigFile`|For Debuging purpose: All Hadoop Configuration will be dumped in the provided file parameter|
|`--keytab`| This parameter specify a keytab for Kerberos authentication. If present, `--principal` parameter must also be defined.|
|`--principal`| This parameter specify a principal for Kerberos authentication. If present, `--keytab` parameter must also be defined.|


### Table and database deletion

All databases or tables not described in the `description.yml` file will be left untouched.

To allow deletion to be performed, theses objects got a `state:` attribute. When not defined, it default to  `present`, or to the value provided by the `--defaultState` parameter. But it could be set to `absent` to trigger the deletion of the corresponding entity.

For example: 

~~~yaml

databases:
- name: jdctest1
  state: absent
    
tables:
- name: testSimple
  database: jdctest1
  state: absent
~~~

Will remove all object created by our previous example.

> Note, as a security, no cascading deletion from database to table will be performed. Deletion of a database can only be effective if all hosted table are explicitly deleted. 

## Database definition

Here is a description of all attributes for a Database definition.

| Attribute | Req. | Description |
| ---: | :---: | --- |
|name|yes|The name of the database|
| properties |no| A map of properties. Equivalent to WITH DBPROPERTIES Hive DDL clause|
| location |no| Equivalent to LOCATION Hive DDL clause|
| owner |no| The owner of the database. May be a user account or a group.|
| owner_type |no| USER or ROLE. Specify what represent the owner attribute|
| comment |no| Equivalent to the COMMENT Hive DDL close|
| state |no| `present`or `absent`. Define the target state to reach.|

### Database ownership

Database owner can be explicitly set by the attribute defiened above. If this attribute is not present, then the owner will be the account under wich the `jdchive` command was launch during database creation.

Once created, one may change owner by setting the corresponding attribute. Launching jdchive under another account will have no impact.

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
CREATE DATABASE jdctest2 COMMENT 'For jdchive table testing' LOCATION 'hdfs://hdp11/user/apphive/db/testtables1'
ALTER DATABASE jdctest2 SET OWNER USER apphive
```

## Table attributes

Here is a description of all attributes for a Database definition.

| Attribute       | Req. | Description |
| ---:            | :---: | --- |
|name             |yes|The name of the table|
|database         |yes|The database this table belong to|
|external         |no |Boolean. Equivalent to the EXTERNAL Hive DDL clause|
|fields           |no |A list of fields definition describing the table's column. Refer to **Field definition** below|
|comment          |no |An optionnal comment associated to the table|
|location         |no | Equivalent to the LOCATION Hive DDL clause|
|properties       |no |A map of properties. Equivalent to TBLPROPERTIES Hive DDL clause|
|stored_as        |no |Specify the file format, such as SEQUENCEFILE, TEXTEFILE, RCFILE, ....<br/>Equivalent to STORED AS Hive DDL clause [1]|
|input_format     |no |Equivalent to STORED AS INPUT FORMAT '....' Hive DDL clause [1][2]|
|output_format    |no |Equivalent to STORED AS OUTPUT FORMAT '....' Hive DDL clause [1][2]|
|delimited        |no |A map of delimiter character. Equivalent to ROW FORMAT DELIMITED Hive DDL clause. Refer to **Delimited row format** below [3]|
|serde            |no |Allow explicit definition of a `serde`'. Equivalent to ROW FORMAT SERDE Hive DDL clause [3]|
|serde_properties |no |A map of properties associated to the `serde`. Equivalent to WITH SERDEPROPERTIES Hive DDL clause|
|storage_handler  |no |Allow definition of the storage handler. Equivalent to STORED BY Hive DDL clause|
|partitions       |no |A list of fields definition describing the table's partitionning. Refer to **Field definition** below|
|clustered_by     |no |Allow definition of a CLUSTERED BY Hive DDL clause. Refer to **Table clustering** below|
|skewed_by        |no |Allow definition of a |
|alterable        |no |xxxxx|
|droppable        |no |xxxxx|
|state            |no |xxxxx|

[1]: Storage format can be defined using two methods:

* Use `stored_by`. This will define implicitly `input_format`, `output_format` and, for some value the `serde`.
* Explictly define `input_format`, `output_format` and eventually `serde`.

The two approaches are exclusive. Defining both `stored_by` and `input/output_format` will generate an error.

[2] `input_format` and `output_format` must be defined both if used.

[3] `delimited` and `serde` are exclusive and can't be defined both.

### Field definition:

Here is the definition of a `field` element:

| Field Attribute | Req. | Description |
| ---:  | :---: | --- |
|name   |yes|The name of the field|
|type   |yes|The type of the field|
|comment|no |An optionnal comment|

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

### Table clustering

Here is the definition of a `clustered_by` element:

| Attribute | Req. | Description |
| ---: | :---: | --- |
|columns     |yes|This list of columns to CLUSTER BY |
|nbr_buckets |yes|The number of buckets|
|sorted_by   |no |A list of sort item element, as defined just below|

Inner sort item element:

| Attribute | Req. | Description |
| ---: | :---: | --- |
|columns   |yes|A list of column |
|direction |no|The direction: `ASC` or `DESC`. Default is `ASC`|

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
 CLUSTERED BY(userid,page_url) SORTED BY (userid asc,page_url desc) INTO 16 BUCKETS
```

### Skewed value

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

### Examples



## Report file

....




## Advanced configuration

...

* `--reportFile` parameter will instruct jdchive the generate a report file. 

          
## Kerberos secured cluster

In the case your Hadoop cluster is protected by Kerberos, you have two methods to provide authentication.

* Using the `--principal` and `--keytab` parameters.

* Issue a `kinit` command before launching jdchive. (You then can check your ticket with the `klist` command).

In both case, the operation will be performed on behalf of the owner of the ticket. Ensure this user has got sufficient access privileges on Hive.

This also means created Database and Table will belong tho the principal owner of the ticket.

## Ansible integration

With its idempotence property, jdchive is very easy to be orchestrated by usual DevOps tools like Chef, Puppet or Ansible.

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
