# logstash-output-jdbc 

[![Build Status](https://travis-ci.org/theangryangel/logstash-output-jdbc.svg?branch=master)](https://travis-ci.org/theangryangel/logstash-output-jdbc)

This plugin is provided as an external plugin and is not part of the Logstash project.

This plugin allows you to output to SQL databases, in batch way.

## Installation
  - manally copy files to your {logstash_install_dir}/vendor/bundle/jruby/{version}/gems/
  - Now either:
    - Use driver_jar_path in your configuraton to specify a path to your jar file
  - Or:
    - Create the directory vendor/jar/jdbc in your logstash installation (`mkdir -p vendor/jar/jdbc/`)
    - Add JDBC jar files to vendor/jar/jdbc in your logstash installation
  - And then configure (examples can be found in the examples directory)

## Configuration options

| Option | Type | Description | Required? | Default |
| ------ | ---- | ----------- | --------- | ------- |
| driver_class | String | Specify a driver class if autoloading fails | No | |
| driver_auto_commit | Boolean | If the driver does not support auto commit, you should set this to false | No | True |
| driver_jar_path | String | File path to jar file containing your JDBC driver. This is optional, and all JDBC jars may be placed in $LOGSTASH_HOME/vendor/jar/jdbc instead. | No | |
| connection_string | String | JDBC connection URL | Yes | |
| connection_test | Boolean | Run a JDBC connection test. Some drivers do not function correctly, and you may need to disable the connection test to supress an error. Cockroach with the postgres JDBC driver is such an example. | No | Yes |
| connection_test_query | String | Connection test and init query string, required for some JDBC drivers that don't support isValid(). Typically you'd set to this "SELECT 1" | No |  |
| username | String | JDBC username - this is optional as it may be included in the connection string, for many drivers | No | |
| password | String | JDBC password - this is optional as it may be included in the connection string, for many drivers | No | |
| tables | Hash | eg, tables => {"table_name" => {seperator => "," fields => ["Date", "String", "String", "Timestamp", "String", "Number", "String", "Number", "String", "String", "String"]}}.  table_name is the destination table to insert record.  seperator is the  delimiter to split message in events. fields is a list of data types for each field | Yes |  |
| max_pool_size | Number | Maximum number of connections to open to the SQL server at any 1 time | No | 5 |
| connection_timeout | Number | Number of seconds before a SQL connection is closed | No | 2800 |
| flush_size | Number | Maximum number of entries to buffer before sending to SQL - if this is reached before idle_flush_time | No | 1000 |
| max_flush_exceptions | Number | Number of sequential flushes which cause an exception, before the set of events are discarded. Set to a value less than 1 if you never want it to stop. This should be carefully configured with respect to retry_initial_interval and retry_max_interval, if your SQL server is not highly available | No | 10 |
| retry_initial_interval | Number | Number of seconds before the initial retry in the event of a failure. On each failure it will be doubled until it reaches retry_max_interval | No | 2 |
| retry_max_interval | Number | Maximum number of seconds between each retry | No | 128 |
| retry_sql_states | Array of strings | An array of custom SQL state codes you wish to retry until `max_flush_exceptions`. Useful if you're using a JDBC driver which returns retry-able, but non-standard SQL state codes in it's exceptions. | No | [] |

## Example configurations
Example logstash configurations, can now be found in the examples directory. Where possible we try to link every configuration with a tested jar.

If you have a working sample configuration, for a DB thats not listed, pull requests are welcome.

## Development and Running tests
For development tests are recommended to run inside a virtual machine (Vagrantfile is included in the repo), as it requires
access to various database engines and could completely destroy any data in a live system.

