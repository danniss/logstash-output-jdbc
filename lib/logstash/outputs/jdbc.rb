# encoding: utf-8
require 'logstash/outputs/base'
require 'logstash/namespace'
require 'concurrent'
require 'stud/interval'
require 'java'
require 'logstash-output-jdbc_jars'

# Write events to a SQL engine, using JDBC.
#
# It is upto the user of the plugin to correctly configure the plugin. This
# includes correctly crafting the SQL statement, and matching the number of
# parameters correctly.
class LogStash::Outputs::Jdbc < LogStash::Outputs::Base
  concurrency :shared

  STRFTIME_FMT = '%Y-%m-%d %T.%L'.freeze

  RETRYABLE_SQLSTATE_CLASSES = [
    # Classes of retryable SQLSTATE codes
    # Not all in the class will be retryable. However, this is the best that 
    # we've got right now.
    # If a custom state code is required, set it in retry_sql_states.
    '08', # Connection Exception
    '24', # Invalid Cursor State (Maybe retry-able in some circumstances)
    '25', # Invalid Transaction State 
    '40', # Transaction Rollback 
    '53', # Insufficient Resources
    '54', # Program Limit Exceeded (MAYBE)
    '55', # Object Not In Prerequisite State
    '57', # Operator Intervention
    '58', # System Error
  ].freeze

  config_name 'jdbc'

  # Driver class - Reintroduced for https://github.com/theangryangel/logstash-output-jdbc/issues/26
  config :driver_class, validate: :string

  # Does the JDBC driver support autocommit?
  config :driver_auto_commit, validate: :boolean, default: true, required: true

  # Where to find the jar
  # Defaults to not required, and to the original behaviour
  config :driver_jar_path, validate: :string, required: false

  # jdbc connection string
  config :connection_base_string, validate: :string, required: true

  # jdbc username - optional, maybe in the connection string
  config :username, validate: :string, required: false

  # jdbc password - optional, maybe in the connection string
  config :password, validate: :string, required: false

  # eg, tables => {"db_name" => {"table_name" => {seperator => "|" fields => ["Date", "String", "String", "Timestamp", "String", "Number", "String", "Number", "String", "String", "String"]}}}
  # db_name is the destination database to insert record,
  # table_name is the destination table to insert record,
  # seperator is the delimiter to split message in events
  # fields is a list of data types for each field
  config :tables, validate: :hash, required: true

  # If this is an unsafe statement, use event.sprintf
  # This also has potential performance penalties due to having to create a
  # new statement for each event, rather than adding to the batch and issuing
  # multiple inserts in 1 go
  config :unsafe_statement, validate: :boolean, default: false

  # Number of connections in the pool to maintain
  config :max_pool_size, validate: :number, default: 24

  # Connection timeout
  config :connection_timeout, validate: :number, default: 10000

  # We buffer a certain number of events before flushing that out to SQL.
  # This setting controls how many events will be buffered before sending a
  # batch of events.
  config :flush_size, validate: :number, default: 1000

  # Set initial interval in seconds between retries. Doubled on each retry up to `retry_max_interval`
  config :retry_initial_interval, validate: :number, default: 2

  # Maximum time between retries, in seconds
  config :retry_max_interval, validate: :number, default: 128

  # Any additional custom, retryable SQL state codes. 
  # Suitable for configuring retryable custom JDBC SQL state codes.
  config :retry_sql_states, validate: :array, default: []

  # Run a connection test on start.
  config :connection_test, validate: :boolean, default: true

  # Connection test and init string, required for some JDBC endpoints
  # notable phoenix-thin - see logstash-output-jdbc issue #60
  config :connection_test_query, validate: :string, required: false

  # Maximum number of sequential failed attempts, before we stop retrying.
  # If set to < 1, then it will infinitely retry.
  # At the default values this is a little over 10 minutes
  config :max_flush_exceptions, validate: :number, default: 10

  config :max_repeat_exceptions, obsolete: 'This has been replaced by max_flush_exceptions - which behaves slightly differently. Please check the documentation.'
  config :max_repeat_exceptions_time, obsolete: 'This is no longer required'
  config :idle_flush_time, obsolete: 'No longer necessary under Logstash v5'

  def register
    @logger.info('JDBC - Starting up')

    load_jar_files!
    init_pools!

    @stopping = Concurrent::AtomicBoolean.new(false)

    @logger.warn('JDBC - Flush size is set to > 1000') if @flush_size > 1000

  end

  def multi_receive(events)
    events.each_slice(@flush_size) do |slice|
      retrying_submit(slice)
    end
  end

  def close
    @stopping.make_true
    @pools.each do |name, pool|
        pool.close
    end
    super
  end

  private

  def setup_and_test_pool!
    # Setup pool
    @pool = Java::ComZaxxerHikari::HikariDataSource.new

    @pool.setAutoCommit(@driver_auto_commit)
    @pool.setDriverClassName(@driver_class) if @driver_class

    @pool.setJdbcUrl(@connection_base_string)

    @pool.setUsername(@username) if @username
    @pool.setPassword(@password) if @password

    @pool.setMaximumPoolSize(@max_pool_size)
    @pool.setConnectionTimeout(@connection_timeout)

    validate_connection_timeout = (@connection_timeout / 1000) / 2

    if !@connection_test_query.nil? and @connection_test_query.length > 1
      @pool.setConnectionTestQuery(@connection_test_query)
      @pool.setConnectionInitSql(@connection_test_query)
    end

    return unless @connection_test

    # Test connection
    test_connection = @pool.getConnection
    unless test_connection.isValid(validate_connection_timeout)
      @logger.warn('JDBC - Connection is not reporting as validate. Either connection is invalid, or driver is not getting the appropriate response.')
    end
    test_connection.close
  end

  def init_pools!
    @pools = {}
  end

  def get_connection(db_name)
    pool = @pools[db_name]
    if pool == nil
        pool = Java::ComZaxxerHikari::HikariDataSource.new
        pool.setAutoCommit(@driver_auto_commit)
        pool.setDriverClassName(@driver_class) if @driver_class

        pool.setJdbcUrl(@connection_base_string + "/" + db_name)

        pool.setUsername(@username) if @username
        pool.setPassword(@password) if @password

        pool.setMaximumPoolSize(@max_pool_size)
        pool.setConnectionTimeout(@connection_timeout)
        pool.setConnectionTestQuery("select 1")
        pool.setConnectionInitSql("select 1")
        @pools[db_name] = pool
    end

    pool.getConnection
  end

  def load_jar_files!
    # Load jar from driver path
    unless @driver_jar_path.nil?
      raise LogStash::ConfigurationError, 'JDBC - Could not find jar file at given path. Check config.' unless File.exist? @driver_jar_path
      require @driver_jar_path
      return
    end

    # Revert original behaviour of loading from vendor directory
    # if no path given
    jarpath = if ENV['LOGSTASH_HOME']
                File.join(ENV['LOGSTASH_HOME'], '/vendor/jar/jdbc/*.jar')
              else
                File.join(File.dirname(__FILE__), '../../../vendor/jar/jdbc/*.jar')
              end

    @logger.trace('JDBC - jarpath', path: jarpath)

    jars = Dir[jarpath]
    raise LogStash::ConfigurationError, 'JDBC - No jars found. Have you read the README?' if jars.empty?

    jars.each do |jar|
      @logger.trace('JDBC - Loaded jar', jar: jar)
      require jar
    end
  end

  def submit(events)
    connection = nil
    statement = nil
    events_to_retry = []


    events.each do |event|
      @logger.info(event.get("db_table"))
      @logger.info(event.get("messages").length.to_s)
      db_name, table = event.get("db_table").split("|")
      if db_name == nil
        next
      end
      begin
        connection = get_connection(db_name)
      rescue => e
        log_jdbc_exception(e, true)
        # If a connection is not available, then the server has gone away
        # We're not counting that towards our retry count.
        return events, false
      end
      if table == nil
        next
      end
      table_settings = @tables.fetch(db_name, nil)
      if table_settings == nil
        next
      end
      table_setting = table_settings.fetch(table, nil)
      if table_setting == nil
        next
      end
      events_to_retry += push_event_into_db(event, connection, table, table_setting)
    end

    return events_to_retry, true
  end

  def retrying_submit(actions)
    # Initially we submit the full list of actions
    submit_actions = actions
    count_as_attempt = true

    attempts = 1

    sleep_interval = @retry_initial_interval
    while @stopping.false? and (submit_actions and !submit_actions.empty?)
      return if !submit_actions || submit_actions.empty? # If everything's a success we move along
      # We retry whatever didn't succeed
      submit_actions, count_as_attempt = submit(submit_actions)

      # Everything was a success!
      break if !submit_actions || submit_actions.empty?

      if @max_flush_exceptions > 0 and count_as_attempt == true
        attempts += 1

        if attempts > @max_flush_exceptions
          @logger.error("JDBC - max_flush_exceptions has been reached. #{submit_actions.length} events have been unable to be sent to SQL and are being dropped. See previously logged exceptions for details.")
          break
        end
      end

      # If we're retrying the action sleep for the recommended interval
      # Double the interval for the next time through to achieve exponential backoff
      Stud.stoppable_sleep(sleep_interval) { @stopping.true? }
      sleep_interval = next_sleep_interval(sleep_interval)
    end
  end

  def build_prepare_statement(connection, table, fields, count)
    sql = "insert into " + table + " values"
    field_str = "("
    fields.each_with_index do |type, idx|
      if type == "TIMESTAMP"
        field_str += "CAST (? AS timestamp)"
      elsif type == "DATETIME"
        field_str += "CAST (? AS timestamp)"
      elsif type == "DATE"
        field_str += "CAST (? AS date)"
      else
        field_str += "?"
      end

      if idx == fields.length - 1
        field_str += ")"
      else
        field_str += ","
      end
    end
    sql += (field_str + ",") * (count - 1)
    sql += field_str
    connection.prepareStatement(sql)
  end

  def push_event_into_db(event, connection, table, table_setting)
    events_to_retry = []
    begin
      fields = table_setting.fetch("fields")
      messages = event.get("messages")
      prefixs = event.get("prefixs")
      seperator = table_setting.fetch("seperator")
      statement = build_prepare_statement(connection, table, fields, messages.length)

      position = 1
      messages.each_with_index do |message, message_idx|
        prefix = prefixs[message_idx]
        if prefix != nil
          values = prefix + message.split(seperator)
        else
          values = message.split(seperator)
        end
        fields.each_with_index do |type, field_idx|
          case type
          when "NUMBER"
            begin
              statement.setLong(position, values[field_idx])
            rescue Exception
              statement.setLong(position, nil)
            end
          when "FLOAT"
            begin
              statement.setFloat(position, values[field_idx])
            rescue Exception
              statement.setFloat(position, nil)
            end
          when "BOOLEAN"
            begin
              statement.setBoolean(position, values[field_idx])
            rescue Exception
              statement.setBoolean(position, nil)
            end
          else
            begin
              statement.setString(position, values[field_idx])
            rescue Exception
              statement.setString(position, nil)
            end
          end
          position += 1
        end
      end

      statement.execute
    rescue => e
      if retry_exception?(e)
        events_to_retry.push(event)
      else
        @logger.warn(statement.to_s)
      end
    ensure
      statement.close unless statement.nil?
    end
    connection.close unless connection.nil?
    return events_to_retry
  end

  def retry_exception?(exception)
    retrying = (exception.respond_to? 'getSQLState' and (RETRYABLE_SQLSTATE_CLASSES.include?(exception.getSQLState.to_s[0,2]) or @retry_sql_states.include?(exception.getSQLState)))
    log_jdbc_exception(exception, retrying)

    retrying
  end

  def log_jdbc_exception(exception, retrying)
    current_exception = exception
    log_text = 'JDBC - Exception. ' + (retrying ? 'Retrying' : 'Not retrying') + '.'
    log_method = (retrying ? 'warn' : 'error')

    loop do
      @logger.send(log_method, log_text, :exception => current_exception)

      if current_exception.respond_to? 'getNextException'
        current_exception = current_exception.getNextException()
      else
        current_exception = nil
      end

      break if current_exception == nil
    end
  end

  def next_sleep_interval(current_interval)
    doubled = current_interval * 2
    doubled > @retry_max_interval ? @retry_max_interval : doubled
  end
end # class LogStash::Outputs::jdbc
