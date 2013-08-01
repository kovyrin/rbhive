# suppress warnings
old_verbose, $VERBOSE = $VERBOSE, nil

raise 'Thrift is not loaded' unless defined?(Thrift)
raise 'RBHive is not loaded' unless defined?(RBHive)

# require thrift autogenerated files
require File.join(File.dirname(__FILE__), *%w[.. thrift t_c_l_i_service_constants])
require File.join(File.dirname(__FILE__), *%w[.. thrift t_c_l_i_service])
require File.join(File.dirname(__FILE__), *%w[.. thrift sasl_client_transport])

# restore warnings
$VERBOSE = old_verbose


module RBHive
  def tcli_connect(server, port=10_000, sasl_params={})
    connection = RBHive::TCLIConnection.new(server, port, sasl_params)
    ret = nil
    begin
      connection.open
      connection.open_session
      ret = yield(connection)

    ensure
      # Try to close the session and our connection if those are still open, ignore io errors
      begin
        connection.close_session if connection.session
        connection.close
      rescue IOError => e
        # noop
      end
    end

    return ret
  end
  module_function :tcli_connect

  class StdOutLogger
    %w(fatal error warn info debug).each do |level|
      define_method level.to_sym do |message|
        STDOUT.puts(message)
     end
   end
  end

  class TCLIConnection
    attr_reader :client

    def initialize(server, port=10_000, sasl_params=nil, logger=StdOutLogger.new)
      @socket = Thrift::Socket.new(server, port)
      @socket.timeout = 1800
      @logger = logger

      @sasl_params = parse_sasl_params(sasl_params)
      if @sasl_params
        @logger.info("Initializing transport with SASL support")
        @transport = Thrift::SaslClientTransport.new(@socket, @sasl_params)
      else
        @transport = Thrift::BufferedTransport.new(@socket)
      end

      @protocol = Thrift::BinaryProtocol.new(@transport)
      @client = Hive2::Thrift::TCLIService::Client.new(@protocol)
      @session = nil
      @logger.info("Connecting to HiveServer2 #{server} on port #{port}")
      @mutex = Mutex.new
    end

    # Processes SASL connection params and returns a hash with symbol keys or a nil
    def parse_sasl_params(sasl_params)
      # Symbilize keys in a hash
      if sasl_params.kind_of?(Hash)
        return sasl_params.inject({}) do |memo,(k,v)|
          memo[k.to_sym] = v;
          memo
        end
      end
      return nil
    end

    def open
      @transport.open
    end

    def close
      @transport.close
    end

    def open_session
      @session = @client.OpenSession(prepare_open_session)
    end

    def close_session
      @client.CloseSession prepare_close_session
      @session = nil
    end

    def session
      @session && @session.sessionHandle
    end

    def client
      @client
    end

    def execute(query)
      execute_safe(query)
    end

    def priority=(priority)
      set("mapred.job.priority", priority)
    end

    def queue=(queue)
      set("mapred.job.queue.name", queue)
    end

    def set(name,value)
      @logger.info("Setting #{name}=#{value}")
      self.execute("SET #{name}=#{value}")
    end

    # Performs a query on the server, fetches up to *max_rows* rows and returns them as an array.
    def fetch(query, max_rows = 100)
      safe do
        # Execute the query and check the result
        exec_result = execute_unsafe(query)
        raise_error_if_failed!(exec_result)

        # Get search operation handle to fetch the results
        op_handle = exec_result.operationHandle

        # Prepare and execute fetch results request
        fetch_req = prepare_fetch_results(op_handle, :first, max_rows)
        fetch_results = client.FetchResults(fetch_req)
        raise_error_if_failed!(fetch_results)

        # Get data rows and format the result
        rows = fetch_results.results.rows
        the_schema = TCLISchemaDefinition.new(get_schema_for( op_handle ), rows.first)
        TCLIResultSet.new(rows, the_schema)
      end
    end

    # Performs a query on the server, fetches the results in batches of *batch_size* rows
    # and yields the result batches to a given block as arrays of rows.
    def fetch_in_batch(query, batch_size = 1000, &block)
      raise "No block given for the batch fetch request!" unless block_given?
      safe do
        # Execute the query and check the result
        exec_result = execute_unsafe(query)
        raise_error_if_failed!(exec_result)

        # Get search operation handle to fetch the results
        op_handle = exec_result.operationHandle

        # Prepare fetch results request
        fetch_req = prepare_fetch_results(op_handle, :next, batch_size)

        # Now let's iterate over the results
        loop do
          # Fetch next batch and raise an exception if it failed
          fetch_results = client.FetchResults(fetch_req)
          raise_error_if_failed!(fetch_results)

          # Get data rows from the result
          rows = fetch_results.results.rows
          break if rows.empty?

          # Prepare schema definition for the row
          the_schema = TCLISchemaDefinition.new(get_schema_for(op_handle), rows.first)

          # Format the results and yield them to the given block
          yield TCLIResultSet.new(rows, the_schema)
        end
      end
    end

    def create_table(schema)
      execute(schema.create_table_statement)
    end

    def drop_table(name)
      name = name.name if name.is_a?(TableSchema)
      execute("DROP TABLE `#{name}`")
    end

    def replace_columns(schema)
      execute(schema.replace_columns_statement)
    end

    def add_columns(schema)
      execute(schema.add_columns_statement)
    end

    def method_missing(meth, *args)
      client.send(meth, *args)
    end

    private

    # Executes a query and makes sure it has succeeded
    def execute_safe(query)
      safe do
        exec_result = execute_unsafe(query)
        raise_error_if_failed!(exec_result)
        return exec_result
      end
    end

    def execute_unsafe(query)
      @logger.info("Executing Hive Query: #{query}")
      req = prepare_execute_statement(query)
      client.ExecuteStatement(req)
    end

    def safe
      ret = nil
      @mutex.synchronize { ret = yield }
      ret
    end

    def prepare_open_session
      ::Hive2::Thrift::TOpenSessionReq.new( @sasl_params.nil? ? [] : @sasl_params )
    end

    def prepare_close_session
      ::Hive2::Thrift::TCloseSessionReq.new( sessionHandle: self.session )
    end

    def prepare_execute_statement(query)
      ::Hive2::Thrift::TExecuteStatementReq.new( sessionHandle: self.session, statement: query.to_s, confOverlay: {} )
    end

    def prepare_fetch_results(handle, orientation=:first, rows=100)
      orientation_value = "FETCH_#{orientation.to_s.upcase}"
      valid_orientations = ::Hive2::Thrift::TFetchOrientation::VALUE_MAP.values
      unless valid_orientations.include?(orientation_value)
        raise ArgumentError, "Invalid orientation: #{orientation.inspect}"
      end
      orientation_const = eval("::Hive2::Thrift::TFetchOrientation::#{orientation_value}")
      ::Hive2::Thrift::TFetchResultsReq.new(
        operationHandle: handle,
        orientation: orientation_const,
        maxRows: rows
      )
    end

    def get_schema_for(handle)
      req = ::Hive2::Thrift::TGetResultSetMetadataReq.new( operationHandle: handle )
      metadata = client.GetResultSetMetadata( req )
      metadata.schema
    end

    # Raises an exception if given operation result is a failure
    def raise_error_if_failed!(result)
      return if result.status.statusCode == 0
      error_message = result.status.errorMessage || 'Execution failed!'
      raise error_message
    end
  end
end
