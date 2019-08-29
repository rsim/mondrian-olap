module Mondrian
  module OLAP
    class Connection
      def self.create(params)
        connection = new(params)
        connection.connect
        connection
      end

      attr_reader :raw_connection, :raw_mondrian_connection, :raw_catalog, :raw_schema,
                  :raw_schema_reader, :raw_cache_control

      def initialize(params = {})
        @params = params
        @driver = params[:driver]
        @connected = false
        @raw_connection = nil
      end

      def connect
        Error.wrap_native_exception do
          # hack to call private constructor of MondrianOlap4jDriver
          # to avoid using DriverManager which fails to load JDBC drivers
          # because of not seeing JRuby required jar files
          cons = Java::MondrianOlap4j::MondrianOlap4jDriver.java_class.declared_constructor
          cons.accessible = true
          driver = cons.new_instance.to_java

          props = java.util.Properties.new
          props.setProperty('JdbcUser', @params[:username]) if @params[:username]
          props.setProperty('JdbcPassword', @params[:password]) if @params[:password]

          # on Oracle increase default row prefetch size
          # as default 10 is very low and slows down loading of all dimension members
          if @driver == 'oracle'
            prefetch_rows = @params[:prefetch_rows] || 100
            props.setProperty("jdbc.defaultRowPrefetch", prefetch_rows.to_s)
          end

          conn_string = connection_string

          # latest Mondrian version added ClassResolver which uses current thread class loader to load some classes
          # therefore need to set it to JRuby class loader to ensure that Mondrian classes are found
          # (e.g. when running mondrian-olap inside OSGi container)
          current_thread = Java::JavaLang::Thread.currentThread
          class_loader = current_thread.getContextClassLoader
          begin
            current_thread.setContextClassLoader JRuby.runtime.jruby_class_loader
            @raw_jdbc_connection = driver.connect(conn_string, props)
          ensure
            current_thread.setContextClassLoader(class_loader)
          end

          @raw_connection = @raw_jdbc_connection.unwrap(Java::OrgOlap4j::OlapConnection.java_class)
          @raw_catalog = @raw_connection.getOlapCatalog
          # currently it is assumed that there is just one schema per connection catalog
          @raw_schema = @raw_catalog.getSchemas.first
          @raw_mondrian_connection = @raw_connection.getMondrianConnection
          @raw_schema_reader = @raw_mondrian_connection.getSchemaReader
          @raw_cache_control = @raw_mondrian_connection.getCacheControl(nil)
          @connected = true
          true
        end
      end

      def connected?
        @connected
      end

      def close
        @raw_jdbc_connection = @raw_catalog = @raw_schema = @raw_mondrian_connection = nil
        @raw_schema_reader = @raw_cache_control = nil
        @raw_connection.close
        @raw_connection = nil
        @connected = false
        true
      end

      def execute(query_string, parameters = {})
        options = {}
        Error.wrap_native_exception(options) do
          start_time = Time.now
          statement = @raw_connection.prepareOlapStatement(query_string)
          options[:profiling_statement] = statement if parameters[:profiling]
          set_statement_parameters(statement, parameters)
          raw_cell_set = statement.executeQuery()
          total_duration = ((Time.now - start_time) * 1000).to_i
          Result.new(self, raw_cell_set, profiling_handler: statement.getProfileHandler, total_duration: total_duration)
        end
      end

      # access mondrian.olap.Parameter object
      def mondrian_parameter(parameter_name)
        Error.wrap_native_exception do
          @raw_schema_reader.getParameter(parameter_name)
        end
      end

      def execute_drill_through(query_string)
        Error.wrap_native_exception do
          statement = @raw_connection.createStatement
          Result::DrillThrough.new(statement.executeQuery(query_string))
        end
      end

      def from(cube_name)
        Query.from(self, cube_name)
      end

      def raw_schema_key
        @raw_mondrian_connection.getSchema.getKey
      end

      def schema_key
        raw_schema_key.toString
      end

      def self.raw_schema_key(schema_key)
        if schema_key =~ /\A<(.*), (.*)>\z/
          schema_content_key = $1
          connection_key = $2

          cons = Java::mondrian.rolap.SchemaContentKey.java_class.declared_constructor(java.lang.String)
          cons.accessible = true
          raw_schema_content_key = cons.new_instance(schema_content_key)

          cons = Java::mondrian.rolap.ConnectionKey.java_class.declared_constructor(java.lang.String)
          cons.accessible = true
          raw_connection_key = cons.new_instance(connection_key)

          cons = Java::mondrian.rolap.SchemaKey.java_class.declared_constructor(
            Java::mondrian.rolap.SchemaContentKey, Java::mondrian.rolap.ConnectionKey)
          cons.accessible = true
          cons.new_instance(raw_schema_content_key, raw_connection_key)
        else
          raise ArgumentError, "invalid schema key #{schema_key}"
        end
      end

      def cube_names
        @raw_schema.getCubes.map{|c| c.getName}
      end

      def cube(name)
        Cube.get(self, name)
      end

      # Will affect only the next created connection. If it is necessary to clear all schema cache then
      # flush_schema_cache should be called, then close and then new connection should be created.
      # This method flushes schemas for all connections (clears the schema pool).
      def flush_schema_cache
        raw_cache_control.flushSchemaCache
      end

      def self.raw_schema_pool
        method = Java::mondrian.rolap.RolapSchemaPool.java_class.declared_method('instance')
        method.accessible = true
        method.invoke_static
      end

      def self.flush_schema_cache
        method = Java::mondrian.rolap.RolapSchemaPool.java_class.declared_method('clear')
        method.accessible = true
        method.invoke(raw_schema_pool)
      end

      # This method flushes the schema only for this connection (removes from the schema pool).
      def flush_schema
        if raw_mondrian_connection && (rolap_schema = raw_mondrian_connection.getSchema)
          raw_cache_control.flushSchema(rolap_schema)
        end
      end

      def self.flush_schema(schema_key)
        method = Java::mondrian.rolap.RolapSchemaPool.java_class.declared_method('remove',
          Java::mondrian.rolap.SchemaKey.java_class)
        method.accessible = true
        method.invoke(raw_schema_pool, raw_schema_key(schema_key))
      end

      def available_role_names
        @raw_connection.getAvailableRoleNames.to_a
      end

      def role_name
        @raw_connection.getRoleName
      end

      def role_names
        # workaround to access non-public method (was not public when using inside Torquebox)
        # @raw_connection.getRoleNames.to_a
        @raw_connection.java_method(:getRoleNames).call.to_a
      end

      def role_name=(name)
        Error.wrap_native_exception do
          @raw_connection.setRoleName(name)
        end
      end

      def role_names=(names)
        Error.wrap_native_exception do
          # workaround to access non-public method (was not public when using inside Torquebox)
          # @raw_connection.setRoleNames(Array(names))
          names = Array(names)
          @raw_connection.java_method(:setRoleNames, [Java::JavaUtil::List.java_class]).call(names)
          names
        end
      end

      def locale
        @raw_connection.getLocale.toString
      end

      def locale=(locale)
        locale_elements = locale.to_s.split('_')
        raise ArgumentError, "invalid locale string #{locale.inspect}" unless [1, 2, 3].include?(locale_elements.length)
        java_locale = Java::JavaUtil::Locale.new(*locale_elements)
        @raw_connection.setLocale(java_locale)
      end

      # access MondrianServer instance
      def mondrian_server
        Error.wrap_native_exception do
          @raw_connection.getMondrianConnection.getServer
        end
      end

      # Force shutdown of static MondrianServer, should not normally be used.
      # Can be used in at_exit block if JRuby based plugin is unloaded from other Java application.
      # WARNING: Mondrian will be unusable after calling this method!
      def self.shutdown_static_mondrian_server!
        static_mondrian_server = Java::MondrianOlap::MondrianServer.forId(nil)

        # force Mondrian to think that static_mondrian_server is not static MondrianServer
        mondrian_server_registry = Java::MondrianServer::MondrianServerRegistry::INSTANCE
        f = mondrian_server_registry.java_class.declared_field("staticServer")
        f.accessible = true
        f.set_value(mondrian_server_registry, nil)

        static_mondrian_server.shutdown

        # shut down expiring reference timer thread
        f = Java::MondrianUtil::ExpiringReference.java_class.declared_field("timer")
        f.accessible = true
        expiring_reference_timer = f.static_value.to_java
        expiring_reference_timer.cancel

        # shut down Mondrian Monitor
        cons = Java::MondrianServer.__send__(:"MonitorImpl$ShutdownCommand").java_class.declared_constructor
        cons.accessible = true
        shutdown_command = cons.new_instance.to_java

        cons = Java::MondrianServer.__send__(:"MonitorImpl$Handler").java_class.declared_constructor
        cons.accessible = true
        handler = cons.new_instance.to_java

        pair = Java::mondrian.util.Pair.new handler, shutdown_command

        f = Java::MondrianServer::MonitorImpl.java_class.declared_field("ACTOR")
        f.accessible = true
        monitor_actor = f.static_value.to_java

        f = monitor_actor.java_class.declared_field("eventQueue")
        f.accessible = true
        event_queue = f.value(monitor_actor)

        event_queue.put pair

        # shut down connection pool thread
        f = Java::mondrian.rolap.RolapConnectionPool.java_class.declared_field("instance")
        f.accessible = true
        rolap_connection_pool = f.static_value.to_java
        f = rolap_connection_pool.java_class.declared_field("mapConnectKeyToPool")
        f.accessible = true
        map_connect_key_to_pool = f.value(rolap_connection_pool)
        map_connect_key_to_pool.values.each do |pool|
          pool.close if pool && !pool.isClosed
        end

        # unregister MBean
        mbs = Java::JavaLangManagement::ManagementFactory.getPlatformMBeanServer
        mbean_name = Java::JavaxManagement::ObjectName.new("mondrian.server:type=Server-#{static_mondrian_server.getId}")
        begin
          mbs.unregisterMBean(mbean_name)
        rescue Java::JavaxManagement::InstanceNotFoundException
        end

        true
      end

      private

      def connection_string
        string = "jdbc:mondrian:Jdbc=#{quote_string(jdbc_uri)};JdbcDrivers=#{jdbc_driver};"
        # by default use content checksum to reload schema when catalog has changed
        string << "UseContentChecksum=true;" unless @params[:use_content_checksum] == false
        string << "PinSchemaTimeout=#{@params[:pin_schema_timeout]};" if @params[:pin_schema_timeout]
        if role = @params[:role] || @params[:roles]
          roles = Array(role).map{|r| r && r.to_s.gsub(',', ',,')}.compact
          string << "Role=#{quote_string(roles.join(','))};" unless roles.empty?
        end
        if locale = @params[:locale]
          string << "Locale=#{quote_string(locale.to_s)};"
        end
        string << (@params[:catalog] ? "Catalog=#{catalog_uri}" : "CatalogContent=#{quote_string(catalog_content)}")
      end

      def jdbc_uri
        case @driver
        when 'mysql', 'postgresql'
          uri = "jdbc:#{@driver}://#{@params[:host]}#{@params[:port] && ":#{@params[:port]}"}/#{@params[:database]}"
          uri << "?useUnicode=yes&characterEncoding=UTF-8" if @driver == 'mysql'
          if (properties = @params[:properties]).is_a?(Hash) && !properties.empty?
            uri << (@driver == 'mysql' ? '&' : '?')
            uri << properties.map{|k, v| "#{k}=#{v}"}.join('&')
          end
          uri
        when 'oracle'
          # connection using TNS alias
          if @params[:database] && !@params[:host] && !@params[:url] && ENV['TNS_ADMIN']
            "jdbc:oracle:thin:@#{@params[:database]}"
          else
            @params[:url] || begin
              database = @params[:database]
              unless database =~ %r{^(:|/)}
                # assume database is a SID if no colon or slash are supplied (backward-compatibility)
                database = ":#{database}"
              end
              "jdbc:oracle:thin:@#{@params[:host] || 'localhost'}:#{@params[:port] || 1521}#{database}"
            end
          end
        when 'luciddb'
          uri = "jdbc:luciddb:http://#{@params[:host]}#{@params[:port] && ":#{@params[:port]}"}"
          uri << ";schema=#{@params[:database_schema]}" if @params[:database_schema]
          uri
        when 'mssql'
          uri = "jdbc:jtds:sqlserver://#{@params[:host]}#{@params[:port] && ":#{@params[:port]}"}/#{@params[:database]}"
          uri << ";instance=#{@params[:instance]}" if @params[:instance]
          uri << ";domain=#{@params[:domain]}" if @params[:domain]
          uri << ";appname=#{@params[:appname]}" if @params[:appname]
          uri
        when 'sqlserver'
          uri = "jdbc:sqlserver://#{@params[:host]}#{@params[:port] && ":#{@params[:port]}"}"
          uri << ";databaseName=#{@params[:database]}" if @params[:database]
          uri << ";integratedSecurity=#{@params[:integrated_security]}" if @params[:integrated_security]
          uri << ";applicationName=#{@params[:application_name]}" if @params[:application_name]
          uri << ";instanceName=#{@params[:instance_name]}" if @params[:instance_name]
          uri
        when 'jdbc'
          @params[:jdbc_url] or raise ArgumentError, 'missing jdbc_url parameter'
        else
          raise ArgumentError, 'unknown JDBC driver'
        end
      end

      def jdbc_driver
        case @driver
        when 'mysql'
          'com.mysql.jdbc.Driver'
        when 'postgresql'
          'org.postgresql.Driver'
        when 'oracle'
          'oracle.jdbc.OracleDriver'
        when 'luciddb'
          'org.luciddb.jdbc.LucidDbClientDriver'
        when 'mssql'
          'net.sourceforge.jtds.jdbc.Driver'
        when 'sqlserver'
          'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        when 'jdbc'
          @params[:jdbc_driver] or raise ArgumentError, 'missing jdbc_driver parameter'
        else
          raise ArgumentError, 'unknown JDBC driver'
        end
      end

      def catalog_uri
        if @params[:catalog]
          "file://#{File.expand_path(@params[:catalog])}"
        else
          raise ArgumentError, 'missing catalog source'
        end
      end

      def catalog_content
        if @params[:catalog_content]
          @params[:catalog_content]
        elsif @params[:schema]
          @params[:schema].to_xml(:driver => @driver)
        else
          raise ArgumentError, "Specify catalog with :catalog, :catalog_content or :schema option"
        end
      end

      def quote_string(string)
        "'#{string.gsub("'", "''")}'"
      end

      def set_statement_parameters(statement, parameters)
        if parameters && !parameters.empty?
          parameters = parameters.dup
          # define addtional parameters which can be accessed from user defined functions
          if define_parameters = parameters.delete(:define_parameters)
            query_validator = statement.getQuery.createValidator
            define_parameters.each do |dp_name, dp_value|
              dp_type_class = dp_value.is_a?(Numeric) ? Java::MondrianOlapType::NumericType : Java::MondrianOlapType::StringType
              query_validator.createOrLookupParam(true, dp_name, dp_type_class.new, nil, nil)
              parameters[dp_name] = dp_value
            end
          end
          if parameters.delete(:profiling)
            statement.enableProfiling(ProfilingHandler.new)
          end
          if timeout = parameters.delete(:timeout)
            statement.getQuery.setQueryTimeoutMillis(timeout * 1000)
          end
          parameters.each do |parameter_name, value|
            statement.getQuery.setParameter(parameter_name, value)
          end
        end
      end

      class ProfilingHandler
        java_implements Java::mondrian.spi.ProfileHandler
        attr_reader :plan
        attr_reader :timing

        java_signature 'void explain(String plan, mondrian.olap.QueryTiming timing)'
        def explain(plan, timing)
          @plan = plan
          @timing = timing
        end
      end

    end
  end
end
