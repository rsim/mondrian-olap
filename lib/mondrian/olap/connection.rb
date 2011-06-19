module Mondrian
  module OLAP
    class Connection
      def self.create(params)
        connection = new(params)
        connection.connect
        connection
      end

      attr_reader :raw_connection, :raw_schema, :raw_schema_reader

      def initialize(params={})
        @params = params
        @driver = params[:driver]
        @connected = false
        @raw_connection = nil
      end

      def connect
        # hack to call private constructor of MondrianOlap4jDriver
        # to avoid using DriverManager which fails to load JDBC drivers
        # because of not seeing JRuby required jar files
        cons = Java::MondrianOlap4j::MondrianOlap4jDriver.java_class.declared_constructor
        cons.accessible = true
        driver = cons.new_instance.to_java

        props = java.util.Properties.new

        # workaround for Mondrian ServiceDiscovery
        current_thread = Java::JavaLang::Thread.currentThread
        class_loader = current_thread.getContextClassLoader
        begin
          current_thread.setContextClassLoader(nil)
          @raw_jdbc_connection = driver.connect(connection_string, props)
        ensure
          current_thread.setContextClassLoader(class_loader)
        end

        @raw_connection = @raw_jdbc_connection.unwrap(Java::OrgOlap4j::OlapConnection.java_class)
        @raw_schema = @raw_connection.getSchema
        @connected = true
        true
      end

      def connected?
        @connected
      end

      def close
        @raw_connection.close
        @connected = false
        @raw_connection = @raw_jdbc_connection = nil
        true
      end

      def execute(query_string)
        statement = @raw_connection.prepareOlapStatement(query_string)
        Result.new(self, statement.executeQuery())
      end

      def from(cube_name)
        Query.from(self, cube_name)
      end

      def cube_names
        @raw_schema.getCubes.map{|c| c.getName}
      end

      def cube(name)
        Cube.get(self, name)
      end

      # Will affect only the next created connection. If it is necessary to clear all schema cache then
      # flush_schema_cache should be called, then close and then new connection should be created.
      def flush_schema_cache
        unwrapped_connection = @raw_connection.unwrap(Java::MondrianOlap::Connection.java_class)
        raw_cache_control = unwrapped_connection.getCacheControl(nil)
        raw_cache_control.flushSchemaCache
      end

      private

      def connection_string
        string = "jdbc:mondrian:Jdbc=#{jdbc_uri};JdbcDrivers=#{jdbc_driver};"
        # by default use content checksum to reload schema when catalog has changed
        string << "UseContentChecksum=true;" unless @params[:use_content_checksum] == false
        string << (@params[:catalog] ? "Catalog=#{catalog_uri}" : "CatalogContent=#{quote_string(catalog_content)}")
      end

      def jdbc_uri
        case @driver
        when 'mysql', 'postgresql'
          uri = "jdbc:#{@driver}://#{@params[:host]}#{@params[:port] && ":#{@params[:port]}"}/#{@params[:database]}" <<
          "?user=#{@params[:username]}&password=#{@params[:password]}"
          uri << "&useUnicode=yes&characterEncoding=UTF-8" if @driver == 'mysql'
          uri
        when 'oracle'
          # connection using TNS alias
          if @params[:database] && !@params[:host] && !@params[:url] && ENV['TNS_ADMIN']
            "jdbc:oracle:thin:#{@params[:username]}/#{@params[:password]}@#{@params[:database]}"
          else
            @params[:url] ||
            "jdbc:oracle:thin:#{@params[:username]}/#{@params[:password]}" <<
            "@#{@params[:host] || 'localhost'}:#{@params[:port] || 1521}:#{@params[:database]}"
          end
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
        "'#{string.gsub("'","''")}'"
      end

    end
  end
end
