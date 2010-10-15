module Mondrian
  module OLAP
    class Connection
      def self.create(params)
        connection = new(params)
        connection.connect
        connection
      end

      attr_reader :raw_connection

      def initialize(params={})
        @params = params
        @driver = params[:driver]
        @connected = false
        @raw_connection = nil
      end

      def connect
        @raw_connection = Java::mondrian.olap.DriverManager.getConnection(connection_string, nil)
        @connected = true
        true
      end

      def connected?
        @connected
      end

      def close
        @raw_connection.close
        @connected = false
        @raw_connection = nil
        true
      end

      def execute(query_string)
        query = @raw_connection.parseQuery(query_string)
        Result.new(@raw_connection.execute(query))
      end

      def from(cube_name)
        Query.from(self, cube_name)
      end

      private

      def connection_string
        "Provider=mondrian;Jdbc=#{jdbc_uri};JdbcDrivers=#{jdbc_driver};" <<
          (@params[:catalog] ? "Catalog=#{catalog_uri}" : "CatalogContent=#{catalog_content}")
      end

      def jdbc_uri
        case @driver
        when 'mysql'
          "jdbc:mysql://#{@params[:host]}#{@params[:port] && ":#{@params[:port]}"}/#{@params[:database]}" <<
          "?user=#{@params[:username]}&password=#{@params[:password]}"
        else
          raise ArgumentError, 'unknown JDBC driver'
        end
      end

      def jdbc_driver
        case @driver
        when 'mysql'
          'com.mysql.jdbc.Driver'
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
          @params[:schema].to_xml
        else
          raise ArgumentError, "Specify catalog with :catalog, :catalog_content or :schema option"
        end
      end

    end
  end
end
