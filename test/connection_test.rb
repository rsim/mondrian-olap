# frozen_string_literal: true

require_relative "test_helper"

describe "Connection" do

  describe "create" do
    before do
      @olap = Mondrian::OLAP::Connection.new(CONNECTION_PARAMS_WITH_CATALOG)
    end

    it "should not be connected before connection" do
      assert_equal false, @olap.connected?
    end

    it "should be successful" do
      assert_equal true, @olap.connect
    end

  end

  describe "create with catalog content" do
    before(:all) do
      @schema_xml = File.read(CATALOG_FILE)
    end
    it "should be successful" do
      @olap = Mondrian::OLAP::Connection.new(CONNECTION_PARAMS.merge(
        catalog_content: @schema_xml
      ))
      assert_equal true, @olap.connect
    end

  end

  describe "properties" do
    before(:all) do
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG)
    end

    it "should be connected" do
      assert @olap.connected?
    end

    # to check that correct database dialect is loaded by ServiceDiscovery detected class loader
    it "should use corresponding Mondrian dialect" do
      # read private "schema" field
      schema_field = @olap.raw_schema.getClass.getDeclaredField("schema")
      schema_field.setAccessible(true)
      private_schema = schema_field.get(@olap.raw_schema)
      assert_equal \
        case MONDRIAN_DRIVER.split('_').last
        when 'mysql' then 'mondrian.spi.impl.MySqlDialect'
        when 'postgresql' then 'mondrian.spi.impl.PostgreSqlDialect'
        when 'oracle' then 'mondrian.spi.impl.OracleDialect'
        when 'sqlserver' then 'mondrian.spi.impl.MicrosoftSqlServerDialect'
        when 'vertica' then 'mondrian.spi.impl.VerticaDialect'
        when 'snowflake' then 'mondrian.spi.impl.SnowflakeDialect'
        when 'clickhouse' then 'mondrian.spi.impl.ClickHouseDialect'
        when 'mariadb' then 'mondrian.spi.impl.MariaDBDialect'
        end,
        private_schema.getDialect.java_class.name
    end

    it "should access Mondrian server" do
      assert @olap.mondrian_server
    end
  end

  describe "locale" do
    %w(en en_US de de_DE).each do |locale|
      it "should set #{locale} locale from connection parameters" do
        @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG.merge(locale: locale))
        assert_equal locale, @olap.locale
        @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG.merge(locale: locale.to_sym))
        assert_equal locale.to_s, @olap.locale
      end

      it "should set #{locale} locale using setter method" do
        @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG)
        @olap.locale = locale
        assert_equal locale, @olap.locale
        @olap.locale = locale.to_sym
        assert_equal locale.to_s, @olap.locale
      end
    end
  end

  describe "close" do
    before(:all) do
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG)
    end

    it "should not be connected after close" do
      @olap.close
      assert_equal false, @olap.connected?
    end

  end

  describe "jdbc_uri" do
    before(:all) { @olap_connection = Mondrian::OLAP::Connection }

    describe "SQL Server" do
      it "should return a valid JDBC URI" do
        assert_equal 'jdbc:sqlserver://example.com:1234;databaseName=example_db;instanceName=MSSQLSERVER',
          @olap_connection.new(
            driver: 'sqlserver',
            host: 'example.com',
            port: 1234,
            instance: 'MSSQLSERVER',
            database: 'example_db'
          ).jdbc_uri
      end

      it "should return a valid JDBC URI with instance name as property" do
        assert_equal 'jdbc:sqlserver://example.com;instanceName=MSSQLSERVER',
          @olap_connection.new(
            driver: 'sqlserver',
            host: 'example.com',
            properties: {
              instanceName: "MSSQLSERVER"
            }
          ).jdbc_uri
      end

      it "should return a valid JDBC URI with enabled integratedSecurity" do
        assert_equal 'jdbc:sqlserver://example.com;integratedSecurity=true',
          @olap_connection.new(
            driver: 'sqlserver',
            host: 'example.com',
            integrated_security: 'true'
          ).jdbc_uri
      end
    end
  end
end
