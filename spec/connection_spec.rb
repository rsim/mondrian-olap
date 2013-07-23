require "spec_helper"

describe "Connection" do

  describe "create" do
    before(:each) do
      @olap = Mondrian::OLAP::Connection.new(CONNECTION_PARAMS_WITH_CATALOG)
    end

    it "should not be connected before connection" do
      @olap.should_not be_connected
    end

    it "should be successful" do
      @olap.connect.should be_true
    end

  end

  describe "create with catalog content" do
    before(:all) do
      @schema_xml = File.read(CATALOG_FILE)
    end
    it "should be successful" do
      @olap = Mondrian::OLAP::Connection.new(CONNECTION_PARAMS.merge(
        :catalog_content => @schema_xml
      ))
      @olap.connect.should be_true
    end

  end

  describe "properties" do
    before(:all) do
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG)
    end

    it "should be connected" do
      @olap.should be_connected
    end

    # to check that correct database dialect is loaded by ServiceDiscovery detected class loader
    it "should use corresponding Mondrian dialect" do
      # read private "schema" field
      schema_field = @olap.raw_schema.getClass.getDeclaredField("schema")
      schema_field.setAccessible(true)
      private_schema = schema_field.get(@olap.raw_schema)
      private_schema.getDialect.java_class.name.should == case MONDRIAN_DRIVER
        when 'mysql' then 'mondrian.spi.impl.MySqlDialect'
        when 'postgresql' then 'mondrian.spi.impl.PostgreSqlDialect'
        when 'oracle' then 'mondrian.spi.impl.OracleDialect'
        when 'luciddb' then 'mondrian.spi.impl.LucidDbDialect'
        when 'mssql' then 'mondrian.spi.impl.MicrosoftSqlServerDialect'
        when 'sqlserver' then 'mondrian.spi.impl.MicrosoftSqlServerDialect'
        end
    end

  end

  describe "locale" do
    %w(en en_US de de_DE).each do |locale|
      it "should set #{locale} locale from connection parameters" do
        @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG.merge(:locale => locale))
        @olap.locale.should == locale
        @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG.merge(:locale => locale.to_sym))
        @olap.locale.should == locale.to_s
      end

      it "should set #{locale} locale using setter method" do
        @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG)
        @olap.locale = locale
        @olap.locale.should == locale
        @olap.locale = locale.to_sym
        @olap.locale.should == locale.to_s
      end
    end
  end

  describe "close" do
    before(:all) do
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG)
    end

    it "should not be connected after close" do
      @olap.close
      @olap.should_not be_connected
    end

  end

end
