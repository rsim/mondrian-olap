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