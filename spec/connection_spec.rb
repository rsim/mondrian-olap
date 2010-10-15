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
      @schema_xml = <<-XML
<?xml version="1.0"?>
<Schema name="FoodMart">
  <Cube name="Sales">
    <Table name="sales_fact_1997"/>
    <Dimension name="Gender" foreignKey="customer_id">
      <Hierarchy hasAll="true" allMemberName="All Genders" primaryKey="customer_id">
        <Table name="customer"/>
        <Level name="Gender" column="gender" uniqueMembers="true"/>
      </Hierarchy>
    </Dimension>
    <Measure name="Unit Sales" column="unit_sales" aggregator="sum" formatString="#,###"/>
  </Cube>
</Schema>
XML
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