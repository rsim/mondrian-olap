require "spec_helper"

describe "Schema definition" do

  describe "elements" do
    before(:each) do
      @schema = Mondrian::OLAP::Schema.new
    end

    describe "root element" do
      it "should render to XML" do
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema/>
        XML
      end

      it "should render to XML with attributes" do
        @schema.define('FoodMart') do
          description 'Demo "FoodMart" schema'
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="FoodMart" description="Demo &quot;FoodMart&quot; schema"/>
        XML
      end

      it "should render to XML using class method" do
        schema = Mondrian::OLAP::Schema.define('FoodMart')
        schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="FoodMart"/>
        XML
      end
    end

    describe "Cube" do
      it "should render to XML" do
        @schema.define do
          cube 'Sales' do
            default_measure 'Unit Sales'
            description 'Sales cube'
            cache false
            enabled true
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales" defaultMeasure="Unit Sales" description="Sales cube" cache="false" enabled="true"/>
        </Schema>
        XML
      end

      it "should render to XML using options hash" do
        @schema.define do
          cube 'Sales', :default_measure => 'Unit Sales',
            :description => 'Sales cube', :cache => false, :enabled => true
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales" defaultMeasure="Unit Sales" description="Sales cube" cache="false" enabled="true"/>
        </Schema>
        XML
      end
    end

    describe "Table" do
      it "should render to XML" do
        @schema.define do
          cube 'Sales' do
            table 'sales_fact_1997', :alias => 'sales'
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Table name="sales_fact_1997" alias="sales"/>
          </Cube>
        </Schema>
        XML
      end
    end

    describe "Dimension" do
      it "should render to XML" do
        @schema.define do
          cube 'Sales' do
            dimension 'Gender' do
              foreign_key 'customer_id'
              hierarchy do
                has_all true
                all_member_name 'All Genders'
                primary_key 'customer_id'
                table 'customer'
                level 'Gender', :column => 'gender', :unique_members => true
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Dimension name="Gender" foreignKey="customer_id">
              <Hierarchy hasAll="true" allMemberName="All Genders" primaryKey="customer_id">
                <Table name="customer"/>
                <Level name="Gender" column="gender" uniqueMembers="true"/>
              </Hierarchy>
            </Dimension>
          </Cube>
        </Schema>
        XML
      end

      it "should render time dimension" do
        @schema.define do
          cube 'Sales' do
            dimension 'Time' do
              foreign_key 'time_id'
              hierarchy do
                has_all false
                primary_key 'time_id'
                table 'time_by_day'
                level 'Year', :column => 'the_year', :type => 'Numeric', :unique_members => true
                level 'Quarter', :column => 'quarter', :unique_members => false
                level 'Month', :column => 'month_of_year', :type => 'Numeric', :unique_members => false
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Dimension name="Time" foreignKey="time_id">
              <Hierarchy hasAll="false" primaryKey="time_id">
                <Table name="time_by_day"/>
                <Level name="Year" column="the_year" type="Numeric" uniqueMembers="true"/>
                <Level name="Quarter" column="quarter" uniqueMembers="false"/>
                <Level name="Month" column="month_of_year" type="Numeric" uniqueMembers="false"/>
              </Hierarchy>
            </Dimension>
          </Cube>
        </Schema>
        XML
      end

      it "should render dimension hierarchy with join" do
        @schema.define do
          cube 'Sales' do
            dimension 'Products', :foreign_key => 'product_id' do
              hierarchy :has_all => true, :all_member_name => 'All Products',
                        :primary_key => 'product_id', :primary_key_table => 'product' do
                join :left_key => 'product_class_id', :right_key => 'product_class_id' do
                  table 'product'
                  table 'product_class'
                end
                level 'Product Family', :table => 'product_class', :column => 'product_family', :unique_members => true
                level 'Brand Name', :table => 'product', :column => 'brand_name', :unique_members => false
                level 'Product Name', :table => 'product', :column => 'product_name', :unique_members => true
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Dimension name="Products" foreignKey="product_id">
              <Hierarchy hasAll="true" allMemberName="All Products" primaryKey="product_id" primaryKeyTable="product">
                <Join leftKey="product_class_id" rightKey="product_class_id">
                  <Table name="product"/>
                  <Table name="product_class"/>
                </Join>
                <Level name="Product Family" table="product_class" column="product_family" uniqueMembers="true"/>
                <Level name="Brand Name" table="product" column="brand_name" uniqueMembers="false"/>
                <Level name="Product Name" table="product" column="product_name" uniqueMembers="true"/>
              </Hierarchy>
            </Dimension>
          </Cube>
        </Schema>
        XML
      end

    end

    describe "Measure" do
      it "should render XML" do
        @schema.define do
          cube 'Sales' do
            measure 'Unit Sales' do
              column 'unit_sales'
              aggregator 'sum'
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Measure name="Unit Sales" column="unit_sales" aggregator="sum"/>
          </Cube>
        </Schema>
        XML
      end
    end

    describe "Calculated Member" do
      it "should render XML" do
        @schema.define do
          cube 'Sales' do
            calculated_member 'Profit' do
              dimension 'Measures'
              formula '[Measures].[Store Sales] - [Measures].[Store Cost]'
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <CalculatedMember name="Profit" dimension="Measures" formula="[Measures].[Store Sales] - [Measures].[Store Cost]"/>
          </Cube>
        </Schema>
        XML
      end
    end
  end

  describe "connection with schema" do
    before(:all) do
      @schema = Mondrian::OLAP::Schema.new
      @schema.define do
        cube 'Sales' do
          table 'sales_fact_1997'
          dimension 'Gender', :foreign_key => 'customer_id' do
            hierarchy :has_all => true, :primary_key => 'customer_id' do
              table 'customer'
              level 'Gender', :column => 'gender', :unique_members => true
            end
          end
          dimension 'Time', :foreign_key => 'time_id' do
            hierarchy :has_all => false, :primary_key => 'time_id' do
              table 'time_by_day'
              level 'Year', :column => 'the_year', :type => 'Numeric', :unique_members => true
              level 'Quarter', :column => 'quarter', :unique_members => false
              level 'Month', :column => 'month_of_year', :type => 'Numeric', :unique_members => false
            end
          end
          measure 'Unit Sales', :column => 'unit_sales', :aggregator => 'sum'
          measure 'Store Sales', :column => 'store_sales', :aggregator => 'sum'
        end
      end
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema)
    end

    it "should connect" do
      @olap.should be_connected
    end

    it "should execute query" do
      @olap.from('Sales').
        columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
        rows('descendants([Time].[1997].[Q1])').
        where('[Gender].[F]').
        execute.should be_a(Mondrian::OLAP::Result)
    end

  end

end