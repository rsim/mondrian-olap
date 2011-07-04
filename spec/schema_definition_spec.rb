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
        <Schema description="Demo &quot;FoodMart&quot; schema" name="FoodMart"/>
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
          <Cube cache="false" defaultMeasure="Unit Sales" description="Sales cube" enabled="true" name="Sales"/>
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
          <Cube cache="false" defaultMeasure="Unit Sales" description="Sales cube" enabled="true" name="Sales"/>
        </Schema>
        XML
      end
    end

    describe "Table" do
      it "should render to XML" do
        @schema.define do
          cube 'Sales' do
            table 'sales_fact', :alias => 'sales'
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Table alias="sales" name="sales_fact"/>
          </Cube>
        </Schema>
        XML
      end

      it "should render table name in uppercase when using Oracle or LucidDB driver" do
        @schema.define do
          cube 'Sales' do
            table 'sales_fact', :alias => 'sales', :schema => 'facts'
          end
        end
        %w(oracle luciddb).each do |driver|
          @schema.to_xml(:driver => driver).should be_like <<-XML
          <?xml version="1.0"?>
          <Schema name="default">
            <Cube name="Sales">
              <Table alias="SALES" name="SALES_FACT" schema="FACTS"/>
            </Cube>
          </Schema>
          XML
        end
      end

      it "should render table name in uppercase when :upcase_data_dictionary option is set to true" do
        @schema.define :upcase_data_dictionary => true do
          cube 'Sales' do
            table 'sales_fact', :alias => 'sales', :schema => 'facts'
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Table alias="SALES" name="SALES_FACT" schema="FACTS"/>
          </Cube>
        </Schema>
        XML
      end

      it "should render table name in lowercase when using Oracle or LucidDB driver but with :upcase_data_dictionary set to false" do
        @schema.define :upcase_data_dictionary => false do
          cube 'Sales' do
            table 'sales_fact', :alias => 'sales', :schema => 'facts'
          end
        end
        %w(oracle luciddb).each do |driver|
          @schema.to_xml(:driver => driver).should be_like <<-XML
          <?xml version="1.0"?>
          <Schema name="default">
            <Cube name="Sales">
              <Table alias="sales" name="sales_fact" schema="facts"/>
            </Cube>
          </Schema>
          XML
        end
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
            <Dimension foreignKey="customer_id" name="Gender">
              <Hierarchy allMemberName="All Genders" hasAll="true" primaryKey="customer_id">
                <Table name="customer"/>
                <Level column="gender" name="Gender" uniqueMembers="true"/>
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
            <Dimension foreignKey="time_id" name="Time">
              <Hierarchy hasAll="false" primaryKey="time_id">
                <Table name="time_by_day"/>
                <Level column="the_year" name="Year" type="Numeric" uniqueMembers="true"/>
                <Level column="quarter" name="Quarter" uniqueMembers="false"/>
                <Level column="month_of_year" name="Month" type="Numeric" uniqueMembers="false"/>
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
            <Dimension foreignKey="product_id" name="Products">
              <Hierarchy allMemberName="All Products" hasAll="true" primaryKey="product_id" primaryKeyTable="product">
                <Join leftKey="product_class_id" rightKey="product_class_id">
                  <Table name="product"/>
                  <Table name="product_class"/>
                </Join>
                <Level column="product_family" name="Product Family" table="product_class" uniqueMembers="true"/>
                <Level column="brand_name" name="Brand Name" table="product" uniqueMembers="false"/>
                <Level column="product_name" name="Product Name" table="product" uniqueMembers="true"/>
              </Hierarchy>
            </Dimension>
          </Cube>
        </Schema>
        XML
      end

      it "should render table and column names in uppercase when using Oracle driver" do
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
        @schema.to_xml(:driver => 'oracle').should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Dimension foreignKey="PRODUCT_ID" name="Products">
              <Hierarchy allMemberName="All Products" hasAll="true" primaryKey="PRODUCT_ID" primaryKeyTable="PRODUCT">
                <Join leftKey="PRODUCT_CLASS_ID" rightKey="PRODUCT_CLASS_ID">
                  <Table name="PRODUCT"/>
                  <Table name="PRODUCT_CLASS"/>
                </Join>
                <Level column="PRODUCT_FAMILY" name="Product Family" table="PRODUCT_CLASS" uniqueMembers="true"/>
                <Level column="BRAND_NAME" name="Brand Name" table="PRODUCT" uniqueMembers="false"/>
                <Level column="PRODUCT_NAME" name="Product Name" table="PRODUCT" uniqueMembers="true"/>
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
            <Measure aggregator="sum" column="unit_sales" name="Unit Sales"/>
          </Cube>
        </Schema>
        XML
      end

      it "should render column name in uppercase when using Oracle driver" do
        @schema.define do
          cube 'Sales' do
            measure 'Unit Sales' do
              column 'unit_sales'
              aggregator 'sum'
            end
          end
        end
        @schema.to_xml(:driver => 'oracle').should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Measure aggregator="sum" column="UNIT_SALES" name="Unit Sales"/>
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
              format_string '#,##0.00'
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <CalculatedMember dimension="Measures" formatString="#,##0.00" name="Profit">
              <Formula>[Measures].[Store Sales] - [Measures].[Store Cost]</Formula>
            </CalculatedMember>
          </Cube>
        </Schema>
        XML
      end
    end

    describe "Aggregates" do
      it "should render named aggregate to XML" do
        @schema.define do
          cube 'Sales' do
            table 'sales_fact_1997' do
              agg_name 'agg_c_special_sales_fact_1997' do
                agg_fact_count :column => 'fact_count'
                agg_measure '[Measures].[Store Cost]', :column => 'store_cost_sum'
                agg_measure '[Measures].[Store Sales]', :column => 'store_sales_sum'
                agg_level '[Product].[Product Family]', :column => 'product_family'
                agg_level '[Time].[Quarter]', :column => 'time_quarter'
                agg_level '[Time].[Year]', :column => 'time_year'
                agg_level '[Time].[Quarter]', :column => 'time_quarter'
                agg_level '[Time].[Month]', :column => 'time_month'
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Table name="sales_fact_1997">
              <AggName name="agg_c_special_sales_fact_1997">
                <AggFactCount column="fact_count"/>
                <AggMeasure column="store_cost_sum" name="[Measures].[Store Cost]"/>
                <AggMeasure column="store_sales_sum" name="[Measures].[Store Sales]"/>
                <AggLevel column="product_family" name="[Product].[Product Family]"/>
                <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                <AggLevel column="time_year" name="[Time].[Year]"/>
                <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                <AggLevel column="time_month" name="[Time].[Month]"/>
              </AggName>
            </Table>
          </Cube>
        </Schema>
        XML
      end

      it "should render aggregate pattern to XML" do
        @schema.define do
          cube 'Sales' do
            table 'sales_fact_1997' do
              agg_pattern :pattern => 'agg_.*_sales_fact_1997' do
                agg_fact_count :column => 'fact_count'
                agg_measure '[Measures].[Store Cost]', :column => 'store_cost_sum'
                agg_measure '[Measures].[Store Sales]', :column => 'store_sales_sum'
                agg_level '[Product].[Product Family]', :column => 'product_family'
                agg_level '[Time].[Quarter]', :column => 'time_quarter'
                agg_level '[Time].[Year]', :column => 'time_year'
                agg_level '[Time].[Quarter]', :column => 'time_quarter'
                agg_level '[Time].[Month]', :column => 'time_month'
                agg_exclude 'agg_c_14_sales_fact_1997'
                agg_exclude 'agg_lc_100_sales_fact_1997'
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Table name="sales_fact_1997">
              <AggPattern pattern="agg_.*_sales_fact_1997">
                <AggFactCount column="fact_count"/>
                <AggMeasure column="store_cost_sum" name="[Measures].[Store Cost]"/>
                <AggMeasure column="store_sales_sum" name="[Measures].[Store Sales]"/>
                <AggLevel column="product_family" name="[Product].[Product Family]"/>
                <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                <AggLevel column="time_year" name="[Time].[Year]"/>
                <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                <AggLevel column="time_month" name="[Time].[Month]"/>
                <AggExclude name="agg_c_14_sales_fact_1997"/>
                <AggExclude name="agg_lc_100_sales_fact_1997"/>
              </AggPattern>
            </Table>
          </Cube>
        </Schema>
        XML
      end

      it "should render embedded aggregate XML defintion to XML" do
        @schema.define do
          cube 'Sales' do
            table 'sales_fact_1997' do
              xml <<-XML
                <AggName name="agg_c_special_sales_fact_1997">
                  <AggFactCount column="fact_count"/>
                  <AggMeasure column="store_cost_sum" name="[Measures].[Store Cost]"/>
                  <AggMeasure column="store_sales_sum" name="[Measures].[Store Sales]"/>
                  <AggLevel column="product_family" name="[Product].[Product Family]"/>
                  <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                  <AggLevel column="time_year" name="[Time].[Year]"/>
                  <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                  <AggLevel column="time_month" name="[Time].[Month]"/>
                </AggName>
                <AggPattern pattern="agg_.*_sales_fact_1997">
                  <AggFactCount column="fact_count"/>
                  <AggMeasure column="store_cost_sum" name="[Measures].[Store Cost]"/>
                  <AggMeasure column="store_sales_sum" name="[Measures].[Store Sales]"/>
                  <AggLevel column="product_family" name="[Product].[Product Family]"/>
                  <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                  <AggLevel column="time_year" name="[Time].[Year]"/>
                  <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                  <AggLevel column="time_month" name="[Time].[Month]"/>
                  <AggExclude name="agg_c_14_sales_fact_1997"/>
                  <AggExclude name="agg_lc_100_sales_fact_1997"/>
                </AggPattern>
              XML
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0"?>
        <Schema name="default">
          <Cube name="Sales">
            <Table name="sales_fact_1997">
              <AggName name="agg_c_special_sales_fact_1997">
                <AggFactCount column="fact_count"/>
                <AggMeasure column="store_cost_sum" name="[Measures].[Store Cost]"/>
                <AggMeasure column="store_sales_sum" name="[Measures].[Store Sales]"/>
                <AggLevel column="product_family" name="[Product].[Product Family]"/>
                <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                <AggLevel column="time_year" name="[Time].[Year]"/>
                <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                <AggLevel column="time_month" name="[Time].[Month]"/>
              </AggName>
              <AggPattern pattern="agg_.*_sales_fact_1997">
                <AggFactCount column="fact_count"/>
                <AggMeasure column="store_cost_sum" name="[Measures].[Store Cost]"/>
                <AggMeasure column="store_sales_sum" name="[Measures].[Store Sales]"/>
                <AggLevel column="product_family" name="[Product].[Product Family]"/>
                <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                <AggLevel column="time_year" name="[Time].[Year]"/>
                <AggLevel column="time_quarter" name="[Time].[Quarter]"/>
                <AggLevel column="time_month" name="[Time].[Month]"/>
                <AggExclude name="agg_c_14_sales_fact_1997"/>
                <AggExclude name="agg_lc_100_sales_fact_1997"/>
              </AggPattern>
            </Table>
          </Cube>
        </Schema>
        XML
      end

    end

  end

  describe "connection with schema" do
    before(:all) do
      @schema = Mondrian::OLAP::Schema.define do
        cube 'Sales' do
          table 'sales'
          dimension 'Gender', :foreign_key => 'customer_id' do
            hierarchy :has_all => true, :primary_key => 'id' do
              table 'customers'
              level 'Gender', :column => 'gender', :unique_members => true
            end
          end
          dimension 'Time', :foreign_key => 'time_id' do
            hierarchy :has_all => false, :primary_key => 'id' do
              table 'time'
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
        rows('descendants([Time].[2010].[Q1])').
        where('[Gender].[F]').
        execute.should be_a(Mondrian::OLAP::Result)
    end

  end

end