# encoding: utf-8

require "spec_helper"
require "coffee-script"

describe "Schema definition" do

  describe "elements" do
    before(:each) do
      @schema = Mondrian::OLAP::Schema.new
    end

    describe "root element" do
      it "should render to XML" do
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema/>
        XML
      end

      it "should render to XML with attributes" do
        @schema.define('FoodMart') do
          description 'Demo "FoodMart" schema āčē'
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema description="Demo &quot;FoodMart&quot; schema āčē" name="FoodMart"/>
        XML
      end

      it "should render to XML using class method" do
        schema = Mondrian::OLAP::Schema.define('FoodMart')
        schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
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
        <?xml version="1.0" encoding="UTF-8"?>
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
        <?xml version="1.0" encoding="UTF-8"?>
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
        <?xml version="1.0" encoding="UTF-8"?>
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
          <?xml version="1.0" encoding="UTF-8"?>
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
        <?xml version="1.0" encoding="UTF-8"?>
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
          <?xml version="1.0" encoding="UTF-8"?>
          <Schema name="default">
            <Cube name="Sales">
              <Table alias="sales" name="sales_fact" schema="facts"/>
            </Cube>
          </Schema>
          XML
        end
      end

      it "should render table with where condition" do
        @schema.define do
          cube 'Sales' do
            table 'sales_fact', :alias => 'sales' do
              sql 'customer_id IS NOT NULL'
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Table alias="sales" name="sales_fact">
              <SQL>customer_id IS NOT NULL</SQL>
            </Table>
          </Cube>
        </Schema>
        XML
      end
    end

    describe "View" do
      it "should render to XML" do
        @schema.define do
          cube 'Sales' do
            view :alias => 'sales' do
              sql 'select * from sales_fact'
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <View alias="sales">
              <SQL>select * from sales_fact</SQL>
            </View>
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
        <?xml version="1.0" encoding="UTF-8"?>
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
        <?xml version="1.0" encoding="UTF-8"?>
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

      it "should render dimension with hierarchy and level defaults" do
        @schema.define do
          cube 'Sales' do
            dimension 'Time' do
              foreign_key 'time_id'
              hierarchy do
                all_member_name 'All Times' # should add :has_all => true
                primary_key 'time_id'
                table 'time_by_day'
                level 'Year', :column => 'the_year', :type => 'Numeric' # first level should have default :unique_members => true
                level 'Quarter', :column => 'quarter' # next levels should have default :unique_members => false
                level 'Month', :column => 'month_of_year', :type => 'Numeric'
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Dimension foreignKey="time_id" name="Time">
              <Hierarchy allMemberName="All Times" hasAll="true" primaryKey="time_id">
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
        <?xml version="1.0" encoding="UTF-8"?>
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
        <?xml version="1.0" encoding="UTF-8"?>
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

      it "should render dimension hierarchy with nested joins" do
        @schema.define do
          cube 'Sales' do
            dimension 'Products', :foreign_key => 'product_id' do
              hierarchy :has_all => true, :all_member_name => 'All Products',
                        :primary_key => 'product_id', :primary_key_table => 'product' do
                join :left_key => 'product_class_id', :right_alias => 'product_class', :right_key => 'product_class_id' do
                  table 'product'
                  join :left_key  => 'product_type_id', :right_key => 'product_type_id' do
                    table 'product_class'
                    table 'product_type'
                  end
                end
                level 'Product Family', :table => 'product_type', :column => 'product_family', :unique_members => true
                level 'Product Category', :table => 'product_class', :column => 'product_category', :unique_members => false
                level 'Brand Name', :table => 'product', :column => 'brand_name', :unique_members => false
                level 'Product Name', :table => 'product', :column => 'product_name', :unique_members => true
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Dimension foreignKey="product_id" name="Products">
              <Hierarchy allMemberName="All Products" hasAll="true" primaryKey="product_id" primaryKeyTable="product">
                <Join leftKey="product_class_id" rightAlias="product_class" rightKey="product_class_id">
                  <Table name="product"/>
                  <Join leftKey="product_type_id" rightKey="product_type_id">
                    <Table name="product_class"/>
                    <Table name="product_type"/>
                  </Join>
                </Join>
                <Level column="product_family" name="Product Family" table="product_type" uniqueMembers="true"/>
                <Level column="product_category" name="Product Category" table="product_class" uniqueMembers="false"/>
                <Level column="brand_name" name="Brand Name" table="product" uniqueMembers="false"/>
                <Level column="product_name" name="Product Name" table="product" uniqueMembers="true"/>
              </Hierarchy>
            </Dimension>
          </Cube>
        </Schema>
        XML
      end

    end

    describe "Shared dimension" do
      it "should render to XML" do
        @schema.define do
          dimension 'Gender' do
            hierarchy do
              has_all true
              all_member_name 'All Genders'
              primary_key 'customer_id'
              table 'customer'
              level 'Gender', :column => 'gender', :unique_members => true
            end
          end
          cube 'Sales' do
            dimension_usage 'Gender', :foreign_key => 'customer_id' # by default :source => 'Gender' will be added
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Dimension name="Gender">
            <Hierarchy allMemberName="All Genders" hasAll="true" primaryKey="customer_id">
              <Table name="customer"/>
              <Level column="gender" name="Gender" uniqueMembers="true"/>
            </Hierarchy>
          </Dimension>
          <Cube name="Sales">
            <DimensionUsage foreignKey="customer_id" name="Gender" source="Gender"/>
          </Cube>
        </Schema>
        XML
      end
    end

    describe "Virtual cube" do
      it "should render to XML" do
        @schema.define do
          virtual_cube 'Warehouse and Sales', :default_measure => 'Store Sales' do
            virtual_cube_dimension 'Customers', :cube_name => 'Sales'
            virtual_cube_dimension 'Product'
            virtual_cube_measure '[Measures].[Store Sales]', :cube_name => 'Sales'
            calculated_member 'Profit' do
              dimension 'Measures'
              formula '[Measures].[Store Sales] - [Measures].[Store Cost]'
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <VirtualCube defaultMeasure="Store Sales" name="Warehouse and Sales">
            <VirtualCubeDimension cubeName="Sales" name="Customers"/>
            <VirtualCubeDimension name="Product"/>
            <VirtualCubeMeasure cubeName="Sales" name="[Measures].[Store Sales]"/>
            <CalculatedMember dimension="Measures" name="Profit">
              <Formula>[Measures].[Store Sales] - [Measures].[Store Cost]</Formula>
            </CalculatedMember>
          </VirtualCube>
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
            measure 'Store Sales', :column => 'store_sales' # by default should use sum aggregator
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Measure aggregator="sum" column="unit_sales" name="Unit Sales"/>
            <Measure aggregator="sum" column="store_sales" name="Store Sales"/>
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
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Measure aggregator="sum" column="UNIT_SALES" name="Unit Sales"/>
          </Cube>
        </Schema>
        XML
      end

      it "should render with measure expression" do
        @schema.define do
          cube 'Sales' do
            measure 'Double Unit Sales', :aggregator => 'sum' do
              measure_expression do
                sql 'unit_sales * 2'
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Measure aggregator="sum" name="Double Unit Sales">
              <MeasureExpression>
                <SQL>unit_sales * 2</SQL>
              </MeasureExpression>
            </Measure>
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
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <CalculatedMember dimension="Measures" formatString="#,##0.00" name="Profit">
              <Formula>[Measures].[Store Sales] - [Measures].[Store Cost]</Formula>
            </CalculatedMember>
          </Cube>
        </Schema>
        XML
      end

      it "should render embedded cube XML defintion before additional calculated member to XML" do
        @schema.define do
          cube 'Sales' do
            xml <<-XML
              <Table name="sales_fact_1997"/>
            XML
            calculated_member 'Profit' do
              dimension 'Measures'
              formula '[Measures].[Store Sales] - [Measures].[Store Cost]'
              format_string '#,##0.00'
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Table name="sales_fact_1997"/>
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
        <?xml version="1.0" encoding="UTF-8"?>
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
        <?xml version="1.0" encoding="UTF-8"?>
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
        <?xml version="1.0" encoding="UTF-8"?>
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

    describe "Member properties" do
      it "should render XML" do
        @schema.define do
          cube 'Sales' do
            dimension 'Employees', :foreign_key => 'employee_id' do
              hierarchy :has_all => true, :all_member_name => 'All Employees', :primary_key => 'employee_id' do
                table 'employee'
                level 'Employee Id', :unique_members => true, :type => 'Numeric', :column => 'employee_id', :name_column => 'full_name',
                                      :parent_column => 'supervisor_id', :null_parent_value => 0 do
                  property 'Marital Status', :column => 'marital_status'
                  property 'Position Title', :column => 'position_title'
                  property 'Gender', :column => 'gender'
                  property 'Salary', :column => 'salary'
                  property 'Education Level', :column => 'education_level'
                end
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Dimension foreignKey="employee_id" name="Employees">
              <Hierarchy allMemberName="All Employees" hasAll="true" primaryKey="employee_id">
                <Table name="employee"/>
                <Level column="employee_id" name="Employee Id" nameColumn="full_name" nullParentValue="0" parentColumn="supervisor_id" type="Numeric" uniqueMembers="true">
                  <Property column="marital_status" name="Marital Status"/>
                  <Property column="position_title" name="Position Title"/>
                  <Property column="gender" name="Gender"/>
                  <Property column="salary" name="Salary"/>
                  <Property column="education_level" name="Education Level"/>
                </Level>
              </Hierarchy>
            </Dimension>
          </Cube>
        </Schema>
        XML
      end
    end

    describe "Element annotations" do
      it "should render XML from block of elements" do
        @schema.define do
          cube 'Sales' do
            annotations do
              annotation 'key1', 'value1'
              annotation 'key2', 'value2'
            end
            measure 'Unit Sales', :column => 'unit_sales' do
              annotations do
                annotation 'key3', 'value3'
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Annotations>
              <Annotation name="key1">value1</Annotation>
              <Annotation name="key2">value2</Annotation>
            </Annotations>
            <Measure aggregator="sum" column="unit_sales" name="Unit Sales">
              <Annotations>
                <Annotation name="key3">value3</Annotation>
              </Annotations>
            </Measure>
          </Cube>
        </Schema>
        XML
      end

      it "should render XML from hash options" do
        @schema.define do
          cube 'Sales' do
            annotations :key1 => 'value1', :key2 => 'value2'
            measure 'Unit Sales', :column => 'unit_sales', :annotations => {:key3 => 'value3'}
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Annotations>
              <Annotation name="key1">value1</Annotation>
              <Annotation name="key2">value2</Annotation>
            </Annotations>
            <Measure aggregator="sum" column="unit_sales" name="Unit Sales">
              <Annotations>
                <Annotation name="key3">value3</Annotation>
              </Annotations>
            </Measure>
          </Cube>
        </Schema>
        XML
      end
    end

    describe "User defined functions and formatters in JavaScript" do
      before(:each) do
        @schema.define do
          cube 'Sales' do
            table 'sales'
            dimension 'Customers', :foreign_key => 'customer_id' do
              hierarchy :has_all => true, :all_member_name => 'All Customers', :primary_key => 'id' do
                table 'customers'
                level 'Name', :column => 'fullname' do
                  member_formatter { javascript "return member.getName().toUpperCase();" }
                  property 'City', :column => 'city' do
                    property_formatter { javascript "return propertyValue.toUpperCase();" }
                  end
                end
              end
            end
            calculated_member 'Factorial' do
              dimension 'Measures'
              formula 'Factorial(6)'
              cell_formatter do
                javascript <<-JS
                  var s = value.toString();
                  while (s.length < 20) {
                    s = "0" + s;
                  }
                  return s;
                JS
              end
            end
            calculated_member 'City' do
              dimension 'Measures'
              formula "[Customers].CurrentMember.Properties('City')"
            end
          end
          user_defined_function 'Factorial' do
            javascript <<-JS
              function getParameterTypes() {
                return new Array(
                  new mondrian.olap.type.NumericType());
              }
              function getReturnType(parameterTypes) {
                return new mondrian.olap.type.NumericType();
              }
              function execute(evaluator, arguments) {
                var n = arguments[0].evaluateScalar(evaluator);
                return factorial(n);
              }
              function factorial(n) {
                return n <= 1 ? 1 : n * factorial(n - 1);
              }
            JS
          end
        end
        @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema)
      end

      it "should render XML" do
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Table name="sales"/>
            <Dimension foreignKey="customer_id" name="Customers">
              <Hierarchy allMemberName="All Customers" hasAll="true" primaryKey="id">
                <Table name="customers"/>
                <Level column="fullname" name="Name" uniqueMembers="true">
                  <MemberFormatter>
                    <Script language="JavaScript">return member.getName().toUpperCase();</Script>
                  </MemberFormatter>
                  <Property column="city" name="City">
                    <PropertyFormatter>
                      <Script language="JavaScript">return propertyValue.toUpperCase();</Script>
                    </PropertyFormatter>
                  </Property>
                </Level>
              </Hierarchy>
            </Dimension>
            <CalculatedMember dimension="Measures" name="Factorial">
              <Formula>Factorial(6)</Formula>
              <CellFormatter>
                <Script language="JavaScript">
                  var s = value.toString();
                  while (s.length &lt; 20) {
                    s = "0" + s;
                  }
                  return s;
                </Script>
              </CellFormatter>
            </CalculatedMember>
            <CalculatedMember dimension="Measures" name="City">
              <Formula>[Customers].CurrentMember.Properties('City')</Formula>
            </CalculatedMember>
          </Cube>
          <UserDefinedFunction name="Factorial">
            <Script language="JavaScript">
              function getParameterTypes() {
                return new Array(
                  new mondrian.olap.type.NumericType());
              }
              function getReturnType(parameterTypes) {
                return new mondrian.olap.type.NumericType();
              }
              function execute(evaluator, arguments) {
                var n = arguments[0].evaluateScalar(evaluator);
                return factorial(n);
              }
              function factorial(n) {
                return n &lt;= 1 ? 1 : n * factorial(n - 1);
              }
            </Script>
          </UserDefinedFunction>
        </Schema>
        XML
      end

      it "should execute user defined function" do
        result = @olap.from('Sales').columns('[Measures].[Factorial]').execute
        value = 1*2*3*4*5*6
        result.values.should == [value]
        result.formatted_values.should == ["%020d" % value]
      end

      it "should format members and properties" do
        result = @olap.from('Sales').columns('[Measures].[City]').rows('[Customers].[All Customers].Children').execute
        result.row_members.each_with_index do |member, i|
          member.caption.should == member.name.upcase
          city = member.property_value('City')
          result.formatted_values[i].first.should == city
          member.property_formatted_value('City').should == city.upcase
        end
      end
    end

    describe "User defined functions and formatters in CoffeeScript" do
      before(:each) do
        @schema.define do
          cube 'Sales' do
            table 'sales'
            dimension 'Customers', :foreign_key => 'customer_id' do
              hierarchy :has_all => true, :all_member_name => 'All Customers', :primary_key => 'id' do
                table 'customers'
                level 'Name', :column => 'fullname' do
                  member_formatter { coffeescript "member.getName().toUpperCase()" }
                  property 'City', :column => 'city' do
                    property_formatter { coffeescript "propertyValue.toUpperCase()" }
                  end
                end
              end
            end
            calculated_member 'Factorial' do
              dimension 'Measures'
              formula 'Factorial(6)'
              cell_formatter do
                coffeescript <<-JS
                  s = value.toString()
                  s = "0" + s while s.length < 20
                  s
                JS
              end
            end
            calculated_member 'City' do
              dimension 'Measures'
              formula "[Customers].CurrentMember.Properties('City')"
            end
          end
          user_defined_function 'Factorial' do
            coffeescript <<-JS
              parameters: ["Numeric"]
              returns: "Numeric"
              execute: (n) ->
                if n <= 1 then 1 else n * @execute(n - 1)
            JS
          end
          user_defined_function 'UpperName' do
            coffeescript <<-JS
              parameters: ["Member"]
              returns: "String"
              syntax: "Property"
              execute: (member) ->
                member.getName().toUpperCase()
            JS
          end
          user_defined_function 'toUpperName' do
            coffeescript <<-JS
              parameters: ["Member", "String"]
              returns: "String"
              syntax: "Method"
              execute: (member, dummy) ->
                member.getName().toUpperCase()
            JS
          end
        end
        @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema)
      end

      it "should execute user defined function" do
        result = @olap.from('Sales').columns('[Measures].[Factorial]').execute
        value = 1*2*3*4*5*6
        result.values.should == [value]
        result.formatted_values.should == ["%020d" % value]
      end

      it "should format members and properties" do
        result = @olap.from('Sales').columns('[Measures].[City]').rows('[Customers].[All Customers].Children').execute
        result.row_members.each_with_index do |member, i|
          member.caption.should == member.name.upcase
          city = member.property_value('City')
          result.formatted_values[i].first.should == city
          member.property_formatted_value('City').should == city.upcase
        end
      end

      it "should execute user defined property on member" do
        result = @olap.from('Sales').
          with_member('[Measures].[Upper Name]').as('[Customers].CurrentMember.UpperName').
          columns('[Measures].[Upper Name]').rows('[Customers].Children').execute
        result.row_members.each_with_index do |member, i|
          result.values[i].should == [member.name.upcase]
        end
      end

      it "should execute user defined method on member" do
        result = @olap.from('Sales').
          with_member('[Measures].[Upper Name]').as("[Customers].CurrentMember.toUpperName('dummy')").
          columns('[Measures].[Upper Name]').rows('[Customers].Children').execute
        result.row_members.each_with_index do |member, i|
          result.values[i].should == [member.name.upcase]
        end
      end
    end

    describe "User defined functions and formatters in Ruby" do
      before(:each) do
        @schema.define do
          cube 'Sales' do
            table 'sales'
            dimension 'Customers', :foreign_key => 'customer_id' do
              hierarchy :has_all => true, :all_member_name => 'All Customers', :primary_key => 'id' do
                table 'customers'
                level 'Name', :column => 'fullname' do
                  member_formatter { ruby {|member| member.getName().upcase } }
                  property 'City', :column => 'city' do
                    property_formatter { ruby {|member, property_name, property_value| property_value.upcase} }
                  end
                end
              end
            end
            calculated_member 'Factorial' do
              dimension 'Measures'
              formula 'Factorial(6)'
              cell_formatter { ruby {|value| "%020d" % value} }
            end
            calculated_member 'City' do
              dimension 'Measures'
              formula "[Customers].CurrentMember.Properties('City')"
            end
          end
          user_defined_function 'Factorial' do
            ruby do
              parameters :numeric
              returns :numeric
              def call(n)
                n <= 1 ? 1 : n * call(n - 1)
              end
            end
          end
          user_defined_function 'UpperName' do
            ruby do
              parameters :member
              returns :string
              syntax :property
              def call(member)
                member.getName.upcase
              end
            end
          end
          user_defined_function 'toUpperName' do
            ruby do
              parameters :member, :string
              returns :string
              syntax :method
              def call(member, dummy)
                member.getName.upcase
              end
            end
          end
          user_defined_function 'firstUpperName' do
            ruby do
              parameters :set
              returns :string
              syntax :property
              def call(set)
                set.first.getName.upcase
              end
            end
          end
          user_defined_function 'firstToUpperName' do
            ruby do
              parameters :set, :string
              returns :string
              syntax :method
              def call(set, dummy)
                set.first.getName.upcase
              end
            end
          end
          user_defined_function 'firstChildUpperName' do
            ruby do
              parameters :hierarchy
              returns :string
              syntax :property
              def call_with_evaluator(evaluator, hierarchy)
                evaluator.getSchemaReader.getMemberChildren(hierarchy.getDefaultMember).first.getName.upcase
              end
            end
          end
          user_defined_function 'firstLevelChildUpperName' do
            ruby do
              parameters :level
              returns :string
              syntax :property
              def call_with_evaluator(evaluator, level)
                evaluator.getSchemaReader.getLevelMembers(level, false).first.getName.upcase
              end
            end
          end
        end
        @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema)
      end

      it "should execute user defined function" do
        result = @olap.from('Sales').columns('[Measures].[Factorial]').execute
        value = 1*2*3*4*5*6
        result.values.should == [value]
        result.formatted_values.should == ["%020d" % value]
      end

      it "should format members and properties" do
        result = @olap.from('Sales').columns('[Measures].[City]').rows('[Customers].[All Customers].Children').execute
        result.row_members.each_with_index do |member, i|
          member.caption.should == member.name.upcase
          city = member.property_value('City')
          result.formatted_values[i].first.should == city
          member.property_formatted_value('City').should == city.upcase
        end
      end

      it "should execute user defined property on member" do
        result = @olap.from('Sales').
          with_member('[Measures].[Upper Name]').as('[Customers].CurrentMember.UpperName').
          columns('[Measures].[Upper Name]').rows('[Customers].Children').execute
        result.row_members.each_with_index do |member, i|
          result.values[i].should == [member.name.upcase]
        end
      end

      it "should execute user defined method on member" do
        result = @olap.from('Sales').
          with_member('[Measures].[Upper Name]').as("[Customers].CurrentMember.toUpperName('dummy')").
          columns('[Measures].[Upper Name]').rows('[Customers].Children').execute
        result.row_members.each_with_index do |member, i|
          result.values[i].should == [member.name.upcase]
        end
      end

      it "should execute user defined property on set" do
        result = @olap.from('Sales').
          with_member('[Measures].[Upper Name]').as("{[Customers].CurrentMember}.firstUpperName").
          columns('[Measures].[Upper Name]').rows('[Customers].Children').execute
        result.row_members.each_with_index do |member, i|
          result.values[i].should == [member.name.upcase]
        end
      end

      it "should execute user defined method on set" do
        result = @olap.from('Sales').
          with_member('[Measures].[Upper Name]').as("{[Customers].CurrentMember}.firstToUpperName('dummy')").
          columns('[Measures].[Upper Name]').rows('[Customers].Children').execute
        result.row_members.each_with_index do |member, i|
          result.values[i].should == [member.name.upcase]
        end
      end

      it "should execute user defined property on hierarchy" do
        result = @olap.from('Sales').
          with_member('[Measures].[Upper Name]').as("[Customers].firstChildUpperName").
          columns('[Measures].[Upper Name]').rows('[Customers].Children').execute
        first_member = result.row_members.first
        result.row_members.each_with_index do |member, i|
          result.values[i].should == [first_member.name.upcase]
        end
      end

      it "should execute user defined property on level" do
        result = @olap.from('Sales').
          with_member('[Measures].[Upper Name]').as("[Customers].[Name].firstLevelChildUpperName").
          columns('[Measures].[Upper Name]').rows('[Customers].Children').execute
        first_member = result.row_members.first
        result.row_members.each_with_index do |member, i|
          result.values[i].should == [first_member.name.upcase]
        end
      end
    end

    describe "Shared user defined functions in Ruby" do
      before(:each) do
        shared_schema = Mondrian::OLAP::Schema.define do
          user_defined_function 'Factorial' do
            ruby :shared do
              parameters :numeric
              returns :numeric
              def call(n)
                n <= 1 ? 1 : n * call(n - 1)
              end
            end
          end
          user_defined_function 'UpperName' do
            ruby :shared do
              parameters :member
              returns :string
              syntax :property
              def call(member)
                member.getName.upcase
              end
            end
          end
          user_defined_function 'toUpperName' do
            ruby :shared do
              parameters :member, :string
              returns :string
              syntax :method
              def call(member, dummy)
                member.getName.upcase
              end
            end
          end
          user_defined_cell_formatter 'Integer20Digits' do
            ruby :shared do |value|
              "%020d" % value
            end
          end
        end

        @schema.define do
          include_schema shared_schema

          cube 'Sales' do
            table 'sales'
            dimension 'Customers', :foreign_key => 'customer_id' do
              hierarchy :has_all => true, :all_member_name => 'All Customers', :primary_key => 'id' do
                table 'customers'
                level 'Name', :column => 'fullname'
              end
            end
            measure 'Unit Sales', :column => 'unit_sales', :aggregator => 'sum', :format_string => '#,##0'
            calculated_member 'Factorial' do
              dimension 'Measures'
              formula 'Factorial(6)'
              cell_formatter 'Integer20Digits'
            end
          end
        end
        @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema)
      end

      it "should render XML" do
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Cube name="Sales">
            <Table name="sales"/>
            <Dimension foreignKey="customer_id" name="Customers">
              <Hierarchy allMemberName="All Customers" hasAll="true" primaryKey="id">
                <Table name="customers"/>
                <Level column="fullname" name="Name" uniqueMembers="true"/>
              </Hierarchy>
            </Dimension>
            <Measure aggregator="sum" column="unit_sales" formatString="#,##0" name="Unit Sales"/>
            <CalculatedMember dimension="Measures" name="Factorial">
              <Formula>Factorial(6)</Formula>
              <CellFormatter className="rubyobj.Mondrian.OLAP.Schema.CellFormatter.Integer20DigitsUdf"/>
            </CalculatedMember>
          </Cube>
          <UserDefinedFunction className="rubyobj.Mondrian.OLAP.Schema.UserDefinedFunction.FactorialUdf" name="Factorial"/>
          <UserDefinedFunction className="rubyobj.Mondrian.OLAP.Schema.UserDefinedFunction.UppernameUdf" name="UpperName"/>
          <UserDefinedFunction className="rubyobj.Mondrian.OLAP.Schema.UserDefinedFunction.TouppernameUdf" name="toUpperName"/>
        </Schema>
        XML
      end

      it "should execute user defined function" do
        result = @olap.from('Sales').columns('[Measures].[Factorial]').execute
        value = 1*2*3*4*5*6
        result.values.should == [value]
        result.formatted_values.should == ["%020d" % value]
      end

      it "should get measure cell formatter name" do
        @olap.cube('Sales').member('[Measures].[Factorial]').cell_formatter_name.should == 'Integer20Digits'
      end

      it "should not get measure cell formatter name if not specified" do
        @olap.cube('Sales').member('[Measures].[Unit Sales]').cell_formatter_name.should be_nil
      end

      it "should get measure format string" do
        @olap.cube('Sales').member('[Measures].[Unit Sales]').format_string.should == '#,##0'
      end

      it "should not get measure format string if not specified" do
        @olap.cube('Sales').member('[Measures].[Factorial]').format_string.should be_nil
      end

    end

    describe "Roles" do
      it "should render XML" do
        @schema.define do
          role 'California manager' do
            schema_grant :access => 'none' do
              cube_grant :cube => 'Sales', :access => 'all' do
                dimension_grant :dimension => '[Measures]', :access => 'all'
                hierarchy_grant :hierarchy => '[Customers]', :access => 'custom',
                                :top_level => '[Customers].[State Province]', :bottom_level => '[Customers].[City]' do
                  member_grant :member => '[Customers].[USA].[CA]', :access => 'all'
                  member_grant :member => '[Customers].[USA].[CA].[Los Angeles]', :access => 'none'
                end
              end
            end
          end
        end
        @schema.to_xml.should be_like <<-XML
        <?xml version="1.0" encoding="UTF-8"?>
        <Schema name="default">
          <Role name="California manager">
            <SchemaGrant access="none">
              <CubeGrant access="all" cube="Sales">
                <DimensionGrant access="all" dimension="[Measures]"/>
                <HierarchyGrant access="custom" bottomLevel="[Customers].[City]" hierarchy="[Customers]" topLevel="[Customers].[State Province]">
                  <MemberGrant access="all" member="[Customers].[USA].[CA]"/>
                  <MemberGrant access="none" member="[Customers].[USA].[CA].[Los Angeles]"/>
                </HierarchyGrant>
              </CubeGrant>
            </SchemaGrant>
          </Role>
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

  describe "errors" do
    before(:each) do
      @schema = Mondrian::OLAP::Schema.new
    end

    it "should raise error when invalid schema" do
      @schema.define do
        cube 'Sales' do
        end
      end
      expect {
        Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema)
      }.to raise_error {|e|
        e.should be_kind_of(Mondrian::OLAP::Error)
        e.message.should == "mondrian.olap.MondrianException: Mondrian Error:Internal error: Must specify fact table of cube 'Sales'"
        e.root_cause_message.should == "Internal error: Must specify fact table of cube 'Sales'"
      }
    end

    it "should raise error when invalid calculated member formula" do
      @schema.define do
        cube 'Sales' do
          table 'sales'
          calculated_member 'Dummy' do
            dimension 'Measures'
            formula 'Dummy(123)'
          end
        end
      end
      expect {
        Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema)
      }.to raise_error {|e|
        e.should be_kind_of(Mondrian::OLAP::Error)
        e.message.should == "mondrian.olap.MondrianException: Mondrian Error:Named set in cube 'Sales' has bad formula"
        e.root_cause_message.should == "No function matches signature 'Dummy(<Numeric Expression>)'"
      }
    end

  end

end
