require "spec_helper"

describe "Mondrian features" do
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
        dimension 'Customers', :foreign_key => 'customer_id' do
          hierarchy :has_all => true, :all_member_name => 'All Customers', :primary_key => 'id' do
            table 'customers'
            level 'Country', :column => 'country', :unique_members => true
            level 'State Province', :column => 'state_province', :unique_members => true
            level 'City', :column => 'city', :unique_members => false
            level 'Name', :column => 'fullname', :unique_members => true
          end
        end
        dimension 'Time', :foreign_key => 'time_id', :type => 'TimeDimension' do
          hierarchy :has_all => false, :primary_key => 'id' do
            table 'time'
            level 'Year', :column => 'the_year', :type => 'Numeric', :unique_members => true, :level_type => 'TimeYears'
            level 'Quarter', :column => 'quarter', :unique_members => false, :level_type => 'TimeQuarters'
            level 'Month', :column => 'month_of_year', :type => 'Numeric', :unique_members => false, :level_type => 'TimeMonths'
          end
          hierarchy 'Weekly', :has_all => false, :primary_key => 'id' do
            table 'time'
            level 'Year', :column => 'the_year', :type => 'Numeric', :unique_members => true, :level_type => 'TimeYears'
            level 'Week', :column => 'weak_of_year', :type => 'Numeric', :unique_members => false, :level_type => 'TimeWeeks'
          end
        end
        measure 'Unit Sales', :column => 'unit_sales', :aggregator => 'sum'
        measure 'Store Sales', :column => 'store_sales', :aggregator => 'sum'
      end
    end
    @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema)
  end

  # test for http://jira.pentaho.com/browse/MONDRIAN-1050
  it "should order rows by DateTime expression" do
    lambda do
      @olap.from('Sales').
      columns('[Measures].[Unit Sales]').
      rows('[Customers].children').order('Now()', :asc).
      execute
    end.should_not raise_error
  end

end
