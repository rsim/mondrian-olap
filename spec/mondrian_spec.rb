# encoding: utf-8

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
        dimension 'Promotions', :foreign_key => 'promotion_id' do
          hierarchy :has_all => true, :primary_key => 'id' do
            table 'promotions'
            level 'Promotion', :column => 'id', :name_column => 'promotion', :unique_members => true, :ordinal_column => 'sequence', :type => 'Numeric'
          end
        end
        dimension 'Linked Promotions', :foreign_key => 'customer_id' do
          hierarchy :has_all => true, :primary_key => 'id', :primary_key_table => 'customers' do
            join :left_key => 'related_fullname', :right_key => 'fullname' do
              table "customers"
              join :left_key => "promotion_id", :right_key => "id" do
                table "customers", :alias => "customers_bt"
                table "promotions"
              end
            end
            level 'Promotion', :column => 'id', :name_column => 'promotion', :unique_members => true, :table => 'promotions', :ordinal_column => 'sequence', :type => 'Numeric', :approx_row_count => 10
          end
        end
        dimension 'Customers', :foreign_key => 'customer_id' do
          hierarchy :has_all => true, :all_member_name => 'All Customers', :primary_key => 'id' do
            table 'customers'
            level 'Country', :column => 'country', :unique_members => true
            level 'State Province', :column => 'state_province', :unique_members => true
            level 'City', :column => 'city', :unique_members => false
            level 'Name', :column => 'fullname', :unique_members => true do
              property 'Related name', :column => 'related_fullname', :type => "String"
              property 'Birthdate', :column => 'birthdate', :type => "String"
            end
          end
          hierarchy 'ID', :has_all => true, :all_member_name => 'All Customers', :primary_key => 'id' do
            table 'customers'
            level 'ID', :column => 'id', :type => 'Numeric', :internal_type => 'long', :unique_members => true do
              property 'Name', :column => 'fullname'
            end
          end
        end
        dimension 'Time', :foreign_key => 'time_id', :type => 'TimeDimension' do
          hierarchy :has_all => false, :primary_key => 'id' do
            table 'time'
            level 'Year', :column => 'the_year', :type => 'Numeric', :unique_members => true, :level_type => 'TimeYears'
            level 'Quarter', :column => 'quarter', :unique_members => false, :level_type => 'TimeQuarters'
            level 'Month', :column => 'month_of_year', :type => 'Numeric', :unique_members => false, :level_type => 'TimeMonths'
            level 'Day', :column => 'day_of_month', :type => 'Numeric', :unique_members => false, :level_type => 'TimeDays' do
              property 'Date', :column => 'the_date', :type => "String"
            end
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

      user_defined_function 'IsDirty' do
        ruby do
          returns :scalar
          syntax :function
          def call_with_evaluator(evaluator)
            evaluator.isDirty
          end
        end
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

  # test for https://jira.pentaho.com/browse/MONDRIAN-2683
  it "should order crossjoin of rows" do
    lambda do
      @olap.from('Sales').
      columns('[Measures].[Unit Sales]').
      rows('[Customers].[Country].Members').crossjoin('[Gender].[Gender].Members').
        order('[Measures].[Unit Sales]', :bdesc).
      execute
    end.should_not raise_error
  end

  it "should generate correct member name from large number key" do
    result = @olap.from('Sales').
      columns("Filter([Customers.ID].[ID].Members, [Customers.ID].CurrentMember.Properties('Name') = 'Big Number')").
      execute
    result.column_names.should == ["10000000000"]
  end

  # test for https://jira.pentaho.com/browse/MONDRIAN-990
  it "should return result when diacritical marks used" do
    full_name = '[Customers].[USA].[CA].[Rīga]'
    result = @olap.from('Sales').columns(full_name).execute
    result.column_full_names.should == [full_name]
  end

  it "should execute MDX with join tables" do
    # Load dimension members in Mondrian cache as the problem occurred when searching members in the cache
    @olap.from('Sales').columns('CROSSJOIN({[Linked Promotions].[Promotion].[Promotion 2]}, [Customers].[Name].Members)').execute

    mdx = <<~MDX
      SELECT
        NON EMPTY FILTER(
          CROSSJOIN({[Linked Promotions].[Promotion].[Promotion 2]}, [Customers].[Name].Members),
          (([Measures].[Unit Sales]) <> 0)
        ) ON ROWS,
        [Measures].[Unit Sales] ON COLUMNS
      FROM [Sales]
    MDX

    expect { @olap.execute mdx }.not_to raise_error
  end

  # Test for https://jira.pentaho.com/browse/MONDRIAN-2714
  it "should return datetime property as java.sql.Timestamp" do
    full_name = '[2010].[Q1].[1].[1]'
    member = @olap.cube('Sales').member(full_name)
    member.property_value('Date').should be_a(java.sql.Timestamp)

    result = @olap.from('Sales').
      with_member('[Measures].[date]').as("#{full_name}.Properties('Date')", format_string: 'dd.mm.yyyy').
      columns('[Measures].[date]').execute
    result.values.first.should be_a(java.sql.Timestamp)
    result.formatted_values.first.should == '01.01.2010'
  end

  it "should return date property as java.sql.Date" do
    expected_date_class =
      case MONDRIAN_DRIVER
      when 'oracle'
        java.sql.Timestamp
      else
        java.sql.Date
      end

    member = @olap.cube('Sales').hierarchy('Customers').level('Name').members.first
    date_value = member.property_value('Birthdate')
    date_value.should be_a(expected_date_class)

    result = @olap.from('Sales').
      with_member('[Measures].[date]').as("#{member.full_name}.Properties('Birthdate')", format_string: 'dd.mm.yyyy').
      columns('[Measures].[date]').execute
    result.values.first.should be_a(expected_date_class)
    result.formatted_values.first.should == Date.parse(date_value.to_s).strftime("%d.%m.%Y")
  end

  describe "optimized Aggregate" do
    def expected_value(crossjoin_members = nil)
      query = @olap.from('Sales').columns('[Measures].[Unit Sales]')
      query = query.crossjoin(crossjoin_members) if crossjoin_members
      query.rows('[Customers].[USA].[CA]', '[Customers].[USA].[OR]').
        execute.values.map(&:first).inject(&:+)
    end

    it "should aggregate stored members" do
      result = @olap.from('Sales').
        with_member('[Customers].[CA and OR]').as("Aggregate({[Customers].[USA].[CA], [Customers].[USA].[OR]})").
        columns('[Measures].[Unit Sales]').
        rows('[Customers].[CA and OR]').execute
      result.values[0][0].should == expected_value
    end

    it "should aggregate stored members from several dimensions" do
      result = @olap.from('Sales').
        with_member('[Customers].[CA and OR]').
          as("Aggregate({[Gender].[F]} * {[Customers].[USA].[CA], [Customers].[USA].[OR]})").
        columns('[Measures].[Unit Sales]').
        rows('[Customers].[CA and OR]').execute
      result.values[0][0].should == expected_value('[Gender].[F]')
    end

    it "should aggregate stored members and a measure" do
      result = @olap.from('Sales').
        with_member('[Measures].[CA and OR]').
          as("Aggregate({[Customers].[USA].[CA], [Customers].[USA].[OR]} * {[Measures].[Unit Sales]})").
        columns('[Measures].[CA and OR]').execute
      result.values[0].should == expected_value
    end

    it "should aggregate stored members with expression" do
      result = @olap.from('Sales').
        with_member('[Measures].[CA and OR twice]').
          as("Aggregate({[Customers].[USA].[CA], [Customers].[USA].[OR]}, [Measures].[Unit Sales] * 2)").
        columns('[Measures].[CA and OR twice]').execute
      result.values[0].should == expected_value * 2
    end

    it "should aggregate calculated aggregate members" do
      result = @olap.from('Sales').
        with_member('[Customers].[CA calculated]').as("Aggregate({[Customers].[USA].[CA]})").
        with_member('[Customers].[OR calculated]').as("Aggregate({[Customers].[USA].[OR]})").
        with_member('[Customers].[CA and OR]').as("Aggregate({[Customers].[CA calculated], [Customers].[OR calculated]})").
        columns('[Measures].[Unit Sales]').
        rows('[Customers].[CA and OR]').execute
      result.values[0][0].should == expected_value
    end
  end

  it "should call evaluator isDirty method" do
    result = @olap.from('Sales').
      with_member('[Measures].[is dirty]').as('IsDirty()').
      columns('[Measures].[is dirty]').execute
    result.values[0].should be_false
  end

  it "should support multiple values IN expression" do
    lambda do
      @olap.from('Sales').
      columns('[Measures].[Unit Sales]').
      where('[Time].[2011].[Q1]', '[Time].[2011].[Q2]').
      execute
    end.should_not raise_error
  end

  describe "functions with double argument" do
    it "should get Abs with decimal measure" do
      result = @olap.from('Sales').
        with_member('[Measures].[Abs Store Sales]').as('Abs([Measures].[Store Sales])').
        columns('[Measures].[Store Sales]', '[Measures].[Abs Store Sales]').execute
      result.values[0].should == result.values[1]
    end

    it "should get Round with decimal measure" do
      result = @olap.from('Sales').
        with_member('[Measures].[Round Store Sales]').as('Round([Measures].[Store Sales])').
        columns('[Measures].[Store Sales]', '[Measures].[Round Store Sales]').
        where('[Customers].[USA].[CA]').execute
      result.values[0].round.should == result.values[1]
    end
  end
end
