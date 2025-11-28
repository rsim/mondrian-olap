require "spec_helper"

describe "Connection role" do

  describe "create connection" do
    before(:all) do
      @all_roles = [
        @role_name = role_name = 'California manager',
        @role_name2 = role_name2 = 'Dummy, with comma',
        @simple_role_name = simple_role_name = 'USA manager',
        @mexico_santa_anita_role_name = mexico_santa_anita_role_name = 'Mexico Santa Anita',
        @union_role_name = union_role_name = 'Union California manager',
        @intermediate_union_role_name = intermediate_union_role_name = "Intermediate #{union_role_name}"
      ]

      @schema = Mondrian::OLAP::Schema.define do
        cube 'Sales' do
          table 'sales'
          dimension 'Gender', :foreign_key => 'customer_id' do
            hierarchy :has_all => true, :primary_key => 'id' do
              table 'customers'
              level 'Gender', :column => 'gender', :unique_members => true, :hide_member_if => 'IfBlankName'
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
          calculated_member 'Mexico DF', dimension: 'Customers', formula: <<~MDX
            Aggregate([Customers].[Mexico].[DF].Children)
          MDX
          calculated_member 'Mexico Santa Anita and Santa Fe', dimension: 'Customers', formula: <<~MDX
            Aggregate({[Customers].[Mexico].[DF].[Santa Anita], [Customers].[Mexico].[DF].[Santa Fe]})
          MDX
        end
        role mexico_santa_anita_role_name do
          schema_grant :access => 'none' do
            cube_grant :cube => 'Sales', :access => 'all' do
              dimension_grant :dimension => '[Measures]', :access => 'all'
              hierarchy_grant :hierarchy => '[Customers]', :bottom_level => '[Customers].[City]', :access => 'custom' do
                member_grant :member => '[Customers].[Mexico].[DF].[Santa Anita]', :access => 'all'
                member_grant :member => '[Customers].[Mexico DF]', :access => 'all'
                member_grant :member => '[Customers].[Mexico Santa Anita and Santa Fe]', :access => 'all'
              end
            end
          end
        end
        role role_name do
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
        role role_name2

        role simple_role_name do
          schema_grant :access => 'none' do
            cube_grant :cube => 'Sales', :access => 'all' do
              hierarchy_grant :hierarchy => '[Customers]', :access => 'custom', :bottom_level => '[Customers].[State Province]' do
                member_grant :member => '[Customers].[USA]', :access => 'all'
              end
            end
          end
        end
        role intermediate_union_role_name do
          union do
            role_usage role_name: simple_role_name
          end
        end
        role union_role_name do
          union do
            role_usage role_name: intermediate_union_role_name
          end
        end

        # to test that Role elements are generated before UserDefinedFunction
        user_defined_function 'Factorial' do
          ruby do
            parameters :numeric
            returns :numeric
            def call(n)
              n <= 1 ? 1 : n * call(n - 1)
            end
          end
        end
      end
    end

    before(:each) do
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema)
    end

    after(:each) do
      @olap.role_name = nil if @olap
    end

    it "should connect" do
      @olap.should be_connected
    end

    it "should get available role names" do
      @olap.available_role_names.sort.should == @all_roles.sort
    end

    it "should not get role name if not set" do
      @olap.role_name.should be_nil
      @olap.role_names.should be_empty
    end

    it "should set and get role name" do
      @olap.role_name = @role_name
      @olap.role_name.should == @role_name
      @olap.role_names.should == [@role_name]
    end

    it "should raise error when invalid role name is set" do
      expect {
        @olap.role_name = 'invalid'
      }.to raise_error {|e|
        e.should be_kind_of(Mondrian::OLAP::Error)
        e.message.should == "org.olap4j.OlapException: Unknown role 'invalid'"
        e.root_cause_message.should == "Unknown role 'invalid'"
      }
    end

    it "should set and get several role names" do
      @olap.role_names = [@role_name, @role_name2]
      @olap.role_name.should == "[#{@role_name}, #{@role_name2}]"
      @olap.role_names.should == [@role_name, @role_name2]
    end

    it "should not get non-visible member" do
      @cube = @olap.cube('Sales')
      @cube.member('[Customers].[USA].[CA].[Los Angeles]').should_not be_nil
      @olap.role_name = @role_name
      @cube.member('[Customers].[USA].[CA].[Los Angeles]').should be_nil
    end

    # TODO: investigate why role name is not returned when set in connection string
    # it "should set role name from connection parameters" do
    #   @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema,
    #     :role => @role_name)
    #   @olap.role_name.should == @role_name
    # end

    it "should not get non-visible member when role name set in connection parameters" do
      olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge schema: @schema, role: @role_name)
      cube = olap.cube('Sales')
      cube.member('[Customers].[USA].[CA].[Los Angeles]').should be_nil
    end

    it "should not get non-visible member when several role names set in connection parameters" do
      olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge schema: @schema, roles: [@role_name, @role_name2])
      cube = olap.cube('Sales')
      cube.member('[Customers].[USA].[CA].[Los Angeles]').should be_nil
    end

    it "should see members from ragged dimensions when using single role" do
      # Workaround for a Mondrian bug which does not allow access to ragged dimensions when using single role.
      # This syntax will create a union role with one role.
      @olap.role_names = [@role_name]
      cube = @olap.cube('Sales')
      cube.member('[Customers].[USA].[CA].[Los Angeles]').should be_nil
      cube.member('[Gender].[All Genders]').should_not be_nil
    end

    it "should see members from ragged dimensions when using multiple roles" do
      @olap.role_names = [@role_name, @role_name2]
      cube = @olap.cube('Sales')
      cube.member('[Customers].[USA].[CA].[Los Angeles]').should be_nil
      cube.member('[Gender].[All Genders]').should_not be_nil
    end

    # Test patch for UnionRoleImpl getBottomLevelDepth method
    it "should see member as drillable when using union of union role" do
      @olap.role_names = [@union_role_name]
      cube = @olap.cube('Sales')
      cube.member('[Customers].[All Customers]').should be_drillable
      cube.member('[Customers].[All Customers].[USA]').should be_drillable
      cube.member('[Customers].[All Customers].[USA].[CA]').should_not be_drillable
    end

    describe "calculated member with role restrictions" do
      before(:each) do
        @olap.role_name = @mexico_santa_anita_role_name
        @query = @olap.from('Sales')
        @sql = ActiveRecord::Base.connection
      end

      it "should aggregate only accessible children" do
        result = @query.rows('[Customers].[Mexico DF]').columns('[Measures].[Unit Sales]').execute
        result.values.should == [[@sql.select_value(<<~SQL).to_i]]
          SELECT SUM(unit_sales)
          FROM sales
          JOIN customers ON sales.customer_id = customers.id
          WHERE customers.country = 'Mexico'
            AND customers.state_province = 'DF'
            AND customers.city = 'Santa Anita'
        SQL
      end

      it "should aggregate only accessible members from set" do
        result = @query.rows('[Customers].[Mexico Santa Anita and Santa Fe]').columns('[Measures].[Unit Sales]').execute
        result.values.should == [[@sql.select_value(<<~SQL).to_i]]
          SELECT SUM(unit_sales)
          FROM sales
          JOIN customers ON sales.customer_id = customers.id
          WHERE customers.country = 'Mexico'
            AND customers.state_province = 'DF'
            AND customers.city = 'Santa Anita'
        SQL
      end
    end
  end
end
