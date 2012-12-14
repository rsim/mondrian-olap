require "spec_helper"

describe "Cube" do
  before(:all) do
    @schema = Mondrian::OLAP::Schema.define do
      cube 'Sales' do
        description 'Sales description'
        table 'sales'
        dimension 'Gender', :foreign_key => 'customer_id' do
          description 'Gender description'
          hierarchy :has_all => true, :primary_key => 'id' do
            description 'Gender hierarchy description'
            table 'customers'
            level 'Gender', :column => 'gender', :unique_members => true, :description => 'Gender level description'
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
        calculated_member 'Non-USA' do
          dimension 'Customers'
          formula '[Customers].[All Customers] - [Customers].[USA]'
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
        measure 'Store Cost', :column => 'store_cost', :aggregator => 'sum', :visible => false
      end
    end
    @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge :schema => @schema)
  end

  it "should get all cube names" do
    @olap.cube_names.should == ['Sales']
  end

  it "should get cube by name" do
    @olap.cube('Sales').should be_a(Mondrian::OLAP::Cube)
  end

  it "should return nil when getting cube with invalid name" do
    @olap.cube('invalid').should be_nil
  end

  it "should get cube name" do
    @olap.cube('Sales').name.should == 'Sales'
  end

  it "should get cube description" do
    @olap.cube('Sales').description.should == 'Sales description'
  end

  describe "dimensions" do
    before(:all) do
      @cube = @olap.cube('Sales')
      @dimension_names = ['Measures', 'Gender', 'Customers', 'Time']
    end

    it "should get dimension names" do
      @cube.dimension_names.should == @dimension_names
    end

    it "should get dimensions" do
      @cube.dimensions.map{|d| d.name}.should == @dimension_names
    end

    it "should get dimension by name" do
      @cube.dimension('Gender').name.should == 'Gender'
    end

    it "should return nil when getting dimension with invalid name" do
      @cube.dimension('invalid').should be_nil
    end

    it "should get dimension description" do
      @cube.dimension('Gender').description.should == 'Gender description'
    end

    it "should get dimension full name" do
      @cube.dimension('Gender').full_name.should == '[Gender]'
    end

    it "should get measures dimension" do
      @cube.dimension('Measures').should be_measures
    end

    it "should get dimension type" do
      @cube.dimension('Gender').dimension_type.should == :standard
      @cube.dimension('Time').dimension_type.should == :time
      @cube.dimension('Measures').dimension_type.should == :measures
    end
  end

  describe "dimension hierarchies" do
    before(:all) do
      @cube = @olap.cube('Sales')
    end

    it "should get hierarchies" do
      hierarchies = @cube.dimension('Gender').hierarchies
      hierarchies.size.should == 1
      hierarchies[0].name.should == 'Gender'
    end

    it "should get hierarchy description" do
      hierarchies = @cube.dimension('Gender').hierarchies.first.description.should == 'Gender hierarchy description'
    end

    it "should get hierarchy names" do
      @cube.dimension('Time').hierarchy_names.should == ['Time', 'Time.Weekly']
    end

    it "should get hierarchy by name" do
      @cube.dimension('Time').hierarchy('Time.Weekly').name.should == 'Time.Weekly'
    end

    it "should return nil when getting hierarchy with invalid name" do
      @cube.dimension('Time').hierarchy('invalid').should be_nil
    end

    it "should get default hierarchy" do
      @cube.dimension('Time').hierarchy.name.should == 'Time'
    end

    it "should get hierarchy levels" do
      @cube.dimension('Customers').hierarchy.levels.map(&:name).should ==  ['(All)', 'Country', 'State Province', 'City', 'Name']
    end

    it "should get hierarchy level names" do
      @cube.dimension('Time').hierarchy.level_names.should == ['Year', 'Quarter', 'Month']
      @cube.dimension('Customers').hierarchy.level_names.should ==  ['(All)', 'Country', 'State Province', 'City', 'Name']
    end

    it "should get hierarchy level depths" do
      @cube.dimension('Customers').hierarchy.levels.map(&:depth).should ==  [0, 1, 2, 3, 4]
    end

    it "should get hierarchy level members count" do
      @cube.dimension('Gender').hierarchy.levels.map(&:members_count).should == [1, 2]
    end
  end

  describe "hierarchy values" do
    before(:all) do
      @cube = @olap.cube('Sales')
    end

    it "should get hierarchy all member" do
      @cube.dimension('Gender').hierarchy.has_all?.should be_true
      @cube.dimension('Gender').hierarchy.all_member_name.should == 'All Genders'
    end

    it "should not get all member for hierarchy without all member" do
      @cube.dimension('Time').hierarchy.has_all?.should be_false
      @cube.dimension('Time').hierarchy.all_member_name.should be_nil
    end

    it "should get hierarchy root members" do
      @cube.dimension('Gender').hierarchy.root_members.map(&:name).should == ['All Genders']
      @cube.dimension('Gender').hierarchy.root_member_names.should == ['All Genders']
      @cube.dimension('Time').hierarchy.root_members.map(&:name).should == ['2010', '2011']
      @cube.dimension('Time').hierarchy.root_member_names.should == ['2010', '2011']
    end

    it "should return child members for specified member" do
      @cube.dimension('Gender').hierarchy.child_names('All Genders').should == ['F', 'M']
      @cube.dimension('Customers').hierarchy.child_names('USA', 'OR').should ==
        ["Albany", "Beaverton", "Corvallis", "Lake Oswego", "Lebanon", "Milwaukie",
        "Oregon City", "Portland", "Salem", "W. Linn", "Woodburn"]
    end

    it "should return child members for hierarchy" do
      @cube.dimension('Gender').hierarchy.child_names.should == ['F', 'M']
    end

    it "should not return child members for leaf member" do
      @cube.dimension('Gender').hierarchy.child_names('All Genders', 'F').should == []
    end

    it "should return nil as child members if parent member not found" do
      @cube.dimension('Gender').hierarchy.child_names('N').should be_nil
    end

  end

  describe "level members" do
    before(:all) do
      @cube = @olap.cube('Sales')
    end

    it "should get level description" do
      @cube.dimension('Gender').hierarchy.level('Gender').description.should == 'Gender level description'
    end

    it "should return nil when getting level with invalid name" do
      @cube.dimension('Gender').hierarchy.level('invalid').should be_nil
    end

    it "should get primary hierarchy level members" do
      @cube.dimension('Customers').hierarchy.level('Country').members.
        map(&:name).should == ['Canada', 'Mexico', 'USA']
    end

    it "should get secondary hierarchy level members" do
      @cube.dimension('Time').hierarchy('Time.Weekly').level('Year').members.
        map(&:name).should == ['2010', '2011']
    end
  end

  describe "members" do
    before(:all) do
      @cube = @olap.cube('Sales')
    end

    it "should return member for specified full name" do
      @cube.member('[Gender].[All Genders]').name.should == 'All Genders'
      @cube.member('[Customers].[USA].[OR]').name.should == 'OR'
    end

    it "should not return member for invalid full name" do
      @cube.member('[Gender].[invalid]').should be_nil
    end

    it "should return child members for member" do
      @cube.member('[Gender].[All Genders]').children.map(&:name).should == ['F', 'M']
      @cube.member('[Customers].[USA].[OR]').children.map(&:name).should ==
        ["Albany", "Beaverton", "Corvallis", "Lake Oswego", "Lebanon", "Milwaukie",
        "Oregon City", "Portland", "Salem", "W. Linn", "Woodburn"]
    end

    it "should return empty children array if member does not have children" do
      @cube.member('[Gender].[All Genders].[F]').children.should be_empty
    end

    it "should return member depth" do
      @cube.member('[Customers].[All Customers]').depth.should == 0
      @cube.member('[Customers].[USA]').depth.should == 1
      @cube.member('[Customers].[USA].[CA]').depth.should == 2
    end

    it "should return descendants for member at specified level" do
      @cube.member('[Customers].[Mexico]').descendants_at_level('City').map(&:name).should ==
        ["San Andres", "Santa Anita", "Santa Fe", "Tixapan", "Acapulco", "Guadalajara",
        "Mexico City", "Tlaxiaco", "La Cruz", "Orizaba", "Merida", "Camacho", "Hidalgo"]
    end

    it "should not return descendants for member when upper level specified" do
      @cube.member('[Customers].[Mexico].[DF]').descendants_at_level('Country').should be_nil
    end

    it "should be drillable when member has descendants" do
      @cube.member('[Customers].[USA]').should be_drillable
    end

    it "should not be drillable when member has no descendants" do
      @cube.member('[Gender].[F]').should_not be_drillable
    end

    it "should not be drillable when member is calculated" do
      @cube.member('[Customers].[Non-USA]').should_not be_drillable
    end

    it "should be calculated when member is calculated" do
      @cube.member('[Customers].[Non-USA]').should be_calculated
    end

    it "should not be calculated when normal member" do
      @cube.member('[Customers].[USA]').should_not be_calculated
    end

    it "should be all member when member is all member" do
      @cube.member('[Customers].[All Customers]').should be_all_member
    end

    it "should not be all member when member is not all member" do
      @cube.member('[Customers].[USA]').should_not be_all_member
    end

    it "should get dimension type of standard dimension member" do
      @cube.member('[Customers].[USA]').dimension_type.should == :standard
    end

    it "should get dimension type of measure" do
      @cube.member('[Measures].[Unit Sales]').dimension_type.should == :measures
    end

    it "should get dimension type of time dimension member" do
      @cube.member('[Time].[2011]').dimension_type.should == :time
    end

    it "should be visble when member is visible" do
      @cube.member('[Measures].[Store Sales]').should be_visible
    end

    it "should not be visble when member is not visible" do
      @cube.member('[Measures].[Store Cost]').should_not be_visible
    end
  end

end
