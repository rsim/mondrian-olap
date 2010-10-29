require "spec_helper"

describe "Cube" do
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
        dimension 'Customers', :foreign_key => 'customer_id' do
          hierarchy :has_all => true, :all_member_name => 'All Customers', :primary_key => 'customer_id' do
            table 'customer'
            level 'Country', :column => 'country', :unique_members => true
            level 'State Province', :column => 'state_province', :unique_members => true
            level 'City', :column => 'city', :unique_members => false
            level 'Name', :column => 'fullname', :unique_members => true
          end
        end
        dimension 'Time', :foreign_key => 'time_id', :type => 'TimeDimension' do
          hierarchy :has_all => false, :primary_key => 'time_id' do
            table 'time_by_day'
            level 'Year', :column => 'the_year', :type => 'Numeric', :unique_members => true, :level_type => 'TimeYears'
            level 'Quarter', :column => 'quarter', :unique_members => false, :level_type => 'TimeQuarters'
            level 'Month', :column => 'month_of_year', :type => 'Numeric', :unique_members => false, :level_type => 'TimeMonths'
          end
          hierarchy 'Weekly', :has_all => false, :primary_key => 'time_id' do
            table 'time_by_day'
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

    it "should get hierarchy names" do
      @cube.dimension('Time').hierarchy_names.should == ['Time', 'Time.Weekly']
    end

    it "should get hierarchy by name" do
      @cube.dimension('Time').hierarchy('Time.Weekly').name.should == 'Time.Weekly'
    end

    it "should get default hierarchy" do
      @cube.dimension('Time').hierarchy.name.should == 'Time'
    end

    it "should get hierarchy level names" do
      @cube.dimension('Time').hierarchy.level_names.should == ['Year', 'Quarter', 'Month']
      @cube.dimension('Customers').hierarchy.level_names.should ==  ['(All)', 'Country', 'State Province', 'City', 'Name']
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
      @cube.dimension('Gender').hierarchy.root_member_names.should == ['All Genders']
      @cube.dimension('Time').hierarchy.root_member_names.should == ['1997', '1998']
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

end
