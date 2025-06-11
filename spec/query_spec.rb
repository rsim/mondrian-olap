require "spec_helper"

describe "Query" do
  def qt(name)
    ActiveRecord::Base.connection.quote_table_name(name.to_s)
  end

  before(:all) do
    @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG)
    @sql = ActiveRecord::Base.connection

    @query_string = <<-SQL
    SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
            {[Product].children} ON ROWS
      FROM  [Sales]
      WHERE ([Time].[2010].[Q1], [Customers].[USA].[CA])
    SQL

    @sql_select = <<-SQL
    SELECT SUM(unit_sales) unit_sales_sum, SUM(store_sales) store_sales_sum
    FROM sales
      LEFT JOIN products ON sales.product_id = products.id
      LEFT JOIN product_classes ON products.product_class_id = product_classes.id
      LEFT JOIN #{qt :time} ON sales.time_id = #{qt :time}.id
      LEFT JOIN customers ON sales.customer_id = customers.id
    WHERE #{qt :time}.the_year = 2010 AND #{qt :time}.quarter = 'Q1'
      AND customers.country = 'USA' AND customers.state_province = 'CA'
    GROUP BY product_classes.product_family
    ORDER BY product_classes.product_family
    SQL

  end

  def sql_select_numbers(select_string)
    @sql.select_rows(select_string).map do |rows|
      rows.map{|col| BigDecimal(col.to_s)}
    end
  end

  describe "result" do
    before(:all) do

      # TODO: replace hardcoded expected values with result of SQL query
      @expected_column_names = ["Unit Sales", "Store Sales"]
      @expected_column_full_names = ["[Measures].[Unit Sales]", "[Measures].[Store Sales]"]
      @expected_drillable_columns = [false, false]
      @expected_row_names = ["Drink", "Food", "Non-Consumable"]
      @expected_row_full_names = ["[Product].[Drink]", "[Product].[Food]", "[Product].[Non-Consumable]"]
      @expected_drillable_rows = [true, true, true]

      # AR JDBC driver always returns strings, need to convert to BigDecimal
      @expected_result_values = sql_select_numbers(@sql_select)

      @expected_result_values_by_columns =
        [@expected_result_values.map{|row| row[0]}, @expected_result_values.map{|row| row[1]}]

      @result = @olap.execute @query_string
    end

    it "should return axes" do
      @result.axes_count.should == 2
    end

    it "should return column names" do
      @result.column_names.should == @expected_column_names
      @result.column_full_names.should == @expected_column_full_names
    end

    it "should return row names" do
      @result.row_names.should == @expected_row_names
      @result.row_full_names.should == @expected_row_full_names
    end

    it "should return axis by index names" do
      @result.axis_names[0].should == @expected_column_names
      @result.axis_full_names[0].should == @expected_column_full_names
    end

    it "should return column members" do
      @result.column_members.map(&:name).should == @expected_column_names
      @result.column_members.map(&:full_name).should == @expected_column_full_names
      @result.column_members.map(&:"drillable?").should == @expected_drillable_columns
    end

    it "should return row members" do
      @result.row_members.map(&:name).should == @expected_row_names
      @result.row_members.map(&:full_name).should == @expected_row_full_names
      @result.row_members.map(&:"drillable?").should == @expected_drillable_rows
    end

    it "should return cells" do
      @result.values.should == @expected_result_values
    end

    it "should return cells with specified axes number sequence" do
      @result.values(0, 1).should == @expected_result_values_by_columns
    end

    it "should return cells with specified axes name sequence" do
      @result.values(:columns, :rows).should == @expected_result_values_by_columns
    end

    it "should return formatted cells" do
      @result.formatted_values.map{|r| r.map{|s| BigDecimal(s.gsub(',',''))}}.should == @expected_result_values
    end

  end

  describe "builder" do

    before(:each) do
      @query = @olap.from('Sales')
    end

    describe "from cube" do
      it "should return query" do
        @query.should be_a(Mondrian::OLAP::Query)
        @query.cube_name.should == 'Sales'
      end
    end

    describe "columns" do
      it "should accept list" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').should equal(@query)
        @query.columns.should == ['[Measures].[Unit Sales]', '[Measures].[Store Sales]']
      end

      it "should accept list as array" do
        @query.columns(['[Measures].[Unit Sales]', '[Measures].[Store Sales]'])
        @query.columns.should == ['[Measures].[Unit Sales]', '[Measures].[Store Sales]']
      end

      it "should accept with several method calls" do
        @query.columns('[Measures].[Unit Sales]').columns('[Measures].[Store Sales]')
        @query.columns.should == ['[Measures].[Unit Sales]', '[Measures].[Store Sales]']
      end
    end

    describe "other axis" do
      it "should accept axis with index member list" do
        @query.axis(0, '[Measures].[Unit Sales]', '[Measures].[Store Sales]')
        @query.axis(0).should == ['[Measures].[Unit Sales]', '[Measures].[Store Sales]']
      end

      it "should accept rows list" do
        @query.rows('[Product].children')
        @query.rows.should == ['[Product].children']
      end

      it "should accept pages list" do
        @query.pages('[Product].children')
        @query.pages.should == ['[Product].children']
      end
    end

    describe "crossjoin" do
      it "should do crossjoin of several dimensions" do
        @query.rows('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]')
        @query.rows.should == [:crossjoin, ['[Product].children'], ['[Customers].[Canada]', '[Customers].[USA]']]
      end

      it "should do crossjoin passing array as first argument" do
        @query.rows('[Product].children').crossjoin(['[Customers].[Canada]', '[Customers].[USA]'])
        @query.rows.should == [:crossjoin, ['[Product].children'], ['[Customers].[Canada]', '[Customers].[USA]']]
      end
    end

    describe "nonempty_crossjoin" do
      it "should do nonempty_crossjoin of several dimensions" do
        @query.rows('[Product].children').nonempty_crossjoin('[Customers].[Canada]', '[Customers].[USA]')
        @query.rows.should == [:nonempty_crossjoin, ['[Product].children'], ['[Customers].[Canada]', '[Customers].[USA]']]
      end
    end

    describe "nonempty" do
      it "should limit to set of members with nonempty values" do
        @query.rows('[Product].children').nonempty
        @query.rows.should == [:nonempty, ['[Product].children']]
      end
    end

    describe "distinct" do
      it "should limit to set of distinct tuples" do
        @query.rows('[Product].children').distinct.nonempty.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]')
        @query.rows.should == [:nonempty, [:distinct, ["[Product].children"]]]
      end
    end

    describe "order" do
      it "should order by one measure" do
        @query.rows('[Product].children').order('[Measures].[Unit Sales]', :bdesc)
        @query.rows.should == [:order, ['[Product].children'], '[Measures].[Unit Sales]', 'BDESC']
      end

      it "should order using String order direction" do
        @query.rows('[Product].children').order('[Measures].[Unit Sales]', 'DESC')
        @query.rows.should == [:order, ['[Product].children'], '[Measures].[Unit Sales]', 'DESC']
      end

      it "should order by measure and other member" do
        @query.rows('[Product].children').order(['[Measures].[Unit Sales]', '[Customers].[USA]'], :basc)
        @query.rows.should == [:order, ['[Product].children'], ['[Measures].[Unit Sales]', '[Customers].[USA]'], 'BASC']
      end
    end

    %w(top bottom).each do |extreme|
      describe extreme do
        it "should select #{extreme} count rows by measure" do
          @query.rows('[Product].children').send(:"#{extreme}_count", 5, '[Measures].[Unit Sales]')
          @query.rows.should == [:"#{extreme}_count", ['[Product].children'], 5, '[Measures].[Unit Sales]']
        end

        it "should select #{extreme} count rows without measure" do
          @query.rows('[Product].children').send(:"#{extreme}_count", 5)
          @query.rows.should == [:"#{extreme}_count", ['[Product].children'], 5]
        end

        it "should select #{extreme} percent rows by measure" do
          @query.rows('[Product].children').send(:"#{extreme}_percent", 20, '[Measures].[Unit Sales]')
          @query.rows.should == [:"#{extreme}_percent", ['[Product].children'], 20, '[Measures].[Unit Sales]']
        end

        it "should select #{extreme} sum rows by measure" do
          @query.rows('[Product].children').send(:"#{extreme}_sum", 1000, '[Measures].[Unit Sales]')
          @query.rows.should == [:"#{extreme}_sum", ['[Product].children'], 1000, '[Measures].[Unit Sales]']
        end
      end
    end

    describe "hierarchize" do
      it "should hierarchize simple set" do
        @query.rows('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize
        @query.rows.should == [:hierarchize, ['[Customers].[Country].Members', '[Customers].[City].Members']]
      end

      it "should hierarchize last set of crossjoin" do
        @query.rows('[Product].children').crossjoin('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize
        @query.rows.should == [:crossjoin, ['[Product].children'],
          [:hierarchize, ['[Customers].[Country].Members', '[Customers].[City].Members']]]
      end

      it "should hierarchize last set of nonempty_crossjoin" do
        @query.rows('[Product].children').nonempty_crossjoin('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize
        @query.rows.should == [:nonempty_crossjoin, ['[Product].children'],
          [:hierarchize, ['[Customers].[Country].Members', '[Customers].[City].Members']]]
      end

      it "should hierarchize all crossjoin" do
        @query.rows('[Product].children').crossjoin('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize_all
        @query.rows.should == [:hierarchize, [:crossjoin, ['[Product].children'],
          ['[Customers].[Country].Members', '[Customers].[City].Members']]]
      end

      it "should hierarchize with POST" do
        @query.rows('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize(:post)
        @query.rows.should == [:hierarchize, ['[Customers].[Country].Members', '[Customers].[City].Members'], 'POST']
      end

    end

    describe "except" do
      it "should except one set from other" do
        @query.rows('[Customers].[Country].Members').except('[Customers].[USA]')
        @query.rows.should == [:except, ['[Customers].[Country].Members'], ['[Customers].[USA]']]
      end

      it "should except from last set of crossjoin" do
        @query.rows('[Product].children').crossjoin('[Customers].[Country].Members').except('[Customers].[USA]')
        @query.rows.should == [:crossjoin, ['[Product].children'],
          [:except, ['[Customers].[Country].Members'], ['[Customers].[USA]']]]
      end

      it "should except from last set of nonempty_crossjoin" do
        @query.rows('[Product].children').nonempty_crossjoin('[Customers].[Country].Members').except('[Customers].[USA]')
        @query.rows.should == [:nonempty_crossjoin, ['[Product].children'],
          [:except, ['[Customers].[Country].Members'], ['[Customers].[USA]']]]
      end
    end

    describe "filter" do
      it "should filter set by condition" do
        @query.rows('[Customers].[Country].Members').filter('[Measures].[Unit Sales] > 1000')
        @query.rows.should == [:filter, ['[Customers].[Country].Members'], '[Measures].[Unit Sales] > 1000']
      end

      it "should filter using set alias" do
        @query.rows('[Customers].[Country].Members').filter('NOT ISEMPTY(S.CURRENT)', as: 'S')
        @query.rows.should == [:filter, ['[Customers].[Country].Members'], 'NOT ISEMPTY(S.CURRENT)', 'S']
      end

      it "should filter last set of nonempty_crossjoin" do
        @query.rows('[Product].children').nonempty_crossjoin('[Customers].[Country].Members').
          filter_last("[Customers].CurrentMember.Name = 'USA'")
        @query.rows.should == [:nonempty_crossjoin, ['[Product].children'],
          [:filter, ['[Customers].[Country].Members'], "[Customers].CurrentMember.Name = 'USA'"]]
      end
    end

    describe "generate" do
      it "should generate new set" do
        @query.rows('[Customers].[Country].Members').generate('[Customers].CurrentMember')
        @query.rows.should == [:generate, ['[Customers].[Country].Members'], ['[Customers].CurrentMember']]
      end

      it "should generate new set with all option" do
        @query.rows('[Customers].[Country].Members').generate('[Customers].CurrentMember', :all)
        @query.rows.should == [:generate, ['[Customers].[Country].Members'], ['[Customers].CurrentMember'], 'ALL']
      end
    end

    describe "where" do
      it "should accept conditions" do
        @query.where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').should equal(@query)
        @query.where.should == ['[Time].[2010].[Q1]', '[Customers].[USA].[CA]']
      end

      it "should accept conditions as array" do
        @query.where(['[Time].[2010].[Q1]', '[Customers].[USA].[CA]'])
        @query.where.should == ['[Time].[2010].[Q1]', '[Customers].[USA].[CA]']
      end

      it "should accept conditions with several method calls" do
        @query.where('[Time].[2010].[Q1]').where('[Customers].[USA].[CA]')
        @query.where.should == ['[Time].[2010].[Q1]', '[Customers].[USA].[CA]']
      end

      it "should do crossjoin of where conditions" do
        @query.where('[Customers].[USA]').crossjoin('[Time].[2011].[Q1]', '[Time].[2011].[Q2]')
        @query.where.should == [:crossjoin, ['[Customers].[USA]'], ['[Time].[2011].[Q1]', '[Time].[2011].[Q2]']]
      end

      it "should do nonempty_crossjoin of where conditions" do
        @query.where('[Customers].[USA]').nonempty_crossjoin('[Time].[2011].[Q1]', '[Time].[2011].[Q2]')
        @query.where.should == [:nonempty_crossjoin, ['[Customers].[USA]'], ['[Time].[2011].[Q1]', '[Time].[2011].[Q2]']]
      end
    end

    describe "with member" do
      it "should accept definition" do
        @query.with_member('[Measures].[ProfitPct]').
          as('Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])').
          should equal(@query)
        @query.with.should == [
          [ :member, '[Measures].[ProfitPct]',
            'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])'
          ]
        ]
      end

      it "should accept definition with additional parameters" do
        @query.with_member('[Measures].[ProfitPct]').
          as('Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',
            :solve_order => 1,
            :format_string => 'Percent')
        @query.with.should == [
          [ :member, '[Measures].[ProfitPct]',
            'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',
            {:solve_order => 1, :format_string => 'Percent'}
          ]
        ]
      end
    end

    describe "with set" do
      it "should accept simple defition" do
        @query.with_set('SelectedRows').as('[Product].children')
        @query.with.should == [
          [ :set, 'SelectedRows',
            ['[Product].children']
          ]
        ]
      end

      it "should accept definition with crossjoin" do
        @query.with_set('SelectedRows').as('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]')
        @query.with.should == [
          [ :set, 'SelectedRows',
            [:crossjoin, ['[Product].children'], ['[Customers].[Canada]', '[Customers].[USA]']]
          ]
        ]
      end

      it "should accept definition with nonempty_crossjoin" do
        @query.with_set('SelectedRows').as('[Product].children').nonempty_crossjoin('[Customers].[Canada]', '[Customers].[USA]')
        @query.with.should == [
          [ :set, 'SelectedRows',
            [:nonempty_crossjoin, ['[Product].children'], ['[Customers].[Canada]', '[Customers].[USA]']]
          ]
        ]
      end
    end

    describe "to MDX" do
      it "should return MDX query" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE ([Time].[2010].[Q1], [Customers].[USA].[CA])
          SQL
      end

      it "should return query with crossjoin" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]').
          where('[Time].[2010].[Q1]').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    CROSSJOIN([Product].children, {[Customers].[Canada], [Customers].[USA]}) ON ROWS
              FROM  [Sales]
              WHERE ([Time].[2010].[Q1])
          SQL
      end

      it "should return query with several crossjoins" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]').
          crossjoin('[Time].[2010].[Q1]', '[Time].[2010].[Q2]').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    CROSSJOIN(CROSSJOIN([Product].children, {[Customers].[Canada], [Customers].[USA]}),
                              {[Time].[2010].[Q1], [Time].[2010].[Q2]}) ON ROWS
              FROM  [Sales]
          SQL
      end

      it "should return query with crossjoin and nonempty" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]').nonempty.
          where('[Time].[2010].[Q1]').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    NON EMPTY CROSSJOIN([Product].children, {[Customers].[Canada], [Customers].[USA]}) ON ROWS
              FROM  [Sales]
              WHERE ([Time].[2010].[Q1])
          SQL
      end

      it "should return query with nonempty_crossjoin" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').nonempty_crossjoin('[Customers].[Canada]', '[Customers].[USA]').
          where('[Time].[2010].[Q1]').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    NONEMPTYCROSSJOIN([Product].children, {[Customers].[Canada], [Customers].[USA]}) ON ROWS
              FROM  [Sales]
              WHERE ([Time].[2010].[Q1])
          SQL
      end

      it "should return query with where with several same dimension members" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Customers].[Canada]', '[Customers].[USA]').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE {[Customers].[Canada], [Customers].[USA]}
          SQL
      end

      it "should return query with where with several different dimension members returned by function" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('Head([Customers].Members).Item(0)', 'Head([Gender].Members).Item(0)').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE (Head([Customers].Members).Item(0), Head([Gender].Members).Item(0))
          SQL
      end

      it "should return query with where with crossjoin" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Customers].[USA]').crossjoin('[Time].[2011].[Q1]', '[Time].[2011].[Q2]').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE CROSSJOIN({[Customers].[USA]}, {[Time].[2011].[Q1], [Time].[2011].[Q2]})
          SQL
      end

      it "should return query with where with nonempty_crossjoin" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Customers].[USA]').nonempty_crossjoin('[Time].[2011].[Q1]', '[Time].[2011].[Q2]').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE NONEMPTYCROSSJOIN({[Customers].[USA]}, {[Time].[2011].[Q1], [Time].[2011].[Q2]})
          SQL
      end

      it "should return query with order by one measure" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').order('[Measures].[Unit Sales]', :bdesc).
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    ORDER([Product].children, [Measures].[Unit Sales], BDESC) ON ROWS
              FROM  [Sales]
          SQL
      end

      it "should return query with order by measure and other member" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').order(['[Measures].[Unit Sales]', '[Customers].[USA]'], :asc).
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    ORDER([Product].children, ([Measures].[Unit Sales], [Customers].[USA]), ASC) ON ROWS
              FROM  [Sales]
          SQL
      end

      %w(top bottom).each do |extreme|
        it "should return query with #{extreme} count by one measure" do
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
            rows('[Product].children').send(:"#{extreme}_count", 5, '[Measures].[Unit Sales]').
            to_mdx.should be_like <<-SQL
              SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                      #{extreme.upcase}COUNT([Product].children, 5, [Measures].[Unit Sales]) ON ROWS
                FROM  [Sales]
            SQL
        end

        it "should return query with #{extreme} count without measure" do
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
            rows('[Product].children').send(:"#{extreme}_count", 5).
            to_mdx.should be_like <<-SQL
              SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                      #{extreme.upcase}COUNT([Product].children, 5) ON ROWS
                FROM  [Sales]
            SQL
        end

        it "should return query with #{extreme} count by measure and other member" do
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
            rows('[Product].children').send(:"#{extreme}_count", 5, ['[Measures].[Unit Sales]', '[Customers].[USA]']).
            to_mdx.should be_like <<-SQL
              SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                      #{extreme.upcase}COUNT([Product].children, 5, ([Measures].[Unit Sales], [Customers].[USA])) ON ROWS
                FROM  [Sales]
            SQL
        end

        it "should return query with #{extreme} percent by one measure" do
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
            rows('[Product].children').send(:"#{extreme}_percent", 20, '[Measures].[Unit Sales]').
            to_mdx.should be_like <<-SQL
              SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                      #{extreme.upcase}PERCENT([Product].children, 20, [Measures].[Unit Sales]) ON ROWS
                FROM  [Sales]
            SQL
        end

        it "should return query with #{extreme} sum by one measure" do
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
            rows('[Product].children').send(:"#{extreme}_sum", 1000, '[Measures].[Unit Sales]').
            to_mdx.should be_like <<-SQL
              SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                      #{extreme.upcase}SUM([Product].children, 1000, [Measures].[Unit Sales]) ON ROWS
                FROM  [Sales]
            SQL
        end
      end

      it "should return query with hierarchize" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize.
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    HIERARCHIZE({[Customers].[Country].Members, [Customers].[City].Members}) ON ROWS
              FROM  [Sales]
          SQL
      end

      it "should return query with hierarchize and order" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize(:post).
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    HIERARCHIZE({[Customers].[Country].Members, [Customers].[City].Members}, POST) ON ROWS
              FROM  [Sales]
          SQL
      end

      it "should return query with except" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').except('[Customers].[USA]').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    EXCEPT([Customers].[Country].Members, {[Customers].[USA]}) ON ROWS
              FROM  [Sales]
          SQL
      end

      it "should return query with filter" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').filter('[Measures].[Unit Sales] > 1000').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    FILTER([Customers].[Country].Members, [Measures].[Unit Sales] > 1000) ON ROWS
              FROM  [Sales]
          SQL
      end

      it "should return query with filter and set alias" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').filter('NOT ISEMPTY(S.CURRENT)', :as => 'S').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    FILTER([Customers].[Country].Members AS S, NOT ISEMPTY(S.CURRENT)) ON ROWS
              FROM  [Sales]
          SQL
      end

      it "should return query with filter non-empty" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').filter_nonempty.
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    FILTER([Customers].[Country].Members AS S, NOT ISEMPTY(S.CURRENT)) ON ROWS
              FROM  [Sales]
          SQL
      end

      it "should return query with generate" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').generate('[Customers].CurrentMember').
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    GENERATE([Customers].[Country].Members, [Customers].CurrentMember) ON ROWS
              FROM  [Sales]
          SQL
      end

      it "should return query with generate all" do
        @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').generate('[Customers].CurrentMember', :all).
          to_mdx.should be_like <<-SQL
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    GENERATE([Customers].[Country].Members, [Customers].CurrentMember, ALL) ON ROWS
              FROM  [Sales]
          SQL
      end

      it "should return query including WITH MEMBER clause" do
        @query.
          with_member('[Measures].[ProfitPct]').
            as('Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',
              :solve_order => 1, :format_string => 'Percent', :caption => 'Profit %').
          with_member('[Measures].[ProfitValue]').
            as('[Measures].[Store Sales] * [Measures].[ProfitPct]',
              :solve_order => 2, :cell_formatter => 'CurrencyFormatter').
          columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
          to_mdx.should be_like <<-SQL
            WITH
               MEMBER [Measures].[ProfitPct] AS
               'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',
               SOLVE_ORDER = 1, FORMAT_STRING = 'Percent', $caption = 'Profit %'
               MEMBER [Measures].[ProfitValue] AS
               '[Measures].[Store Sales] * [Measures].[ProfitPct]',
               SOLVE_ORDER = 2, CELL_FORMATTER = 'rubyobj.Mondrian.OLAP.Schema.CellFormatter.CurrencyFormatterUdf'
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE ([Time].[2010].[Q1], [Customers].[USA].[CA])
          SQL
      end

      it "should return query including WITH SET clause" do
        @query.with_set('CrossJoinSet').
            as('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]').
          with_set('MemberSet').as('[Product].[All Products]').
          with_set('FunctionSet').as('[Product].AllMembers').
          with_set('ItemSet').as('[Product].AllMembers.Item(0)').
          with_set('DefaultMemberSet').as('[Product].DefaultMember').
          with_member('[Measures].[Profit]').
            as('[Measures].[Store Sales] - [Measures].[Store Cost]').
          columns('[Measures].[Profit]').
          rows('CrossJoinSet').
          to_mdx.should be_like <<-SQL
            WITH
               SET CrossJoinSet AS 'CROSSJOIN([Product].children, {[Customers].[Canada], [Customers].[USA]})'
               SET MemberSet AS '{[Product].[All Products]}'
               SET FunctionSet AS '[Product].AllMembers'
               SET ItemSet AS '{[Product].AllMembers.Item(0)}'
               SET DefaultMemberSet AS '{[Product].DefaultMember}'
               MEMBER [Measures].[Profit] AS
               '[Measures].[Store Sales] - [Measures].[Store Cost]'
            SELECT  {[Measures].[Profit]} ON COLUMNS,
                    CrossJoinSet ON ROWS
              FROM  [Sales]
          SQL
      end
    end

    describe "execute" do
      it "should return result" do
        result = @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
          execute
        result.values.should == sql_select_numbers(@sql_select)
      end

      it "should not fail without columns" do
        result = @query.rows('[Product].DefaultMember').execute
        result.values.should == [[]]
      end
    end

    describe "result HTML formatting" do
      it "should format result" do
        result = @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
          execute
        Nokogiri::HTML.fragment(result.to_html).css('tr').size.should == (sql_select_numbers(@sql_select).size + 1)
      end
    end

  end

  describe "errors" do
    before(:each) do
      @query = @olap.from('Sales')
    end

    it "should raise error when invalid MDX statement" do
      expect {
        @olap.execute "SELECT dummy FROM"
      }.to raise_error {|e|
        e.should be_kind_of(Mondrian::OLAP::Error)
        e.message.should == 'org.olap4j.OlapException: mondrian gave exception while parsing query'
        e.root_cause_message.should == "Syntax error at line 1, column 14, token 'FROM'"
      }
    end

    it "should raise error when invalid MDX object" do
      expect {
        @query.columns('[Measures].[Dummy]').execute
      }.to raise_error {|e|
        e.should be_kind_of(Mondrian::OLAP::Error)
        e.message.should == 'org.olap4j.OlapException: mondrian gave exception while parsing query'
        e.root_cause_message.should == "MDX object '[Measures].[Dummy]' not found in cube 'Sales'"
      }
    end

    it "should raise error when invalid formula" do
      expect {
        @query.with_member('[Measures].[Dummy]').as('Dummy(123)').
          columns('[Measures].[Dummy]').execute
      }.to raise_error {|e|
        e.should be_kind_of(Mondrian::OLAP::Error)
        e.message.should == 'org.olap4j.OlapException: mondrian gave exception while parsing query'
        e.root_cause_message.should == "No function matches signature 'Dummy(<Numeric Expression>)'"
      }
    end

    it "should raise error when TokenMgrError is raised" do
      expect {
        @query.with_member('[Measures].[Dummy]').as('[Measures].[Store Sales]]').
          columns('[Measures].[Dummy]').execute
      }.to raise_error {|e|
        e.should be_kind_of(Mondrian::OLAP::Error)
        e.message.should =~ /mondrian\.parser\.TokenMgrError/
        e.root_cause_message.should =~ /Lexical error/
      }
    end

  end

  describe "drill through cell" do
    before(:all) do
      @query = @olap.from('Sales')
      @result = @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
        rows('[Product].children').
        where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
        execute
      @drill_through = @result.drill_through(:row => 0, :column => 0)
    end

    it "should return column types" do
      @drill_through.column_types.should == [
        :INT, :VARCHAR, :INT, :INT, :INT,
        :VARCHAR, :VARCHAR, :VARCHAR, :VARCHAR, :VARCHAR, :VARCHAR,
        :VARCHAR, :VARCHAR, :VARCHAR, :BIGINT,
        :VARCHAR,
        :DECIMAL
      ]
    end if MONDRIAN_DRIVER == 'mysql'

    it "should return column names" do
      # ignore calculated customer full name column name which is shown differently on each database
      @drill_through.column_names[0..12].should == %w(
        the_year quarter month_of_year week_of_year day_of_month
        product_family product_department product_category product_subcategory brand_name product_name
        state_province city
      )
      @drill_through.column_names[14..16].should == %w(
        id gender unit_sales
      )
    end if %w(mysql postgresql).include? MONDRIAN_DRIVER

    it "should return table names" do
      # ignore calculated customer full name column name which is shown differently on each database
      @drill_through.table_names[0..12].should == [
        "time", "time", "time", "time", "time",
        "product_classes", "product_classes", "product_classes", "product_classes", "products", "products",
        "customers", "customers"
      ]
      @drill_through.table_names[14..16].should == [
        "customers", "customers", "sales"
      ]
    end if %w(mysql postgresql).include? MONDRIAN_DRIVER

    it "should return column labels" do
      @drill_through.column_labels.should == [
        "Year", "Quarter", "Month", "Week", "Day",
        "Product Family", "Product Department", "Product Category", "Product Subcategory", "Brand Name", "Product Name",
        "State Province", "City", "Name", "Name (Key)",
        "Gender",
        "Unit Sales"
      ]
    end

    it "should return row values" do
      @drill_through.rows.size.should == 15 # number of generated test rows
    end

    it "should return correct row value types" do
      expected_value_types = case MONDRIAN_DRIVER
        when "oracle"
          [
            BigDecimal, String, BigDecimal, BigDecimal, BigDecimal,
            String, String, String, String, String, String,
            String, String, String, BigDecimal,
            String,
            BigDecimal
          ]
        else
          [
            Integer, String, Integer, Integer, Integer,
            String, String, String, String, String, String,
            String, String, String, Integer,
            String,
            BigDecimal
          ]
        end

      @drill_through.rows.first.each_with_index do |value, i|
        value.should be_a expected_value_types[i]
      end
    end

    it "should return only specified max rows" do
      drill_through = @result.drill_through(:row => 0, :column => 0, :max_rows => 10)
      drill_through.rows.size.should == 10
    end
  end

  describe "drill through cell with return" do
    before(:all) do
      @query = @olap.from('Sales')
      @result = @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
        rows('[Product].children').
        where('[Time].[2010].[Q1]', '[Time].[2010].[Q2]').
        execute
    end

    it "should return only specified fields in specified order" do
      @drill_through = @result.drill_through(:row => 0, :column => 0, :return => [
        '[Time].[Month]',
        '[Customers].[City]',
        '[Product].[Product Family]',
        '[Measures].[Unit Sales]', '[Measures].[Store Sales]'
      ])
      @drill_through.column_labels.should == [
        "Month (Key)",
        "City (Key)",
        "Product Family (Key)",
        "Unit Sales", "Store Sales"
      ]
    end

    it "should return rows also for field dimension that is not present in the report query" do
      result = @olap.from('Sales').columns('[Measures].[Unit Sales]').rows('[Customers].[Canada].[BC].[Burnaby]').execute
      drill_through = result.drill_through(row: 0, column: 0, return: ["[Product].[Product Family]"])
      drill_through.rows.should == @sql.select_rows(<<-SQL)
        SELECT
          product_classes.product_family
        FROM
          sales,
          products,
          product_classes,
          customers
        WHERE
          products.product_class_id = product_classes.id AND
          sales.product_id = products.id AND
          sales.customer_id = customers.id AND
          customers.country = 'Canada' AND customers.state_province = 'BC' AND customers.city = 'Burnaby'
      SQL
    end

    it "should return only nonempty measures" do
      @drill_through = @result.drill_through(:row => 0, :column => 0,
        :return => "[Measures].[Unit Sales], [Measures].[Store Sales]",
        :nonempty => "[Measures].[Unit Sales]"
      )
      @drill_through.column_labels.should == [
        "Unit Sales", "Store Sales"
      ]
      @drill_through.rows.all?{|r| r.any?{|c| c}}.should == true
    end

    it "should return member name and property values" do
      @drill_through = @result.drill_through(row: 0, column: 0,
        return: [
          "Name([Customers].[Name])",
          "Property([Customers].[Name], 'Gender')",
          "Property([Customers].[Name], 'Description')",
          "Property([Customers].[Name], 'Non-existing property name')"
        ]
      )
      @drill_through.column_labels.should == [
        "Name", "Gender", "Description",
        "Non-existing property name"
      ]
      @drill_through.rows.should == @sql.select_rows(<<-SQL)
        SELECT
          customers.fullname,
          customers.gender,
          customers.description,
          '' as non_existing
        FROM
          sales,
          customers,
          time,
          products,
          product_classes
        WHERE
          (time.quarter = 'Q1' OR time.quarter = 'Q2') AND
          time.the_year = 2010 AND
          product_classes.product_family = 'Drink' AND
          products.product_class_id = product_classes.id AND
          sales.product_id = products.id AND
          sales.time_id = time.id AND
          customers.id = sales.customer_id
        ORDER BY
          customers.fullname,
          customers.gender,
          customers.description
      SQL
    end

    it "should group by" do
      @drill_through = @result.drill_through(row: 0, column: 0,
        return: [
          "[Product].[Product Family]",
          "[Measures].[Unit Sales]",
          "[Measures].[Store Cost]"
        ],
        group_by: true
      )
      @drill_through.column_labels.should == [ "Product Family (Key)", "Unit Sales", "Store Cost" ]
      @drill_through.rows.should == @sql.select_rows(<<-SQL
        SELECT
          product_classes.product_family,
          SUM(sales.unit_sales) AS unit_sales,
          SUM(sales.store_cost) AS store_cost
        FROM
          sales,
          time,
          products,
          product_classes
        WHERE
          (time.quarter = 'Q1' OR time.quarter = 'Q2') AND
          time.the_year = 2010 AND
          product_classes.product_family = 'Drink' AND
          products.product_class_id = product_classes.id AND
          sales.product_id = products.id AND
          sales.time_id = time.id
        GROUP BY
          product_classes.product_family
      SQL
      )
    end
  end

  describe "drill through cell with return and role restrictions" do
    before(:all) do
      @olap.role_name = "Mexico manager"
      @query = @olap.from('Sales')
      @result = @query.columns('[Measures].[Unit Sales]').
        rows('[Customers].[All Customers]').
        execute
      @drill_through = @result.drill_through(
        row: 0,
        column: 0,
        return: ['[Customers].[Country]', '[Measures].[Unit Sales]'],
        max_rows: 10
      )
    end

    after(:all) do
      @olap.role_name = nil
    end

    it "should return data according to role restriction" do
      @drill_through.rows.all? { |r| r.first == "Mexico" }.should == true
    end

    it "should return only specified max rows" do
      @drill_through.rows.size.should == 10
    end
  end

  describe "drill through virtual cube cell with return" do
    before(:all) do
      @query = @olap.from('Sales and Warehouse')
      @result = @query.columns(
          '[Measures].[Unit Sales]', '[Measures].[Store Sales]',
          '[Measures].[Units Shipped]', '[Measures].[Products with units shipped]'
        ).
        rows('[Product].children').
        where('[Time].[2010].[Q1]', '[Time].[2010].[Q2]').
        execute
    end

    it "should return specified fields from other cubes as empty strings" do
      @drill_through = @result.drill_through(:row => 0, :column => 3, :return => [
        '[Time].[Month]',
        '[Product].[Product Family]',
        '[Customers].[City]', # missing in Warehouse cube
        '[Measures].[Unit Sales]', # missing in Warehouse cube
        '[Measures].[Units Shipped]',
        '[Measures].[Products with units shipped]'
      ])
      @drill_through.column_labels.should == [
        "Month (Key)",
        "Product Family (Key)",
        "City (Key)",
        "Unit Sales",
        "Units Shipped",
        "Products with units shipped"
      ]
      # Validate that only City and Unit Sales values are missing
      @drill_through.rows.map { |r| r.map(&:present?) }.uniq.should == [
        [true, true, false, false, true, true]
      ]
    end
  end

  describe "drill through statement" do
    before(:all) do
      @query = @olap.from('Sales').
        columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
        rows('[Product].children').
        where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]')
    end

    it "should return column labels" do
      @drill_through = @query.execute_drill_through
      @drill_through.column_labels.should == [
        "Year", "Quarter", "Month", "Week", "Day",
        "Product Family", "Product Department", "Product Category", "Product Subcategory", "Brand Name", "Product Name",
        "State Province", "City", "Name", "Name (Key)",
        "Gender",
        "Unit Sales"
      ]
    end

    it "should return row values" do
      @drill_through = @query.execute_drill_through
      @drill_through.rows.size.should == 15 # number of generated test rows
    end

    it "should return only specified max rows" do
      drill_through = @query.execute_drill_through(:max_rows => 10)
      drill_through.rows.size.should == 10
    end

    it "should return only specified fields" do
      @drill_through = @query.execute_drill_through(:return => [
        '[Time].[Month]',
        '[Product].[Product Family]',
        '[Customers].[City]',
        '[Measures].[Unit Sales]', '[Measures].[Store Sales]'
      ])
      @drill_through.column_labels.should == [
        "Month",
        "Product Family",
        "City",
        "Unit Sales", "Store Sales"
      ]
    end

  end

  describe "parse expression" do
    it "should parse expression" do
      @olap.parse_expression("1").should be_kind_of(Java::MondrianOlap::Literal)
    end

    it "should raise error when invalid expression" do
      expression = "1, dummy"
      expect {
        @olap.parse_expression expression
      }.to raise_error { |e|
        e.should be_kind_of(Mondrian::OLAP::Error)
        e.message.should == "mondrian.olap.MondrianException: Mondrian Error:Failed to parse query '#{expression}'"
        e.root_cause_message.should == "Syntax error at line 1, column 2, token ','"
      }
    end
  end

  describe "schema cache" do
    before(:all) do
      product_id = @sql.select_value("SELECT MIN(id) FROM products")
      time_id = @sql.select_value("SELECT MIN(id) FROM #{qt :time}")
      customer_id = @sql.select_value("SELECT MIN(id) FROM customers")
      @condition = "product_id = #{product_id} AND time_id = #{time_id} AND customer_id = #{customer_id}"
      # check expected initial value
      @first_unit_sales = 1
      @sql.select_value("SELECT unit_sales FROM sales WHERE #{@condition}").to_i.should == @first_unit_sales
    end

    before do
      create_olap_connection
      @unit_sales = query_unit_sales_value

      update_first_unit_sales(@first_unit_sales + 1)

      # should still use previous value from cache
      create_olap_connection
      query_unit_sales_value.should == @unit_sales
    end

    after do
      update_first_unit_sales(@first_unit_sales)
      Mondrian::OLAP::Connection.flush_schema_cache
    end

    def create_olap_connection(options = {})
      @olap2.close if @olap2
      @olap2 = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG.merge(options))
    end

    def update_first_unit_sales(value)
      @sql.update "UPDATE sales SET unit_sales = #{value} WHERE #{@condition}"
    end

    def query_unit_sales_value
      @olap2.from('Sales').columns('[Measures].[Unit Sales]').execute.values.first
    end

    it "should flush schema cache" do
      @olap2.flush_schema
      create_olap_connection
      query_unit_sales_value.should == @unit_sales + 1
    end

    it "should remove schema by key" do
      Mondrian::OLAP::Connection.flush_schema(@olap2.schema_key)
      create_olap_connection
      query_unit_sales_value.should == @unit_sales + 1
    end

  end

  describe "profiling" do
    before(:all) do
      if @olap
        @olap.flush_schema
        @olap.close
      end
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG)
      @result = @olap.execute "SELECT [Measures].[Unit Sales] ON COLUMNS, [Product].Children ON ROWS FROM [Sales]", profiling: true
      @result.profiling_mark_full("MDX query time", 100)
    end

    it "should return query plan" do
      @result.profiling_plan.strip.should == <<-EOS.strip
Axis (COLUMNS):
SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc, type=SetType<MemberType<member=[Measures].[Unit Sales]>>, resultStyle=MUTABLE_LIST)
    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE)
        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales])

Axis (ROWS):
Children(name=Children, class=class mondrian.olap.fun.BuiltinFunTable$22$1, type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=LIST)
    CurrentMemberFixed(hierarchy=[Product], name=CurrentMemberFixed, class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Product]>, resultStyle=VALUE)
      EOS
    end

    it "should return SQL timing string" do
      @result.profiling_timing_string.strip.should =~
        %r{^SqlStatement-Segment.load invoked 1 times for total of \d+ms.  \(Avg. \d+ms/invocation\)$}
    end

    it "should return custom profiling string" do
      @result.profiling_timing_string.strip.should =~
        %r{^MDX query time invoked 1 times for total of 100ms.  \(Avg. 100ms/invocation\)$}
    end

    it "should return total duration" do
      @result.total_duration.should > 0
    end
  end

  describe "error with profiling" do
    before(:all) do
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG)
      begin
        @olap.execute <<-MDX, profiling: true
          SELECT [Measures].[Unit Sales] ON COLUMNS,
          FILTER([Customers].Children, ([Customers].DefaultMember, [Measures].[Unit Sales]) > 'dummy') ON ROWS
          FROM [Sales]
        MDX
      rescue => e
        @error = e
      end
    end

    it "should return query plan" do
      @error.profiling_plan.should =~ /^Axis \(COLUMNS\):/
    end

    it "should return timing string" do
      @error.profiling_timing_string.should =~
        %r{^FilterFunDef invoked 1 times for total of \d+ms.  \(Avg. \d+ms/invocation\)$}
    end
  end

  describe "timeout" do
    before(:all) do
      @schema = Mondrian::OLAP::Schema.new
      @schema.define do
        cube 'Sales' do
          table 'sales'
          dimension 'Customers', foreign_key: 'customer_id' do
            hierarchy all_member_name: 'All Customers', primary_key: 'id' do
              table 'customers'
              level 'Name', column: 'fullname'
            end
          end
          calculated_member 'Sleep 5' do
            dimension 'Measures'
            formula 'Sleep(5)'
          end
          calculated_member 'Sleep 0' do
            dimension 'Measures'
            formula 'Sleep(0)'
          end
        end
        user_defined_function 'Sleep' do
          ruby do
            parameters :numeric
            returns :numeric
            def call(n)
              sleep n
              n
            end
          end
        end
      end
      @olap = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS.merge schema: @schema)
    end

    it "should raise timeout error for long queries" do
      expect do
        @olap.from('Sales').columns('[Measures].[Sleep 5]').execute(timeout: 0.1)
      end.to raise_error do |e|
        e.should be_kind_of(Mondrian::OLAP::Error)
        e.message.should == 'org.olap4j.OlapException: Mondrian Error:Query timeout of 0 seconds reached'
      end
    end

    it "should not raise timeout error for short queries" do
      @olap.from('Sales').columns('[Measures].[Sleep 0]').execute(timeout: 1).values.should == [0]
    end
  end

end
