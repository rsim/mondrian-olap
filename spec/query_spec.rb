require "spec_helper"

describe "Query" do
  def quote_table_name(name)
    ActiveRecord::Base.connection.quote_table_name(name)
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
      LEFT JOIN #{quote_table_name('time')} ON sales.time_id = #{quote_table_name('time')}.id
      LEFT JOIN customers ON sales.customer_id = customers.id
    WHERE #{quote_table_name('time')}.the_year = 2010 AND #{quote_table_name('time')}.quarter = 'Q1'
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
      @result.formatted_values.map{|r| r.map{|s| BigDecimal.new(s.gsub(',',''))}}.should == @expected_result_values
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
        @query.rows('[Customers].[Country].Members').filter('NOT ISEMPTY(S.CURRENT)', :as => 'S')
        @query.rows.should == [:filter, ['[Customers].[Country].Members'], 'NOT ISEMPTY(S.CURRENT)', 'S']
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

      it "should return query including WITH MEMBER clause" do
        @query.
          with_member('[Measures].[ProfitPct]').
            as('Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',
              :solve_order => 1, :format_string => 'Percent').
          with_member('[Measures].[ProfitValue]').
            as('[Measures].[Store Sales] * [Measures].[ProfitPct]',
              :solve_order => 2, :format_string => 'Currency').
          columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
          to_mdx.should be_like <<-SQL
            WITH
               MEMBER [Measures].[ProfitPct] AS
               'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',
               SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'
               MEMBER [Measures].[ProfitValue] AS
               '[Measures].[Store Sales] * [Measures].[ProfitPct]',
               SOLVE_ORDER = 2, FORMAT_STRING = 'Currency'
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE ([Time].[2010].[Q1], [Customers].[USA].[CA])
          SQL
      end

      it "should return query including WITH SET clause" do
        @query.with_set('SelectedRows').
            as('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]').
          with_member('[Measures].[Profit]').
            as('[Measures].[Store Sales] - [Measures].[Store Cost]').
          columns('[Measures].[Profit]').
          rows('SelectedRows').
          to_mdx.should be_like <<-SQL
            WITH
               SET SelectedRows AS
               'CROSSJOIN([Product].children, {[Customers].[Canada], [Customers].[USA]})'
               MEMBER [Measures].[Profit] AS
               '[Measures].[Store Sales] - [Measures].[Store Cost]'
            SELECT  {[Measures].[Profit]} ON COLUMNS,
                    SelectedRows ON ROWS
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
    end

    describe "result HTML formatting" do
      it "should format result" do
        result = @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
          execute
        Nokogiri::HTML.fragment(result.to_html).css('tr').size.should == (sql_select_numbers(@sql_select).size + 1)
      end

      # it "test" do
      #   puts @olap.from('Sales').
      #     columns('[Product].children').
      #     rows('[Customers].[USA].[CA].children').
      #     where('[Time].[2010].[Q1]', '[Measures].[Store Sales]').
      #     execute.to_html
      # end
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

  end

  describe "drill through" do
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
        :VARCHAR, :VARCHAR, :VARCHAR, :INT,
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
      @drill_through.table_names.should == [
        "time", "time", "time", "time", "time",
        "product_classes", "product_classes", "product_classes", "product_classes", "products", "products",
        "customers", "customers", "", "customers",
        "customers",
        "sales"
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
      @drill_through.rows.first.map(&:class).should ==
        case MONDRIAN_DRIVER
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
            Fixnum, String, Fixnum, Fixnum, Fixnum,
            String, String, String, String, String, String,
            String, String, String, Fixnum,
            String,
            BigDecimal
          ]
        end
    end

    it "should return only specified max rows" do
      drill_through = @result.drill_through(:row => 0, :column => 0, :max_rows => 10)
      drill_through.rows.size.should == 10
    end
  end


end
