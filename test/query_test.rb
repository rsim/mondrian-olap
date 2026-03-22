# frozen_string_literal: true

require_relative "test_helper"

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
      rows.map { |col| BigDecimal(col.to_s) }
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
        [@expected_result_values.map { |row| row[0] }, @expected_result_values.map { |row| row[1] }]

      @result = @olap.execute @query_string
    end

    it "should return axes" do
      assert_equal 2, @result.axes_count
    end

    it "should return column names" do
      assert_equal @expected_column_names, @result.column_names
      assert_equal @expected_column_full_names, @result.column_full_names
    end

    it "should return row names" do
      assert_equal @expected_row_names, @result.row_names
      assert_equal @expected_row_full_names, @result.row_full_names
    end

    it "should return axis by index names" do
      assert_equal @expected_column_names, @result.axis_names[0]
      assert_equal @expected_column_full_names, @result.axis_full_names[0]
    end

    it "should return column members" do
      assert_equal @expected_column_names, @result.column_members.map(&:name)
      assert_equal @expected_column_full_names, @result.column_members.map(&:full_name)
      assert_equal @expected_drillable_columns, @result.column_members.map(&:"drillable?")
    end

    it "should return row members" do
      assert_equal @expected_row_names, @result.row_members.map(&:name)
      assert_equal @expected_row_full_names, @result.row_members.map(&:full_name)
      assert_equal @expected_drillable_rows, @result.row_members.map(&:"drillable?")
    end

    it "should return cells" do
      assert_equal @expected_result_values, @result.values
    end

    it "should return cells with specified axes number sequence" do
      assert_equal @expected_result_values_by_columns, @result.values(0, 1)
    end

    it "should return cells with specified axes name sequence" do
      assert_equal @expected_result_values_by_columns, @result.values(:columns, :rows)
    end

    it "should return formatted cells" do
      assert_equal @expected_result_values, @result.formatted_values.map { |r| r.map { |s| BigDecimal(s.gsub(',', '')) } }
    end

  end

  describe "builder" do

    before do
      @query = @olap.from('Sales')
    end

    describe "from cube" do
      it "should return query" do
        assert_kind_of Mondrian::OLAP::Query, @query
        assert_equal 'Sales', @query.cube_name
      end
    end

    describe "columns" do
      it "should accept list" do
        assert_same @query, @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]')
        assert_equal ['[Measures].[Unit Sales]', '[Measures].[Store Sales]'], @query.columns
      end

      it "should accept list as array" do
        @query.columns(['[Measures].[Unit Sales]', '[Measures].[Store Sales]'])
        assert_equal ['[Measures].[Unit Sales]', '[Measures].[Store Sales]'], @query.columns
      end

      it "should accept with several method calls" do
        @query.columns('[Measures].[Unit Sales]').columns('[Measures].[Store Sales]')
        assert_equal ['[Measures].[Unit Sales]', '[Measures].[Store Sales]'], @query.columns
      end
    end

    describe "other axis" do
      it "should accept axis with index member list" do
        @query.axis(0, '[Measures].[Unit Sales]', '[Measures].[Store Sales]')
        assert_equal ['[Measures].[Unit Sales]', '[Measures].[Store Sales]'], @query.axis(0)
      end

      it "should accept rows list" do
        @query.rows('[Product].children')
        assert_equal ['[Product].children'], @query.rows
      end

      it "should accept pages list" do
        @query.pages('[Product].children')
        assert_equal ['[Product].children'], @query.pages
      end
    end

    describe "crossjoin" do
      it "should do crossjoin of several dimensions" do
        @query.rows('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]')
        assert_equal [:crossjoin, ['[Product].children'], ['[Customers].[Canada]', '[Customers].[USA]']], @query.rows
      end

      it "should do crossjoin passing array as first argument" do
        @query.rows('[Product].children').crossjoin(['[Customers].[Canada]', '[Customers].[USA]'])
        assert_equal [:crossjoin, ['[Product].children'], ['[Customers].[Canada]', '[Customers].[USA]']], @query.rows
      end
    end

    describe "nonempty_crossjoin" do
      it "should do nonempty_crossjoin of several dimensions" do
        @query.rows('[Product].children').nonempty_crossjoin('[Customers].[Canada]', '[Customers].[USA]')
        assert_equal [:nonempty_crossjoin, ['[Product].children'], ['[Customers].[Canada]', '[Customers].[USA]']], @query.rows
      end
    end

    describe "nonempty" do
      it "should limit to set of members with nonempty values" do
        @query.rows('[Product].children').nonempty
        assert_equal [:nonempty, ['[Product].children']], @query.rows
      end
    end

    describe "distinct" do
      it "should limit to set of distinct tuples" do
        @query.rows('[Product].children').distinct.nonempty.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]')
        assert_equal [:nonempty, [:distinct, ["[Product].children"]]], @query.rows
      end
    end

    describe "order" do
      it "should order by one measure" do
        @query.rows('[Product].children').order('[Measures].[Unit Sales]', :bdesc)
        assert_equal [:order, ['[Product].children'], '[Measures].[Unit Sales]', 'BDESC'], @query.rows
      end

      it "should order using String order direction" do
        @query.rows('[Product].children').order('[Measures].[Unit Sales]', 'DESC')
        assert_equal [:order, ['[Product].children'], '[Measures].[Unit Sales]', 'DESC'], @query.rows
      end

      it "should order by measure and other member" do
        @query.rows('[Product].children').order(['[Measures].[Unit Sales]', '[Customers].[USA]'], :basc)
        assert_equal [:order, ['[Product].children'], ['[Measures].[Unit Sales]', '[Customers].[USA]'], 'BASC'], @query.rows
      end
    end

    %w(top bottom).each do |extreme|
      describe extreme do
        it "should select #{extreme} count rows by measure" do
          @query.rows('[Product].children').send(:"#{extreme}_count", 5, '[Measures].[Unit Sales]')
          assert_equal [:"#{extreme}_count", ['[Product].children'], 5, '[Measures].[Unit Sales]'], @query.rows
        end

        it "should select #{extreme} count rows without measure" do
          @query.rows('[Product].children').send(:"#{extreme}_count", 5)
          assert_equal [:"#{extreme}_count", ['[Product].children'], 5], @query.rows
        end

        it "should select #{extreme} percent rows by measure" do
          @query.rows('[Product].children').send(:"#{extreme}_percent", 20, '[Measures].[Unit Sales]')
          assert_equal [:"#{extreme}_percent", ['[Product].children'], 20, '[Measures].[Unit Sales]'], @query.rows
        end

        it "should select #{extreme} sum rows by measure" do
          @query.rows('[Product].children').send(:"#{extreme}_sum", 1000, '[Measures].[Unit Sales]')
          assert_equal [:"#{extreme}_sum", ['[Product].children'], 1000, '[Measures].[Unit Sales]'], @query.rows
        end
      end
    end

    describe "hierarchize" do
      it "should hierarchize simple set" do
        @query.rows('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize
        assert_equal [:hierarchize, ['[Customers].[Country].Members', '[Customers].[City].Members']], @query.rows
      end

      it "should hierarchize last set of crossjoin" do
        @query.rows('[Product].children').crossjoin('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize
        assert_equal [:crossjoin, ['[Product].children'],
          [:hierarchize, ['[Customers].[Country].Members', '[Customers].[City].Members']]], @query.rows
      end

      it "should hierarchize last set of nonempty_crossjoin" do
        @query.rows('[Product].children').nonempty_crossjoin('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize
        assert_equal [:nonempty_crossjoin, ['[Product].children'],
          [:hierarchize, ['[Customers].[Country].Members', '[Customers].[City].Members']]], @query.rows
      end

      it "should hierarchize all crossjoin" do
        @query.rows('[Product].children').crossjoin('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize_all
        assert_equal [:hierarchize, [:crossjoin, ['[Product].children'],
          ['[Customers].[Country].Members', '[Customers].[City].Members']]], @query.rows
      end

      it "should hierarchize with POST" do
        @query.rows('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize(:post)
        assert_equal [:hierarchize, ['[Customers].[Country].Members', '[Customers].[City].Members'], 'POST'], @query.rows
      end

    end

    describe "except" do
      it "should except one set from other" do
        @query.rows('[Customers].[Country].Members').except('[Customers].[USA]')
        assert_equal [:except, ['[Customers].[Country].Members'], ['[Customers].[USA]']], @query.rows
      end

      it "should except from last set of crossjoin" do
        @query.rows('[Product].children').crossjoin('[Customers].[Country].Members').except('[Customers].[USA]')
        assert_equal [:crossjoin, ['[Product].children'],
          [:except, ['[Customers].[Country].Members'], ['[Customers].[USA]']]], @query.rows
      end

      it "should except from last set of nonempty_crossjoin" do
        @query.rows('[Product].children').nonempty_crossjoin('[Customers].[Country].Members').except('[Customers].[USA]')
        assert_equal [:nonempty_crossjoin, ['[Product].children'],
          [:except, ['[Customers].[Country].Members'], ['[Customers].[USA]']]], @query.rows
      end
    end

    describe "filter" do
      it "should filter set by condition" do
        @query.rows('[Customers].[Country].Members').filter('[Measures].[Unit Sales] > 1000')
        assert_equal [:filter, ['[Customers].[Country].Members'], '[Measures].[Unit Sales] > 1000'], @query.rows
      end

      it "should filter using set alias" do
        @query.rows('[Customers].[Country].Members').filter('NOT ISEMPTY(S.CURRENT)', as: 'S')
        assert_equal [:filter, ['[Customers].[Country].Members'], 'NOT ISEMPTY(S.CURRENT)', 'S'], @query.rows
      end

      it "should filter last set of nonempty_crossjoin" do
        @query.rows('[Product].children').nonempty_crossjoin('[Customers].[Country].Members').
          filter_last("[Customers].CurrentMember.Name = 'USA'")
        assert_equal [:nonempty_crossjoin, ['[Product].children'],
          [:filter, ['[Customers].[Country].Members'], "[Customers].CurrentMember.Name = 'USA'"]], @query.rows
      end
    end

    describe "generate" do
      it "should generate new set" do
        @query.rows('[Customers].[Country].Members').generate('[Customers].CurrentMember')
        assert_equal [:generate, ['[Customers].[Country].Members'], ['[Customers].CurrentMember']], @query.rows
      end

      it "should generate new set with all option" do
        @query.rows('[Customers].[Country].Members').generate('[Customers].CurrentMember', :all)
        assert_equal [:generate, ['[Customers].[Country].Members'], ['[Customers].CurrentMember'], 'ALL'], @query.rows
      end
    end

    describe "where" do
      it "should accept conditions" do
        assert_same @query, @query.where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]')
        assert_equal ['[Time].[2010].[Q1]', '[Customers].[USA].[CA]'], @query.where
      end

      it "should accept conditions as array" do
        @query.where(['[Time].[2010].[Q1]', '[Customers].[USA].[CA]'])
        assert_equal ['[Time].[2010].[Q1]', '[Customers].[USA].[CA]'], @query.where
      end

      it "should accept conditions with several method calls" do
        @query.where('[Time].[2010].[Q1]').where('[Customers].[USA].[CA]')
        assert_equal ['[Time].[2010].[Q1]', '[Customers].[USA].[CA]'], @query.where
      end

      it "should do crossjoin of where conditions" do
        @query.where('[Customers].[USA]').crossjoin('[Time].[2011].[Q1]', '[Time].[2011].[Q2]')
        assert_equal [:crossjoin, ['[Customers].[USA]'], ['[Time].[2011].[Q1]', '[Time].[2011].[Q2]']], @query.where
      end

      it "should do nonempty_crossjoin of where conditions" do
        @query.where('[Customers].[USA]').nonempty_crossjoin('[Time].[2011].[Q1]', '[Time].[2011].[Q2]')
        assert_equal [:nonempty_crossjoin, ['[Customers].[USA]'], ['[Time].[2011].[Q1]', '[Time].[2011].[Q2]']], @query.where
      end
    end

    describe "with member" do
      it "should accept definition" do
        assert_same @query, @query.with_member('[Measures].[ProfitPct]').
          as('Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])')
        assert_equal [
          [ :member, '[Measures].[ProfitPct]',
            'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])'
          ]
        ], @query.with
      end

      it "should accept definition with additional parameters" do
        @query.with_member('[Measures].[ProfitPct]').
          as('Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',
            solve_order: 1,
            format_string: 'Percent')
        assert_equal [
          [ :member, '[Measures].[ProfitPct]',
            'Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',
            {solve_order: 1, format_string: 'Percent'}
          ]
        ], @query.with
      end
    end

    describe "with set" do
      it "should accept simple defition" do
        @query.with_set('SelectedRows').as('[Product].children')
        assert_equal [
          [ :set, 'SelectedRows',
            ['[Product].children']
          ]
        ], @query.with
      end

      it "should accept definition with crossjoin" do
        @query.with_set('SelectedRows').as('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]')
        assert_equal [
          [ :set, 'SelectedRows',
            [:crossjoin, ['[Product].children'], ['[Customers].[Canada]', '[Customers].[USA]']]
          ]
        ], @query.with
      end

      it "should accept definition with nonempty_crossjoin" do
        @query.with_set('SelectedRows').as('[Product].children').nonempty_crossjoin('[Customers].[Canada]', '[Customers].[USA]')
        assert_equal [
          [ :set, 'SelectedRows',
            [:nonempty_crossjoin, ['[Product].children'], ['[Customers].[Canada]', '[Customers].[USA]']]
          ]
        ], @query.with
      end
    end

    describe "to MDX" do
      it "should return MDX query" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE ([Time].[2010].[Q1], [Customers].[USA].[CA])
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
          to_mdx
      end

      it "should return query with crossjoin" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    CROSSJOIN([Product].children, {[Customers].[Canada], [Customers].[USA]}) ON ROWS
              FROM  [Sales]
              WHERE ([Time].[2010].[Q1])
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]').
          where('[Time].[2010].[Q1]').
          to_mdx
      end

      it "should return query with several crossjoins" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    CROSSJOIN(CROSSJOIN([Product].children, {[Customers].[Canada], [Customers].[USA]}),
                              {[Time].[2010].[Q1], [Time].[2010].[Q2]}) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]').
          crossjoin('[Time].[2010].[Q1]', '[Time].[2010].[Q2]').
          to_mdx
      end

      it "should return query with crossjoin and nonempty" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    NON EMPTY CROSSJOIN([Product].children, {[Customers].[Canada], [Customers].[USA]}) ON ROWS
              FROM  [Sales]
              WHERE ([Time].[2010].[Q1])
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]').nonempty.
          where('[Time].[2010].[Q1]').
          to_mdx
      end

      it "should return query with nonempty_crossjoin" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    NONEMPTYCROSSJOIN([Product].children, {[Customers].[Canada], [Customers].[USA]}) ON ROWS
              FROM  [Sales]
              WHERE ([Time].[2010].[Q1])
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').nonempty_crossjoin('[Customers].[Canada]', '[Customers].[USA]').
          where('[Time].[2010].[Q1]').
          to_mdx
      end

      it "should return query with where with several same dimension members" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE {[Customers].[Canada], [Customers].[USA]}
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Customers].[Canada]', '[Customers].[USA]').
          to_mdx
      end

      it "should return query with where with several different dimension members returned by function" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE (Head([Customers].Members).Item(0), Head([Gender].Members).Item(0))
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('Head([Customers].Members).Item(0)', 'Head([Gender].Members).Item(0)').
          to_mdx
      end

      it "should return query with where with crossjoin" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE CROSSJOIN({[Customers].[USA]}, {[Time].[2011].[Q1], [Time].[2011].[Q2]})
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Customers].[USA]').crossjoin('[Time].[2011].[Q1]', '[Time].[2011].[Q2]').
          to_mdx
      end

      it "should return query with where with nonempty_crossjoin" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    [Product].children ON ROWS
              FROM  [Sales]
              WHERE NONEMPTYCROSSJOIN({[Customers].[USA]}, {[Time].[2011].[Q1], [Time].[2011].[Q2]})
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Customers].[USA]').nonempty_crossjoin('[Time].[2011].[Q1]', '[Time].[2011].[Q2]').
          to_mdx
      end

      it "should return query with order by one measure" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    ORDER([Product].children, [Measures].[Unit Sales], BDESC) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').order('[Measures].[Unit Sales]', :bdesc).
          to_mdx
      end

      it "should return query with order by measure and other member" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    ORDER([Product].children, ([Measures].[Unit Sales], [Customers].[USA]), ASC) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').order(['[Measures].[Unit Sales]', '[Customers].[USA]'], :asc).
          to_mdx
      end

      %w(top bottom).each do |extreme|
        it "should return query with #{extreme} count by one measure" do
          assert_like <<~MDX,
              SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                      #{extreme.upcase}COUNT([Product].children, 5, [Measures].[Unit Sales]) ON ROWS
                FROM  [Sales]
            MDX
            @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
            rows('[Product].children').send(:"#{extreme}_count", 5, '[Measures].[Unit Sales]').
            to_mdx
        end

        it "should return query with #{extreme} count without measure" do
          assert_like <<~MDX,
              SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                      #{extreme.upcase}COUNT([Product].children, 5) ON ROWS
                FROM  [Sales]
            MDX
            @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
            rows('[Product].children').send(:"#{extreme}_count", 5).
            to_mdx
        end

        it "should return query with #{extreme} count by measure and other member" do
          assert_like <<~MDX,
              SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                      #{extreme.upcase}COUNT([Product].children, 5, ([Measures].[Unit Sales], [Customers].[USA])) ON ROWS
                FROM  [Sales]
            MDX
            @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
            rows('[Product].children').send(:"#{extreme}_count", 5, ['[Measures].[Unit Sales]', '[Customers].[USA]']).
            to_mdx
        end

        it "should return query with #{extreme} percent by one measure" do
          assert_like <<~MDX,
              SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                      #{extreme.upcase}PERCENT([Product].children, 20, [Measures].[Unit Sales]) ON ROWS
                FROM  [Sales]
            MDX
            @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
            rows('[Product].children').send(:"#{extreme}_percent", 20, '[Measures].[Unit Sales]').
            to_mdx
        end

        it "should return query with #{extreme} sum by one measure" do
          assert_like <<~MDX,
              SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                      #{extreme.upcase}SUM([Product].children, 1000, [Measures].[Unit Sales]) ON ROWS
                FROM  [Sales]
            MDX
            @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
            rows('[Product].children').send(:"#{extreme}_sum", 1000, '[Measures].[Unit Sales]').
            to_mdx
        end
      end

      it "should return query with hierarchize" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    HIERARCHIZE({[Customers].[Country].Members, [Customers].[City].Members}) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize.
          to_mdx
      end

      it "should return query with hierarchize and order" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    HIERARCHIZE({[Customers].[Country].Members, [Customers].[City].Members}, POST) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members', '[Customers].[City].Members').hierarchize(:post).
          to_mdx
      end

      it "should return query with except" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    EXCEPT([Customers].[Country].Members, {[Customers].[USA]}) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').except('[Customers].[USA]').
          to_mdx
      end

      it "should return query with filter" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    FILTER([Customers].[Country].Members, [Measures].[Unit Sales] > 1000) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').filter('[Measures].[Unit Sales] > 1000').
          to_mdx
      end

      it "should return query with filter and set alias" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    FILTER([Customers].[Country].Members AS S, NOT ISEMPTY(S.CURRENT)) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').filter('NOT ISEMPTY(S.CURRENT)', as: 'S').
          to_mdx
      end

      it "should return query with filter non-empty" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    FILTER([Customers].[Country].Members AS S, NOT ISEMPTY(S.CURRENT)) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').filter_nonempty.
          to_mdx
      end

      it "should return query with generate" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    GENERATE([Customers].[Country].Members, [Customers].CurrentMember) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').generate('[Customers].CurrentMember').
          to_mdx
      end

      it "should return query with generate all" do
        assert_like <<~MDX,
            SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
                    GENERATE([Customers].[Country].Members, [Customers].CurrentMember, ALL) ON ROWS
              FROM  [Sales]
          MDX
          @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Customers].[Country].Members').generate('[Customers].CurrentMember', :all).
          to_mdx
      end

      it "should return query including WITH MEMBER clause" do
        assert_like <<~MDX,
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
          MDX
          @query.
          with_member('[Measures].[ProfitPct]').
            as('Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',
              solve_order: 1, format_string: 'Percent', caption: 'Profit %').
          with_member('[Measures].[ProfitValue]').
            as('[Measures].[Store Sales] * [Measures].[ProfitPct]',
              solve_order: 2, cell_formatter: 'CurrencyFormatter').
          columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
          to_mdx
      end

      it "should return query including WITH SET clause" do
        assert_like <<~MDX,
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
          MDX
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
          to_mdx
      end
    end

    describe "execute" do
      it "should return result" do
        result = @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
          execute
        assert_equal sql_select_numbers(@sql_select), result.values
      end

      it "should not fail without columns" do
        result = @query.rows('[Product].DefaultMember').execute
        assert_equal [[]], result.values
      end
    end

    describe "result HTML formatting" do
      it "should format result" do
        result = @query.columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
          rows('[Product].children').
          where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
          execute
        assert_equal (sql_select_numbers(@sql_select).size + 1), Nokogiri::HTML.fragment(result.to_html).css('tr').size
      end
    end

  end

  describe "errors" do
    before do
      @query = @olap.from('Sales')
    end

    it "should raise error when invalid MDX statement" do
      error = assert_raises(Mondrian::OLAP::Error) {
        @olap.execute "SELECT dummy FROM"
      }
      assert_kind_of Mondrian::OLAP::Error, error
      assert_equal 'org.olap4j.OlapException: mondrian gave exception while parsing query', error.message
      assert_equal "Syntax error at line 1, column 14, token 'FROM'", error.root_cause_message
    end

    it "should raise error when invalid MDX object" do
      error = assert_raises(Mondrian::OLAP::Error) {
        @query.columns('[Measures].[Dummy]').execute
      }
      assert_kind_of Mondrian::OLAP::Error, error
      assert_equal 'org.olap4j.OlapException: mondrian gave exception while parsing query', error.message
      assert_equal "MDX object '[Measures].[Dummy]' not found in cube 'Sales'", error.root_cause_message
    end

    it "should raise error when invalid formula" do
      error = assert_raises(Mondrian::OLAP::Error) {
        @query.with_member('[Measures].[Dummy]').as('Dummy(123)').
          columns('[Measures].[Dummy]').execute
      }
      assert_kind_of Mondrian::OLAP::Error, error
      assert_equal 'org.olap4j.OlapException: mondrian gave exception while parsing query', error.message
      assert_equal "No function matches signature 'Dummy(<Numeric Expression>)'", error.root_cause_message
    end

    it "should raise error when TokenMgrError is raised" do
      error = assert_raises(Mondrian::OLAP::Error) {
        @query.with_member('[Measures].[Dummy]').as('[Measures].[Store Sales]]').
          columns('[Measures].[Dummy]').execute
      }
      assert_kind_of Mondrian::OLAP::Error, error
      assert_match /mondrian\.parser\.TokenMgrError/, error.message
      assert_match /Lexical error/, error.root_cause_message
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
      # Ensure that column metadata are loaded before loading rows and closing result set, as tests are executed in random order.
      @drill_through.column_types
      @drill_through.column_names
      @drill_through.table_names
      @drill_through.column_labels
    end

    if MONDRIAN_DRIVER == 'mysql'
      it "should return column types" do
        assert_equal [
          :INT, :VARCHAR, :INT, :INT, :INT,
          :VARCHAR, :VARCHAR, :VARCHAR, :VARCHAR, :VARCHAR, :VARCHAR,
          :VARCHAR, :VARCHAR, :VARCHAR, :BIGINT,
          :VARCHAR,
          :DECIMAL
        ], @drill_through.column_types
      end
    end

    if %w(mysql postgresql).include? MONDRIAN_DRIVER
      it "should return column names" do
        # ignore calculated customer full name column name which is shown differently on each database
        assert_equal %w(
          the_year quarter month_of_year week_of_year day_of_month
          product_family product_department product_category product_subcategory brand_name product_name
          state_province city
        ), @drill_through.column_names[0..12]
        assert_equal %w(
          id gender unit_sales
        ), @drill_through.column_names[14..16]
      end

      it "should return table names" do
        # ignore calculated customer full name column name which is shown differently on each database
        assert_equal [
          "time", "time", "time", "time", "time",
          "product_classes", "product_classes", "product_classes", "product_classes", "products", "products",
          "customers", "customers"
        ], @drill_through.table_names[0..12]
        assert_equal [
          "customers", "customers", "sales"
        ], @drill_through.table_names[14..16]
      end
    end

    it "should return column labels" do
      assert_equal [
        "Year", "Quarter", "Month", "Week", "Day",
        "Product Family", "Product Department", "Product Category", "Product Subcategory", "Brand Name", "Product Name",
        "State Province", "City", "Name", "Name (Key)",
        "Gender",
        "Unit Sales"
      ], @drill_through.column_labels
    end

    it "should return row values" do
      assert_equal 15, @drill_through.rows.size # number of generated test rows
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
        assert_kind_of expected_value_types[i], value
      end
    end

    it "should return only specified max rows" do
      drill_through = @result.drill_through(:row => 0, :column => 0, :max_rows => 10)
      assert_equal 10, drill_through.rows.size
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
      assert_equal [
        "Month (Key)",
        "City (Key)",
        "Product Family (Key)",
        "Unit Sales", "Store Sales"
      ], @drill_through.column_labels
    end

    it "should return rows also for field dimension that is not present in the report query" do
      result = @olap.from('Sales').columns('[Measures].[Unit Sales]').rows('[Customers].[Canada].[BC].[Burnaby]').execute
      drill_through = result.drill_through(row: 0, column: 0, return: ["[Product].[Product Family]"])
      assert_equal @sql.select_rows(<<-SQL), drill_through.rows
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
      assert_equal [
        "Unit Sales", "Store Sales"
      ], @drill_through.column_labels
      assert_equal true, @drill_through.rows.all? { |r| r.any? { |c| c } }
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
      assert_equal [
        "Name", "Gender", "Description",
        "Non-existing property name"
      ], @drill_through.column_labels
      assert_equal @sql.select_rows(<<-SQL), @drill_through.rows
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
      assert_equal [ "Product Family (Key)", "Unit Sales", "Store Cost" ], @drill_through.column_labels
      assert_equal @sql.select_rows(<<-SQL
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
      ), @drill_through.rows
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
      assert_equal true, @drill_through.rows.all? { |r| r.first == "Mexico" }
    end

    it "should return only specified max rows" do
      assert_equal 10, @drill_through.rows.size
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

    it "should be virtual cube" do
      assert @olap.cube('Sales and Warehouse').virtual?
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
      assert_equal [
        "Month (Key)",
        "Product Family (Key)",
        "City (Key)",
        "Unit Sales",
        "Units Shipped",
        "Products with units shipped"
      ], @drill_through.column_labels
      # Validate that only City and Unit Sales values are missing
      assert_equal [
        [true, true, false, false, true, true]
      ], @drill_through.rows.map { |r| r.map(&:present?) }.uniq
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
      assert_equal [
        "Year", "Quarter", "Month", "Week", "Day",
        "Product Family", "Product Department", "Product Category", "Product Subcategory", "Brand Name", "Product Name",
        "State Province", "City", "Name", "Name (Key)",
        "Gender",
        "Unit Sales"
      ], @drill_through.column_labels
    end

    it "should return row values" do
      @drill_through = @query.execute_drill_through
      assert_equal 15, @drill_through.rows.size # number of generated test rows
    end

    it "should return only specified max rows" do
      drill_through = @query.execute_drill_through(:max_rows => 10)
      assert_equal 10, drill_through.rows.size
    end

    it "should return only specified fields" do
      @drill_through = @query.execute_drill_through(:return => [
        '[Time].[Month]',
        '[Product].[Product Family]',
        '[Customers].[City]',
        '[Measures].[Unit Sales]', '[Measures].[Store Sales]'
      ])
      assert_equal [
        "Month",
        "Product Family",
        "City",
        "Unit Sales", "Store Sales"
      ], @drill_through.column_labels
    end

  end

  describe "parse expression" do
    it "should parse expression" do
      assert_kind_of Java::MondrianOlap::Literal, @olap.parse_expression("1")
    end

    it "should raise error when invalid expression" do
      expression = "1, dummy"
      error = assert_raises(Mondrian::OLAP::Error) {
        @olap.parse_expression expression
      }
      assert_kind_of Mondrian::OLAP::Error, error
      assert_equal "mondrian.olap.MondrianException: Mondrian Error:Failed to parse query '#{expression}'", error.message
      assert_equal "Syntax error at line 1, column 2, token ','", error.root_cause_message
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
      assert_equal @first_unit_sales, @sql.select_value("SELECT unit_sales FROM sales WHERE #{@condition}").to_i
    end

    before do
      create_olap_connection
      @unit_sales = query_unit_sales_value

      update_first_unit_sales(@first_unit_sales + 1)

      # should still use previous value from cache
      create_olap_connection
      assert_equal @unit_sales, query_unit_sales_value
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
      assert_equal @unit_sales + 1, query_unit_sales_value
    end

    it "should remove schema by key" do
      Mondrian::OLAP::Connection.flush_schema(@olap2.schema_key)
      create_olap_connection
      assert_equal @unit_sales + 1, query_unit_sales_value
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
      assert_like <<~EOS, @result.profiling_plan
        Axis (COLUMNS):
        SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc,
                    type=SetType<MemberType<member=[Measures].[Unit Sales]>>, resultStyle=MUTABLE_LIST)
            2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$2,
                    type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE)
                Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc,
                    type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales])

        Axis (ROWS):
        Children(name=Children, class=class mondrian.olap.fun.BuiltinFunTable$22$1,
                    type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=LIST)
            CurrentMemberFixed(hierarchy=[Product], name=CurrentMemberFixed,
                    class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl,
                    type=MemberType<hierarchy=[Product]>, resultStyle=VALUE)
      EOS
    end

    it "should return SQL timing string" do
      assert_match %r{^SqlStatement-Segment.load invoked 1 times for total of \d+ms.  \(Avg. \d+ms/invocation\)$},
        @result.profiling_timing_string.strip
    end

    it "should return custom profiling string" do
      assert_match %r{^MDX query time invoked 1 times for total of 100ms.  \(Avg. 100ms/invocation\)$},
        @result.profiling_timing_string.strip
    end

    it "should return total duration" do
      assert_operator @result.total_duration, :>, 0
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
      assert_match /^Axis \(COLUMNS\):/, @error.profiling_plan
    end

    it "should return timing string" do
      assert_match %r{^FilterFunDef invoked 1 times for total of \d+ms.  \(Avg. \d+ms/invocation\)$},
        @error.profiling_timing_string
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
      error = assert_raises(Mondrian::OLAP::Error) do
        @olap.from('Sales').columns('[Measures].[Sleep 5]').execute(timeout: 0.1)
      end
      assert_kind_of Mondrian::OLAP::Error, error
      assert_equal 'org.olap4j.OlapException: Mondrian Error:Query timeout of 0 seconds reached', error.message
    end

    it "should not raise timeout error for short queries" do
      assert_equal [0], @olap.from('Sales').columns('[Measures].[Sleep 0]').execute(timeout: 1).values
    end
  end

end
