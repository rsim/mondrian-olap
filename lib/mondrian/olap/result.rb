require 'bigdecimal'

module Mondrian
  module OLAP
    class Result
      def initialize(connection, raw_cell_set)
        @connection = connection
        @raw_cell_set = raw_cell_set
      end

      def axes_count
        axes.length
      end

      def axis_names
        @axis_names ||= axis_positions(:getName)
      end

      def axis_full_names
        @axis_full_names ||= axis_positions(:getUniqueName)
      end

      def axis_members
        @axis_members ||= axis_positions(:to_member)
      end

      AXIS_SYMBOLS = [:column, :row, :page, :section, :chapter]
      AXIS_SYMBOLS.each_with_index do |axis, i|
        define_method :"#{axis}_names" do
          axis_names[i]
        end

        define_method :"#{axis}_full_names" do
          axis_full_names[i]
        end

        define_method :"#{axis}_members" do
          axis_members[i]
        end
      end

      def values(*axes_sequence)
        values_using(:getValue, axes_sequence)
      end

      def formatted_values(*axes_sequence)
        values_using(:getFormattedValue, axes_sequence)
      end

      def values_using(values_method, axes_sequence = [])
        if axes_sequence.empty?
          axes_sequence = (0...axes_count).to_a.reverse
        elsif axes_sequence.size != axes_count
          raise ArgumentError, "axes sequence size is not equal to result axes count"
        end
        recursive_values(values_method, axes_sequence, 0)
      end

      # format results in simple HTML table
      def to_html(options = {})
        case axes_count
        when 1
          builder = Nokogiri::XML::Builder.new(:encoding => 'UTF-8') do |doc|
            doc.table do
              doc.tr do
                column_full_names.each do |column_full_name|
                  column_full_name = column_full_name.join(',') if column_full_name.is_a?(Array)
                  doc.th column_full_name, :align => 'right'
                end
              end
              doc.tr do
                (options[:formatted] ? formatted_values : values).each do |value|
                  doc.td value, :align => 'right'
                end
              end
            end
          end
          builder.doc.to_html
        when 2
          builder = Nokogiri::XML::Builder.new(:encoding => 'UTF-8') do |doc|
            doc.table do
              doc.tr do
                doc.th
                column_full_names.each do |column_full_name|
                  column_full_name = column_full_name.join(',') if column_full_name.is_a?(Array)
                  doc.th column_full_name, :align => 'right'
                end
              end
              (options[:formatted] ? formatted_values : values).each_with_index do |row, i|
                doc.tr do
                  row_full_name = row_full_names[i].is_a?(Array) ? row_full_names[i].join(',') : row_full_names[i]
                  doc.th row_full_name, :align => 'left'
                  row.each do |cell|
                    doc.td cell, :align => 'right'
                  end
                end
              end
            end
          end
          builder.doc.to_html
        else
          raise ArgumentError, "just columns and rows axes are supported"
        end
      end

      # Specify drill through cell position, for example, as
      #   :row => 0, :cell => 1
      # Specify max returned rows with :max_rows parameter
      # Specify returned fields (as list of MDX levels and measures) with :return parameter
      # Specify measures which at least one should not be empty (NULL) with :nonempty parameter
      def drill_through(params = {})
        Error.wrap_native_exception do
          cell_params = []
          axes_count.times do |i|
            axis_symbol = AXIS_SYMBOLS[i]
            raise ArgumentError, "missing position #{axis_symbol.inspect}" unless axis_position = params[axis_symbol]
            cell_params << Java::JavaLang::Integer.new(axis_position)
          end
          raw_cell = @raw_cell_set.getCell(cell_params)
          DrillThrough.from_raw_cell(raw_cell, params)
        end
      end

      class DrillThrough
        def self.from_raw_cell(raw_cell, params = {})
          max_rows = params[:max_rows] || -1
          # workaround to avoid calling raw_cell.drillThroughInternal private method
          # which fails when running inside TorqueBox
          cell_field = raw_cell.java_class.declared_field('cell')
          cell_field.accessible = true
          rolap_cell = cell_field.value(raw_cell)

          if params[:return] || rolap_cell.canDrillThrough
            sql_statement = drill_through_internal(rolap_cell, params)
            raw_result_set = sql_statement.getWrappedResultSet
            new(raw_result_set)
          end
        end

        def initialize(raw_result_set)
          @raw_result_set = raw_result_set
        end

        def column_types
          @column_types ||= (1..metadata.getColumnCount).map{|i| metadata.getColumnTypeName(i).to_sym}
        end

        def column_names
          @column_names ||= begin
            # if PostgreSQL then use getBaseColumnName as getColumnName returns empty string
            if metadata.respond_to?(:getBaseColumnName)
              (1..metadata.getColumnCount).map{|i| metadata.getBaseColumnName(i)}
            else
              (1..metadata.getColumnCount).map{|i| metadata.getColumnName(i)}
            end
          end
        end

        def table_names
          @table_names ||= begin
            # if PostgreSQL then use getBaseTableName as getTableName returns empty string
            if metadata.respond_to?(:getBaseTableName)
              (1..metadata.getColumnCount).map{|i| metadata.getBaseTableName(i)}
            else
              (1..metadata.getColumnCount).map{|i| metadata.getTableName(i)}
            end
          end
        end

        def column_labels
          @column_labels ||= (1..metadata.getColumnCount).map{|i| metadata.getColumnLabel(i)}
        end

        def fetch
          if @raw_result_set.next
            row_values = []
            column_types.each_with_index do |column_type, i|
              row_values << Result.java_to_ruby_value(@raw_result_set.getObject(i+1), column_type)
            end
            row_values
          else
            @raw_result_set.close
            nil
          end
        end

        def rows
          @rows ||= begin
            rows_values = []
            while row_values = fetch
              rows_values << row_values
            end
            rows_values
          end
        end

        private

        def metadata
          @metadata ||= @raw_result_set.getMetaData
        end

        # modified RolapCell drillThroughInternal method
        def self.drill_through_internal(rolap_cell, params)
          max_rows = params[:max_rows] || -1

          result_field = rolap_cell.java_class.declared_field('result')
          result_field.accessible = true
          result = result_field.value(rolap_cell)

          sql = generate_drill_through_sql(rolap_cell, result, params)

          # Choose the appropriate scrollability. If we need to start from an
          # offset row, it is useful that the cursor is scrollable, but not
          # essential.
          statement = result.getExecution.getMondrianStatement
          execution = Java::MondrianServer::Execution.new(statement, 0)
          connection = statement.getMondrianConnection
          result_set_type = Java::JavaSql::ResultSet::TYPE_FORWARD_ONLY
          result_set_concurrency = Java::JavaSql::ResultSet::CONCUR_READ_ONLY
          schema = statement.getSchema
          dialect = schema.getDialect

          Java::MondrianRolap::RolapUtil.executeQuery(
            connection.getDataSource,
            sql,
            nil,
            max_rows,
            -1, # firstRowOrdinal
            Java::MondrianRolap::SqlStatement::StatementLocus.new(
              execution,
              "RolapCell.drillThrough",
              "Error in drill through",
              Java::MondrianServerMonitor::SqlStatementEvent::Purpose::DRILL_THROUGH, 0
            ),
            result_set_type,
            result_set_concurrency,
            nil
          )
        end

        def self.generate_drill_through_sql(rolap_cell, result, params)
          return_field_names, return_expressions, nonempty_columns = parse_return_fields(result, params)

          sql_non_extended = rolap_cell.getDrillThroughSQL(return_expressions, false)
          sql_extended = rolap_cell.getDrillThroughSQL(return_expressions, true)

          if sql_non_extended =~ /\Aselect (.*) from (.*) where (.*) order by (.*)\Z/
            non_extended_from = $2
            non_extended_where = $3
          # if drill through total measure with just all members selection
          elsif sql_non_extended =~ /\Aselect (.*) from (.*)\Z/
            non_extended_from = $2
            non_extended_where = "1 = 1" # dummy true condition
          else
            raise ArgumentError, "cannot parse drill through SQL: #{sql_non_extended}"
          end

          if sql_extended =~ /\Aselect (.*) from (.*) where (.*) order by (.*)\Z/
            extended_select = $1
            extended_from = $2
            extended_where = $3
            extended_order_by = $4
          # if only measures are selected then there will be no order by
          elsif sql_extended =~ /\Aselect (.*) from (.*) where (.*)\Z/
            extended_select = $1
            extended_from = $2
            extended_where = $3
            extended_order_by = ''
          else
            raise ArgumentError, "cannot parse drill through SQL: #{sql_extended}"
          end

          return_column_positions = {}

          if return_field_names && !return_field_names.empty?
            new_select = extended_select.split(/,\s*/).map do |part|
              column_name, column_alias = part.split(' as ')
              field_name = column_alias[1..-2].gsub(' (Key)', '')
              position = return_field_names.index(field_name) || 9999
              return_column_positions[column_name] = position
              [part, position]
            end.sort_by(&:last).map(&:first).join(', ')

            new_order_by = extended_order_by.split(/,\s*/).map do |part|
              column_name, asc_desc = part.split(/\s+/)
              position = return_column_positions[column_name] || 9999
              [part, position]
            end.sort_by(&:last).map(&:first).join(', ')
          else
            new_select = extended_select
            new_order_by = extended_order_by
          end

          new_from_parts = non_extended_from.split(/,\s*/)
          outer_join_from_parts = extended_from.split(/,\s*/) - new_from_parts
          where_parts = extended_where.split(' and ')

          # reverse outer_join_from_parts to support dimensions with several table joins
          # where join with detailed level table should be constructed first
          outer_join_from_parts.reverse.each do |part|
            part_elements = part.split(/\s+/)
            # first is original table, then optional 'as' and the last is alias
            table_name = part_elements.first
            table_alias = part_elements.last
            join_conditions = where_parts.select do |where_part|
              where_part.include?(" = #{table_alias}.")
            end
            outer_join = " left outer join #{part} on (#{join_conditions.join(' and ')})"
            left_table_alias = join_conditions.first.split('.').first

            if left_table_from_part = new_from_parts.detect{|from_part| from_part.include?(left_table_alias)}
              left_table_from_part << outer_join
            else
              raise ArgumentError, "cannot extract outer join left table #{left_table_alias} in drill through SQL: #{sql_extended}"
            end
          end

          new_from = new_from_parts.join(', ')

          new_where = non_extended_where
          if nonempty_columns && !nonempty_columns.empty?
            not_null_condition = nonempty_columns.map{|c| "(#{c}) IS NOT NULL"}.join(' OR ')
            new_where += " AND (#{not_null_condition})"
          end

          sql = "select #{new_select} from #{new_from} where #{new_where}"
          sql << " order by #{new_order_by}" unless new_order_by.empty?
          sql
        end

        def self.parse_return_fields(result, params)
          return_field_names = []
          return_expressions = nil
          nonempty_columns = []

          if params[:return] || params[:nonempty]
            rolap_cube = result.getCube
            schema_reader = rolap_cube.getSchemaReader

            if return_fields = params[:return]
              return_fields = return_fields.split(/,\s*/) if return_fields.is_a?(String)
              return_expressions = return_fields.map do |return_field|
                begin
                  segment_list = Java::MondrianOlap::Util.parseIdentifier(return_field)
                  return_field_names << segment_list.to_a.last.name
                rescue Java::JavaLang::IllegalArgumentException
                  raise ArgumentError, "invalid return field #{return_field}"
                end

                level_or_member = schema_reader.lookupCompound rolap_cube, segment_list, false, 0

                case level_or_member
                when Java::MondrianOlap::Level
                  Java::MondrianMdx::LevelExpr.new level_or_member
                when Java::MondrianOlap::Member
                  raise ArgumentError, "cannot use calculated member #{return_field} as return field" if level_or_member.isCalculated
                  Java::mondrian.mdx.MemberExpr.new level_or_member
                else
                  raise ArgumentError, "return field #{return_field} should be level or measure"
                end
              end
            end

            if nonempty_fields = params[:nonempty]
              nonempty_fields = nonempty_fields.split(/,\s*/) if nonempty_fields.is_a?(String)
              nonempty_columns = nonempty_fields.map do |nonempty_field|
                begin
                  segment_list = Java::MondrianOlap::Util.parseIdentifier(nonempty_field)
                rescue Java::JavaLang::IllegalArgumentException
                  raise ArgumentError, "invalid return field #{return_field}"
                end
                member = schema_reader.lookupCompound rolap_cube, segment_list, false, 0
                if member.is_a? Java::MondrianOlap::Member
                  raise ArgumentError, "cannot use calculated member #{return_field} as nonempty field" if member.isCalculated
                  sql_query = member.getStarMeasure.getSqlQuery
                  member.getStarMeasure.generateExprString(sql_query)
                else
                  raise ArgumentError, "nonempty field #{return_field} should be measure"
                end
              end
            end
          end

          [return_field_names, return_expressions, nonempty_columns]
        end

      end

      def self.java_to_ruby_value(value, column_type = nil)
        case value
        when Numeric, String
          value
        when Java::JavaMath::BigDecimal
          BigDecimal(value.to_s)
        else
          value
        end
      end

      private

      def axes
        @axes ||= @raw_cell_set.getAxes
      end

      def axis_positions(map_method, join_with=false)
        axes.map do |axis|
          axis.getPositions.map do |position|
            names = position.getMembers.map do |member|
              if map_method == :to_member
                Member.new(member)
              else
                member.send(map_method)
              end
            end
            if names.size == 1
              names[0]
            elsif join_with
              names.join(join_with)
            else
              names
            end
          end
        end
      end

      AXIS_SYMBOL_TO_NUMBER = {
        :columns => 0,
        :rows => 1,
        :pages => 2,
        :sections => 3,
        :chapters => 4
      }.freeze

      def recursive_values(value_method, axes_sequence, current_index, cell_params=[])
        if axis_number = axes_sequence[current_index]
          axis_number = AXIS_SYMBOL_TO_NUMBER[axis_number] if axis_number.is_a?(Symbol)
          positions_size = axes[axis_number].getPositions.size
          (0...positions_size).map do |i|
            cell_params[axis_number] = Java::JavaLang::Integer.new(i)
            recursive_values(value_method, axes_sequence, current_index + 1, cell_params)
          end
        else
          self.class.java_to_ruby_value(@raw_cell_set.getCell(cell_params).send(value_method))
        end
      end

    end
  end
end
