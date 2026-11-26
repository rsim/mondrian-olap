# frozen_string_literal: true

module Mondrian
  module OLAP
    class Result
      class DrillThrough
        def self.from_raw_cell(raw_cell, params = {})
          # Workaround to avoid calling raw_cell.drillThroughInternal private method
          cell_field = raw_cell.java_class.declared_field('cell')
          cell_field.accessible = true
          rolap_cell = cell_field.value(raw_cell)

          if params[:return] || rolap_cell.canDrillThrough
            max_rows = params[:max_rows]
            if role_name = params[:role_name]
              # Remove max rows limitation for the drill through SQL statement when the data access roles are used.
              # As the data restrictions validation later may reduce returned rows count.
              max_rows = nil
            end
            sql_statement, return_fields = drill_through_internal(rolap_cell, params.merge(max_rows: max_rows))
            raw_result_set = sql_statement.getWrappedResultSet
            raw_cube = raw_cell.getCellSet.getMetaData.getCube
            new(raw_result_set,
              return_fields: return_fields,
              raw_cube: raw_cube,
              role_name: role_name,
              max_rows: params[:max_rows]
            )
          end
        end

        def initialize(raw_result_set, options = {})
          @raw_result_set = raw_result_set
          @return_fields = options[:return_fields]
          @raw_cube = options[:raw_cube]
          @role_name = options[:role_name]
          @max_rows = options[:max_rows]
        end

        def column_types
          @column_types ||= (1..metadata.getColumnCount).map { |i| metadata.getColumnTypeName(i).to_sym }
        end

        def column_names
          @column_names ||= begin
            # If PostgreSQL then use getBaseColumnName as getColumnName returns empty string
            if metadata.respond_to?(:getBaseColumnName)
              (1..metadata.getColumnCount).map { |i| metadata.getBaseColumnName(i) }
            else
              (1..metadata.getColumnCount).map { |i| metadata.getColumnName(i) }
            end
          end
        end

        def table_names
          @table_names ||= begin
            # If PostgreSQL then use getBaseTableName as getTableName returns empty string
            if metadata.respond_to?(:getBaseTableName)
              (1..metadata.getColumnCount).map { |i| metadata.getBaseTableName(i) }
            else
              (1..metadata.getColumnCount).map { |i| metadata.getTableName(i) }
            end
          end
        end

        def column_labels
          @column_labels ||= (1..metadata.getColumnCount).map { |i| metadata.getColumnLabel(i) }
        end

        def fetch
          types = column_types
          # Use loop instead of recursion to avoid deep recursion when many rows are skipped by role restrictions.
          while @raw_result_set.next
            row_values = Array.new(types.size)
            types.each_with_index do |column_type, i|
              row_values[i] = Result.java_to_ruby_value(@raw_result_set.getObject(i + 1), column_type)
            end
            return row_values if can_access_row_values?(row_values)
          end
          @raw_result_set.close
          nil
        end

        def rows
          @rows ||= begin
            rows_values = []
            while row_values = fetch
              rows_values << row_values
              break if rows_values.size == @max_rows
            end
            rows_values
          ensure
            # Close the result set also when fetching was stopped by max_rows or by an exception
            # (closing an already closed JDBC result set is a no-op).
            @raw_result_set.close
          end
        end

        private

        def can_access_row_values?(row_values)
          return true unless @role_name

          member_full_name_columns_indexes.each do |column_indexes|
            segment_names = [@return_fields[column_indexes.first][:member].getHierarchy.getName]
            column_indexes.each { |i| segment_names << row_values[i].to_s }
            segment_list = Java::OrgOlap4jMdx::IdentifierNode.ofNames(*segment_names).getSegmentList
            return false unless @raw_cube.lookupMember(segment_list)
          end

          true
        end

        def member_full_name_columns_indexes
          @member_full_name_columns_indexes ||= begin
            fieldset_columns = Hash.new { |h, k| h[k] = Array.new }
            column_labels.each_with_index do |label, i|
              # Find all role restriction columns with a label pattern "_level:<Fieldset ID>:<Level depth>"
              if label =~ /\A_level:(\d+):(\d+)\z/
                # Group by fieldset ID with a compound value of level depth and column index
                fieldset_columns[$1] << [$2.to_i, i]
              end
            end
            # For each fieldset create an array with columns indexes sorted by level depth
            fieldset_columns.each { |k, v| fieldset_columns[k] = v.sort_by(&:first).map(&:last) }
            fieldset_columns.values
          end
        end

        def metadata
          @metadata ||= @raw_result_set.getMetaData
        end

        # Modified RolapCell drillThroughInternal method
        def self.drill_through_internal(rolap_cell, params)
          max_rows = params[:max_rows] || -1

          result_field = rolap_cell.java_class.declared_field('result')
          result_field.accessible = true
          result = result_field.value(rolap_cell)

          sql, return_fields = generate_drill_through_sql(rolap_cell, result, params)

          # Choose the appropriate scrollability. If we need to start from an
          # Offset row, it is useful that the cursor is scrollable, but not essential.
          statement = result.getExecution.getMondrianStatement
          execution = Java::MondrianServer::Execution.new(statement, 0)
          connection = statement.getMondrianConnection
          result_set_type = Java::JavaSql::ResultSet::TYPE_FORWARD_ONLY
          result_set_concurrency = Java::JavaSql::ResultSet::CONCUR_READ_ONLY

          sql_statement = Java::MondrianRolap::RolapUtil.executeQuery(
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
          [sql_statement, return_fields]
        end

        def self.generate_drill_through_sql(rolap_cell, result, params)
          nonempty_columns, return_fields = parse_return_fields(result, params)
          return_expressions = return_fields.map { |field| field[:member] }

          sql_non_extended = rolap_cell.getDrillThroughSQL(return_expressions, false)
          sql_extended = rolap_cell.getDrillThroughSQL(return_expressions, true)

          if sql_non_extended =~ /\Aselect (.*) from (.*) where (.*) order by (.*)\Z/m
            non_extended_from = $2
            non_extended_where = $3
          # The latest Mondrian version sometimes returns sql_non_extended without order by
          elsif sql_non_extended =~ /\Aselect (.*) from (.*) where (.*)\Z/m
            non_extended_from = $2
            non_extended_where = $3
          # If drill through total measure with just all members selection
          elsif sql_non_extended =~ /\Aselect (.*) from (.*)\Z/m
            non_extended_from = $2
            non_extended_where = "1 = 1" # dummy true condition
          else
            raise ArgumentError, "cannot parse drill through SQL: #{sql_non_extended}"
          end

          if sql_extended =~ /\Aselect (.*) from (.*) where (.*) order by (.*)\Z/m
            extended_select = $1
            extended_from = $2
            extended_where = $3
            extended_order_by = $4
          # If only measures are selected then there will be no order by
          elsif sql_extended =~ /\Aselect (.*) from (.*) where (.*)\Z/m
            extended_select = $1
            extended_from = $2
            extended_where = $3
            extended_order_by = +''
          else
            raise ArgumentError, "cannot parse drill through SQL: #{sql_extended}"
          end

          if return_fields.present?
            new_select_columns = []
            new_order_by_columns = []
            new_group_by_columns = []
            group_by = params[:group_by]

            return_fields.size.times do |i|
              column_alias = return_fields[i][:column_alias]
              column_expression = return_fields[i][:column_expression]
              quoted_table_name = return_fields[i][:quoted_table_name]
              new_select_columns <<
                if column_expression && (!quoted_table_name || extended_from.include?(quoted_table_name))
                  new_order_by_columns << column_expression unless return_fields[i][:name].start_with?('_level:')
                  new_group_by_columns << column_expression if group_by && return_fields[i][:type] != :measure
                  "#{column_expression} AS #{column_alias}"
                else
                  "'' AS #{column_alias}"
                end
            end

            new_select = new_select_columns.join(', ')
            new_order_by = new_order_by_columns.join(', ')
            new_group_by = new_group_by_columns.join(', ')
          else
            new_select = extended_select
            new_order_by = extended_order_by
            new_group_by = +''
          end

          new_from_parts = non_extended_from.split(/,\s*/)
          outer_join_from_parts = extended_from.split(/,\s*/) - new_from_parts
          where_parts = extended_where.split(' and ')

          outer_join_from_parts.each do |part|
            part_elements = part.split(/\s+/)
            # First is original table, then optional 'as' and the last is alias
            table_alias = part_elements.last
            join_conditions = where_parts.select do |where_part|
              where_part.include?(" = #{table_alias}.")
            end
            outer_join = " left outer join #{part} on (#{join_conditions.join(' and ')})"
            left_table_alias = join_conditions.first.split('.').first

            if left_table_from_part = new_from_parts.detect { |from_part| from_part.include?(left_table_alias) }
              left_table_from_part << outer_join
            else
              raise ArgumentError,
                "cannot extract outer join left table #{left_table_alias} in drill through SQL: #{sql_extended}"
            end
          end

          new_from = new_from_parts.join(', ')

          new_where = non_extended_where
          if nonempty_columns && !nonempty_columns.empty?
            not_null_condition = nonempty_columns.map { |c| "(#{c}) IS NOT NULL" }.join(' OR ')
            new_where += " AND (#{not_null_condition})"
          end

          sql = "select #{new_select} from #{new_from} where #{new_where}"
          sql << " group by #{new_group_by}" unless new_group_by.empty?
          sql << " order by #{new_order_by}" unless new_order_by.empty?
          [sql, return_fields]
        end

        def self.parse_return_fields(result, params)
          nonempty_columns = []
          return_fields = []

          if params[:return] || params[:nonempty]
            rolap_cube = result.getCube
            schema_reader = rolap_cube.getSchemaReader
            dialect = result.getCube.getSchema.getDialect
            sql_query = Java::mondrian.rolap.sql.SqlQuery.new(dialect)

            if fields = params[:return]
              fields = fields.split(/,\s*/) if fields.is_a? String
              fields.each do |field|
                return_fields <<
                  case field
                  when /\AName\((.*)\)\z/i
                    {member_full_name: $1, type: :name}
                  when /\AProperty\((.*)\s*,\s*'(.*)'\)\z/i
                    {member_full_name: $1, type: :property, name: $2}
                  else
                    {member_full_name: field}
                  end
              end

              # Old versions of Oracle had a limit of 30 character identifiers.
              # Do not limit it for other databases (as e.g. in MySQL aliases can be longer than column names)
              max_alias_length = dialect.getMaxColumnNameLength # 0 means that there is no limit
              max_alias_length = nil if max_alias_length && (max_alias_length > 30 || max_alias_length == 0)
              sql_options = {
                dialect: dialect,
                sql_query: sql_query,
                max_alias_length: max_alias_length,
                params: params
              }

              return_fields.size.times do |i|
                member_full_name = return_fields[i][:member_full_name]
                begin
                  segment_list = Java::MondrianOlap::Util.parseIdentifier(member_full_name)
                rescue Java::JavaLang::IllegalArgumentException
                  raise ArgumentError, "invalid return field #{member_full_name}"
                end

                # If this is property field then the name is initialized already
                return_fields[i][:name] ||= segment_list.to_a.last.name
                level_or_member = schema_reader.lookupCompound rolap_cube, segment_list, false, 0
                return_fields[i][:member] = level_or_member

                if level_or_member.is_a? Java::MondrianOlap::Member
                  raise ArgumentError,
                    "cannot use calculated member #{member_full_name} as return field" if level_or_member.isCalculated
                elsif !level_or_member.is_a? Java::MondrianOlap::Level
                  raise ArgumentError, "return field #{member_full_name} should be level or measure"
                end

                add_sql_attributes return_fields[i], sql_options
              end
            end

            if nonempty_fields = params[:nonempty]
              nonempty_fields = nonempty_fields.split(/,\s*/) if nonempty_fields.is_a?(String)
              nonempty_columns = nonempty_fields.map do |nonempty_field|
                begin
                  segment_list = Java::MondrianOlap::Util.parseIdentifier(nonempty_field)
                rescue Java::JavaLang::IllegalArgumentException
                  raise ArgumentError, "invalid return field #{nonempty_field}"
                end
                member = schema_reader.lookupCompound rolap_cube, segment_list, false, 0
                if member.is_a? Java::MondrianOlap::Member
                  raise ArgumentError, "cannot use calculated member #{nonempty_field} as nonempty field" if member.isCalculated

                  sql_query = member.getStarMeasure.getSqlQuery
                  member.getStarMeasure.generateExprString(sql_query)
                else
                  raise ArgumentError, "nonempty field #{nonempty_field} should be measure"
                end
              end
            end
          end

          if params[:role_name].present?
            add_role_restriction_fields return_fields, sql_options
          end

          [nonempty_columns, return_fields]
        end

        def self.add_sql_attributes(field, options = {})
          member = field[:member]
          dialect = options[:dialect]
          sql_query = options[:sql_query]
          max_alias_length = options[:max_alias_length]
          params = options[:params]

          if table_name = (member.respond_to?(:getTableName) && member.getTableName ||
              member.respond_to?(:getMondrianDefExpression) && (expr = member.getMondrianDefExpression) &&
              expr.respond_to?(:table) && expr.table)
            field[:quoted_table_name] = dialect.quoteIdentifier(table_name)
          end

          field[:column_expression] =
            case field[:type]
            when :name
              if member.respond_to? :getNameExp
                member.getNameExp.getExpression sql_query
              end
            when :property
              if property = member.getProperties.to_a.detect { |p| p.getName == field[:name] }
                # Property.getExp is a protected method therefore
                # use a workaround to get the value from the field
                f = property.java_class.declared_field("exp")
                f.accessible = true
                if column = f.value(property)
                  column.getExpression sql_query
                end
              end
            when :name_or_key
              member.getNameExp&.getExpression(sql_query) || member.getKeyExp&.getExpression(sql_query)
            else
              if member.respond_to? :getKeyExp
                field[:type] = :key
                member.getKeyExp.getExpression sql_query
              else
                field[:type] = :measure
                column_expression = member.getMondrianDefExpression.getExpression sql_query
                if params[:group_by]
                  member.getAggregator.getExpression column_expression
                else
                  column_expression
                end
              end
            end

          column_alias = field[:type] == :key ? "#{field[:name]} (Key)" : field[:name]
          field[:column_alias] = dialect.quoteIdentifier(max_alias_length ? column_alias[0, max_alias_length] : column_alias)
        end

        def self.add_role_restriction_fields(fields, options = {})
          # For each unique level field add a set of fields to be able to build level member full name from database query results
          fields.map { |f| f[:member] }.uniq.each_with_index do |level_or_member, i|
            next if level_or_member.is_a?(Java::MondrianOlap::Member)

            current_level = level_or_member
            loop do
              # Create an additional field name using a pattern "_level:<Fieldset ID>:<Level depth>"
              fields << {member: current_level, type: :name_or_key, name: "_level:#{i}:#{current_level.getDepth}"}
              add_sql_attributes fields.last, options
              break unless (current_level = current_level.getParentLevel) && !current_level.isAll
            end
          end
        end
      end
    end
  end
end
