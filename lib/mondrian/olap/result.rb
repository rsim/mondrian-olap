# frozen_string_literal: true

require 'bigdecimal'

module Mondrian
  module OLAP
    class Result
      def initialize(connection, raw_cell_set, options = {})
        @connection = connection
        @raw_cell_set = raw_cell_set
        @profiling_handler = options[:profiling_handler]
        @total_duration = options[:total_duration]
      end

      attr_reader :raw_cell_set, :profiling_handler, :total_duration

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
        # Convert a string method name to a symbol as later it is used as a block for mapping cell values.
        values_method = values_method.to_sym
        if axes_sequence.empty?
          axes_sequence = (0...axes_count).to_a.reverse
        elsif axes_sequence.size != axes_count
          raise ArgumentError, "axes sequence size is not equal to result axes count"
        end
        axes_numbers_sequence = axes_sequence.map do |axis_number|
          if axis_number.is_a?(Symbol)
            AXIS_SYMBOL_TO_NUMBER.fetch(axis_number) do
              raise ArgumentError, "invalid axis name #{axis_number.inspect}, " \
                "valid axis names are #{AXIS_SYMBOL_TO_NUMBER.keys.map(&:inspect).join(', ')}"
            end
          else
            axis_number
          end
        end
        recursive_values(values_method, axes_numbers_sequence, 0)
      end

      # Format results in simple HTML table
      def to_html(options = {})
        case axes_count
        when 1
          builder = Nokogiri::XML::Builder.new(encoding: 'UTF-8') do |doc|
            doc.table do
              doc.tr do
                column_full_names.each do |column_full_name|
                  column_full_name = column_full_name.join(',') if column_full_name.is_a?(Array)
                  doc.th column_full_name, align: 'right'
                end
              end
              doc.tr do
                (options[:formatted] ? formatted_values : values).each do |value|
                  doc.td value, align: 'right'
                end
              end
            end
          end
          builder.doc.to_html
        when 2
          builder = Nokogiri::XML::Builder.new(encoding: 'UTF-8') do |doc|
            doc.table do
              doc.tr do
                doc.th
                column_full_names.each do |column_full_name|
                  column_full_name = column_full_name.join(',') if column_full_name.is_a?(Array)
                  doc.th column_full_name, align: 'right'
                end
              end
              (options[:formatted] ? formatted_values : values).each_with_index do |row, i|
                doc.tr do
                  row_full_name = row_full_names[i].is_a?(Array) ? row_full_names[i].join(',') : row_full_names[i]
                  doc.th row_full_name, align: 'left'
                  row.each do |cell|
                    doc.td cell, align: 'right'
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

      def profiling_plan
        if profiling_handler
          @raw_cell_set.close
          if plan = profiling_handler.plan
            plan.gsub("\r\n", "\n")
          end
        end
      end

      def profiling_timing
        if profiling_handler
          @raw_cell_set.close
          profiling_handler.timing
        end
      end

      def profiling_mark_full(name, duration)
        profiling_timing && profiling_timing.markFull(name, duration)
      end

      QUERY_TIMING_CUMULATIVE_REGEXP = /\AQuery Timing \(Cumulative\):\n/

      def profiling_timing_string
        if profiling_timing && (timing_string = profiling_timing.toString)
          timing_string.gsub("\r\n", "\n").sub(QUERY_TIMING_CUMULATIVE_REGEXP, '')
        end
      end

      # Specify drill through cell position, for example, as
      #   row: 0, cell: 1
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
          DrillThrough.from_raw_cell(raw_cell, params.merge(role: @connection.raw_mondrian_connection.getRole))
        end
      end

      class DrillThrough
        def self.from_raw_cell(raw_cell, params = {})
          # Workaround to avoid calling raw_cell.drillThroughInternal private method
          cell_field = raw_cell.java_class.declared_field('cell')
          cell_field.accessible = true
          rolap_cell = cell_field.value(raw_cell)

          if params[:return] || rolap_cell.canDrillThrough
            result = rolap_result(rolap_cell)
            sql = generate_drill_through_sql(rolap_cell, result, params)
            sql_statement = execute_drill_through_sql(result, sql, params[:max_rows])
            new(sql_statement.getWrappedResultSet, max_rows: params[:max_rows])
          end
        end

        def self.rolap_result(rolap_cell)
          result_field = rolap_cell.java_class.declared_field('result')
          result_field.accessible = true
          result_field.value(rolap_cell)
        end

        def initialize(raw_result_set, options = {})
          @raw_result_set = raw_result_set
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
          if @raw_result_set.next
            row_values = Array.new(types.size)
            types.each_with_index do |column_type, i|
              row_values[i] = Result.java_to_ruby_value(@raw_result_set.getObject(i + 1), column_type)
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

        def metadata
          @metadata ||= @raw_result_set.getMetaData
        end

        # Modified RolapCell drillThroughInternal method
        def self.execute_drill_through_sql(result, sql, max_rows)
          # Choose the appropriate scrollability. If we need to start from an
          # Offset row, it is useful that the cursor is scrollable, but not essential.
          statement = result.getExecution.getMondrianStatement
          execution = Java::MondrianServer::Execution.new(statement, 0)
          connection = statement.getMondrianConnection
          result_set_type = Java::JavaSql::ResultSet::TYPE_FORWARD_ONLY
          result_set_concurrency = Java::JavaSql::ResultSet::CONCUR_READ_ONLY

          Java::MondrianRolap::RolapUtil.executeQuery(
            connection.getDataSource,
            sql,
            nil,
            max_rows || -1,
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
          params = params.merge(role: nil) unless role_restricts_cube?(params[:role], result.getCube)
          # An empty return string also selects the default fields.
          if (role = params[:role]) && Array(params[:return]).all?(&:empty?)
            params = params.merge(return: accessible_return_fields(rolap_cell, role))
          end
          nonempty_columns, return_fields, role_conditions = parse_return_fields(result, params)
          # Mondrian joins the tables of the return expressions, so the levels of the role conditions
          # are passed too. The select list is built from the return fields below.
          return_expressions = return_fields.map { |field| field[:member] } +
            role_conditions.flat_map { |condition| condition[:levels] }

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

          unless return_fields.empty?
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
                  new_order_by_columns << column_expression
                  new_group_by_columns << column_expression if group_by && return_fields[i][:type] != :measure
                  "#{column_expression} AS #{column_alias}"
                else
                  "'' AS #{column_alias}"
                end
            end

            new_select = new_select_columns.join(', ')
            # Fields of different hierarchies may share a column (e.g. the year of two time hierarchies),
            # and SQL Server rejects a column listed twice in ORDER BY.
            new_order_by = new_order_by_columns.uniq.join(', ')
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
          role_conditions.each do |condition|
            # A hierarchy of another cube of a virtual cube has no table in the query, so its rows are not limited.
            next unless condition[:quoted_table_names].all? { |table_name| extended_from.include?(table_name) }

            new_where += " AND (#{condition[:sql]})"
          end

          sql = "select #{new_select} from #{new_from} where #{new_where}"
          sql << " group by #{new_group_by}" unless new_group_by.empty?
          sql << " order by #{new_order_by}" unless new_order_by.empty?
          sql
        end

        NONE_ACCESS = Java::MondrianOlap::Access::NONE
        ALL_ACCESS = Java::MondrianOlap::Access::ALL

        # The role is checked by its grants and not by the olap4j role name, because a role from the
        # connection string or from Connection custom_role= has no role name.
        def self.role_restricts_cube?(role, cube)
          !role.nil? && cube.getHierarchies.any? { |hierarchy| role.getAccess(hierarchy) != ALL_ACCESS }
        end

        # The fields Mondrian selects without a return clause (every level of every cube hierarchy
        # and the cell measure), limited to the levels and measures the role grants. Mondrian adds
        # these columns without consulting the role, so a denied hierarchy would be returned.
        def self.accessible_return_fields(rolap_cell, role)
          members_method = rolap_cell.java_class.declared_method('getMembersForDrillThrough')
          members_method.accessible = true
          measure, *members = members_method.invoke(rolap_cell).to_a

          fields = members.flat_map do |member|
            hierarchy = member.getHierarchy
            next [] if closure_hierarchy?(hierarchy) || role.getAccess(hierarchy) == NONE_ACCESS

            hierarchy.getLevels.to_a.flat_map do |level|
              next [] if level.isAll || !level_accessible?(level, role)

              level_fields = []
              level_fields << "Name(#{level.getUniqueName})" if level.getNameExp
              level_fields << level.getUniqueName
            end
          end
          if measure.is_a?(Java::MondrianRolap::RolapStoredMeasure) && role.canAccess(measure)
            fields << measure.getUniqueName
          end
          raise ArgumentError, "no accessible drill through fields" if fields.empty?

          fields
        end

        # Role getAccess of a level below the bottom level of a hierarchy grant falls back to the
        # dimension access, so the level depth is checked against the grant explicitly.
        def self.level_accessible?(level, role)
          hierarchy = level.getHierarchy
          return false if role.getAccess(hierarchy) == NONE_ACCESS
          return true unless access_details = role.getAccessDetails(hierarchy)

          level.getDepth.between?(access_details.getTopLevelDepth, access_details.getBottomLevelDepth)
        end

        # Mondrian keeps the closure table of a parent child hierarchy as a hidden hierarchy.
        def self.closure_hierarchy?(hierarchy)
          return false unless hierarchy.respond_to?(:getRolapHierarchy)

          closure_for_field = hierarchy.getRolapHierarchy.java_class.declared_field('closureFor')
          closure_for_field.accessible = true
          !closure_for_field.value(hierarchy.getRolapHierarchy).nil?
        end

        def self.parse_return_fields(result, params)
          nonempty_columns = []
          return_fields = []
          sql_options = nil
          role = params[:role]

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

                # The cube schema reader resolves every level and measure regardless of the role.
                if role && level_or_member && !return_field_accessible?(level_or_member, role)
                  raise ArgumentError, "return field #{member_full_name} is not accessible"
                end
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
                  raise ArgumentError, "nonempty field #{nonempty_field} is not accessible" if role && !role.canAccess(member)

                  sql_query = member.getStarMeasure.getSqlQuery
                  member.getStarMeasure.generateExprString(sql_query)
                else
                  raise ArgumentError, "nonempty field #{nonempty_field} should be measure"
                end
              end
            end
          end

          role_conditions = []
          if sql_options && role
            role_conditions = role_restriction_conditions(sql_options, role, result.getCube, schema_reader.withLocus)
          end

          [nonempty_columns, return_fields, role_conditions]
        end

        def self.return_field_accessible?(level_or_member, role)
          if level_or_member.is_a?(Java::MondrianOlap::Level)
            level_accessible?(level_or_member, role)
          else
            role.canAccess(level_or_member)
          end
        end

        def self.add_sql_attributes(field, options = {})
          member = field[:member]
          dialect = options[:dialect]
          sql_query = options[:sql_query]
          max_alias_length = options[:max_alias_length]
          params = options[:params]

          field[:quoted_table_name] = quoted_table_name(member, dialect)

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

        def self.quoted_table_name(member, dialect)
          table_name = member.respond_to?(:getTableName) && member.getTableName ||
            member.respond_to?(:getMondrianDefExpression) && (expr = member.getMondrianDefExpression) &&
            expr.respond_to?(:table) && expr.table
          dialect.quoteIdentifier(table_name) if table_name
        end

        CUSTOM_ACCESS = Java::MondrianOlap::Access::CUSTOM

        # SQL conditions limiting the rows to the granted members of every hierarchy the role limits
        # to specific members (for example an embed token page filter). Mondrian generates the drill
        # through SQL without the role, so the statement would return the rows of denied members.
        def self.role_restriction_conditions(options, role, cube, schema_reader)
          cube.getHierarchies.to_a.filter_map do |hierarchy|
            next if hierarchy.getDimension.isMeasures
            next unless role.getAccess(hierarchy) == CUSTOM_ACCESS

            if parent_child_level = hierarchy.getLevels.to_a.detect(&:isParentChild)
              parent_child_condition(parent_child_level, role, schema_reader, options)
            else
              member_roots = accessible_member_roots(hierarchy, role, schema_reader)
              # Every row is accessible when the role grants the all member completely.
              next if member_roots.any?(&:isAll)

              member_roots_condition(member_roots, options)
            end
          end
        end

        # The members whose descendants the role grants completely, found from the root members down.
        def self.accessible_member_roots(hierarchy, role, schema_reader)
          access_details = role.getAccessDetails(hierarchy)
          member_roots = []
          members = schema_reader.getHierarchyRootMembers(hierarchy).to_a
          while member = members.shift
            access = role.getAccess(member)
            if expand_member?(member, access, access_details)
              members.concat(schema_reader.getMemberChildren(member).to_a)
            elsif access != NONE_ACCESS
              member_roots << member
            end
          end
          member_roots
        end

        # A member above the top level of the hierarchy grant, or one the role grants partly above the
        # bottom level, is replaced by its children. The levels below the bottom level are not
        # accessible and their rows belong to the bottom level member.
        def self.expand_member?(member, access, access_details)
          depth = member.getLevel.getDepth
          return true if depth < access_details.getTopLevelDepth
          return false unless depth < access_details.getBottomLevelDepth

          access == CUSTOM_ACCESS || access == ALL_ACCESS && access_details.hasInaccessibleDescendants(member)
        end

        # A root member is matched by the key columns of its level and its ancestor levels, because
        # a level key may be unique only within the parent member.
        def self.member_roots_condition(member_roots, options)
          levels = []
          root_conditions = member_roots.map do |member_root|
            member_conditions = []
            member = member_root
            until member.nil? || member.isAll
              levels << member.getLevel
              member_conditions << member_key_condition(member, options)
              member = member.getParentMember
            end
            "(#{member_conditions.join(' AND ')})"
          end
          restriction_condition(levels.uniq, root_conditions, options[:dialect])
        end

        # The members of a parent child level have unique keys. The parent member is on the same level
        # and its key does not match the rows of its descendants, so each member is matched by its own key.
        # Role getAccess returns custom both for an ancestor that is visible only because of a granted
        # descendant and for a granted member with a denied descendant. Only the rows of fully granted
        # members are returned, so the rows of such an ancestor stay hidden.
        def self.parent_child_condition(level, role, schema_reader, options)
          members = schema_reader.getLevelMembers(level, false).to_a.select do |member|
            role.getAccess(member) == ALL_ACCESS
          end
          member_conditions = members.map { |member| member_key_condition(member, options) }
          restriction_condition([level], member_conditions, options[:dialect])
        end

        def self.restriction_condition(levels, member_conditions, dialect)
          {
            # No row is accessible when the role grants no member.
            sql: member_conditions.empty? ? '1 = 0' : member_conditions.join(' OR '),
            levels: levels,
            quoted_table_names: levels.map { |level| quoted_table_name(level, dialect) }.compact.uniq
          }
        end

        SQL_NULL_KEY = Java::MondrianRolap::RolapUtil.sqlNullValue

        # Mondrian stores a NULL key as the sqlNullValue marker object.
        def self.member_key_condition(member, options)
          key_expression = member.getLevel.getKeyExp.getExpression(options[:sql_query])
          if member.getKey == SQL_NULL_KEY
            "#{key_expression} IS NULL"
          else
            "#{key_expression} = #{quoted_key(member, options[:dialect])}"
          end
        end

        def self.quoted_key(member, dialect)
          buffer = Java::JavaLang::StringBuilder.new
          dialect.quote(buffer, member.getKey, member.getLevel.getDatatype)
          buffer.toString
        end
      end

      def self.java_to_ruby_value(value, column_type = nil)
        case value
        # Check nil value first as it is the most common case for empty cells in large sparse results.
        when NilClass, Numeric, String
          value
        when Java::JavaMath::BigDecimal
          BigDecimal(value.to_s)
        when Java::JavaSql::Clob
          clob_to_string(value)
        else
          value
        end
      end

      private

      def self.clob_to_string(value)
        if reader = value.getCharacterStream
          buffered_reader = Java::JavaIo::BufferedReader.new(reader)
          result = []
          while str = buffered_reader.readLine
            result << str
          end
          result.join("\n")
        end
      ensure
        if buffered_reader
          buffered_reader.close
        elsif reader
          reader.close
        end
      end

      def axes
        @axes ||= @raw_cell_set.getAxes
      end

      def axis_positions(map_method, join_with = false)
        axes.map do |axis|
          axis.getPositions.map do |position|
            raw_members = position.getMembers
            names =
              case map_method
              when :to_member then raw_members.map { |member| Member.new(member) }
              else raw_members.map(&map_method)
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
        columns: 0,
        rows: 1,
        pages: 2,
        sections: 3,
        chapters: 4
      }.freeze

      # Use cell ordinal arithmetics instead of passing a list of boxed java.lang.Integer coordinates
      # to getCell as it avoids creation of many short lived Java objects for large results.
      def recursive_values(value_method, axes_sequence, current_index, cell_ordinal = 0)
        axis_number = axes_sequence[current_index]
        return cell_value(value_method, cell_ordinal) unless axis_number

        axis_ordinal_multiplier = cell_ordinal_multipliers[axis_number]
        positions_size = axis_positions_sizes[axis_number]
        if axes_sequence[current_index + 1]
          (0...positions_size).map do |i|
            recursive_values(value_method, axes_sequence, current_index + 1,
              cell_ordinal + i * axis_ordinal_multiplier)
          end
        else
          # For the last axis in the sequence map cell values without recursion
          # to reduce method call overhead for each cell in large results.
          map_cell_values(positions_size, cell_ordinal, axis_ordinal_multiplier, &value_method)
        end
      end

      def map_cell_values(positions_size, first_cell_ordinal, axis_ordinal_multiplier)
        (0...positions_size).map do |i|
          value = yield @raw_cell_set.getCell(first_cell_ordinal + i * axis_ordinal_multiplier)
          # Check the most common value types inline to avoid a method call for each cell.
          value.nil? || value.is_a?(Numeric) || value.is_a?(String) ? value : self.class.java_to_ruby_value(value)
        end
      end

      def cell_value(value_method, cell_ordinal)
        self.class.java_to_ruby_value(@raw_cell_set.getCell(cell_ordinal).send(value_method))
      end

      def axis_positions_sizes
        @axis_positions_sizes ||= axes.map { |axis| axis.getPositions.size }
      end

      # Cell ordinal is a sum of cell coordinates on each axis multiplied by a corresponding axis multiplier
      # which is a product of positions sizes of all lower number axes.
      def cell_ordinal_multipliers
        @cell_ordinal_multipliers ||= begin
          multiplier = 1
          axis_positions_sizes.map do |positions_size|
            axis_multiplier = multiplier
            multiplier *= positions_size
            axis_multiplier
          end
        end
      end

    end
  end
end
