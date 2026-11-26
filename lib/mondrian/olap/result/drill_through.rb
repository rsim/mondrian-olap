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
          if (role = params[:role]) && (return_elements = params[:return_elements])
            params = params.merge(return: statement_return_fields(rolap_cell, result, return_elements), native_columns: true)
          # A return string without fields, e.g. an empty one, also selects the default fields.
          # Mondrian does not group the default fields.
          elsif role && return_field_names(params[:return]).all?(&:empty?)
            params = params.merge(
              return: accessible_return_fields(rolap_cell, result, role), native_columns: true, group_by: nil
            )
          end
          base_cube = drill_through_cube(drill_through_members(rolap_cell), result)
          nonempty_columns, return_fields = parse_return_fields(result, params, base_cube)
          role_conditions = role ? role_restriction_conditions(rolap_cell, result, role) : []
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
                elsif return_fields[i][:denied] || params[:native_columns]
                  # Mondrian native drill through returns NULL for a field of another cube of a virtual cube.
                  "NULL AS #{column_alias}"
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
          outer_join_conditions = {}

          outer_join_from_parts.each do |part|
            part_elements = part.split(/\s+/)
            # First is original table, then optional 'as' and the last is alias
            table_alias = part_elements.last
            join_conditions = where_parts.select do |where_part|
              where_part.include?(" = #{table_alias}.")
            end
            outer_join_conditions[table_alias] = join_conditions
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
            new_where += " AND (#{role_condition_sql(condition, outer_join_conditions)})"
          end

          sql = "select #{new_select} from #{new_from} where #{new_where}"
          sql << " group by #{new_group_by}" unless new_group_by.empty?
          sql << " order by #{new_order_by}" unless new_order_by.empty?
          sql
        end

        NONE_ACCESS = Java::MondrianOlap::Access::NONE
        ALL_ACCESS = Java::MondrianOlap::Access::ALL

        # The role is checked by its grants and not by the olap4j role name, because a role from the
        # connection string has no role name.
        def self.role_restricts_cube?(role, cube)
          !role.nil? && cube.getHierarchies.any? { |hierarchy| role.getAccess(hierarchy) != ALL_ACCESS }
        end

        # The fields Mondrian selects without a return clause, limited to the levels and measures the role grants.
        # Mondrian adds these columns without consulting the role, so a denied hierarchy would be returned.
        def self.accessible_return_fields(rolap_cell, result, role)
          cell_members = drill_through_members(rolap_cell)
          base_cube = drill_through_cube(cell_members, result)
          measure, *members = cell_members.to_a
          schema_reader = result.getCube.getSchemaReader.withLocus
          # Mondrian selects the columns of the drill through cell request, which depend on the cell members.
          # For example, it omits the country of a cell with a state of unique members.
          request_columns = Java::MondrianRolap::RolapAggregationManager.
            makeDrillThroughRequest(cell_members, true, result.getCube, Java::JavaUtil::ArrayList.new).
            getConstrainedColumns.to_a

          levels = members.flat_map do |member|
            hierarchy = member.getHierarchy
            next [] if closure_hierarchy?(hierarchy)

            hierarchy.getLevels.to_a.select do |level|
              request_columns.include?(level.getBaseStarKeyColumn(base_cube)) && level_accessible?(level, role, schema_reader)
            end
          end
          fields = level_fields(levels, base_cube)
          if measure.is_a?(Java::MondrianRolap::RolapStoredMeasure) && role.canAccess(measure)
            fields << measure.getUniqueName
          end
          raise ArgumentError, "no accessible drill through fields" if fields.empty?

          fields
        end

        # The fields of a DRILLTHROUGH statement return clause in the order Mondrian selects them: the
        # levels and then the measures of the drill through cube, followed by the fields of another cube
        # of a virtual cube in the return clause order. A hierarchy or a dimension stands for its first level.
        def self.statement_return_fields(rolap_cell, result, return_elements)
          base_cube = drill_through_cube(drill_through_members(rolap_cell), result)
          elements = return_elements.map do |element|
            if element.is_a?(Java::MondrianOlap::Hierarchy) || element.is_a?(Java::MondrianOlap::Dimension)
              first_level(element.getHierarchy)
            else
              element
            end
          end
          cube_elements, other_cube_elements = elements.partition do |element|
            drill_through_cube_element?(element, base_cube)
          end
          cube_levels, cube_members = cube_elements.partition { |element| element.is_a?(Java::MondrianOlap::Level) }
          other_cube_fields = other_cube_elements.map do |element|
            use_name_field = element.is_a?(Java::MondrianOlap::Level) && element.getNameExp
            use_name_field ? "Name(#{element.getUniqueName})" : element.getUniqueName
          end
          level_fields(cube_levels, base_cube) + cube_members.map(&:getUniqueName) + other_cube_fields
        end

        # A level or a stored measure of another cube of a virtual cube does not belong to the drill through cube.
        def self.drill_through_cube_element?(element, base_cube)
          if element.is_a?(Java::MondrianOlap::Level)
            !element.getBaseStarKeyColumn(base_cube).nil?
          elsif element.is_a?(Java::MondrianRolap::RolapStoredMeasure)
            element.getCube == base_cube
          else
            true
          end
        end

        def self.drill_through_members(rolap_cell)
          members_method = rolap_cell.java_class.declared_method('getMembersForDrillThrough')
          members_method.accessible = true
          members_method.invoke(rolap_cell)
        end

        def self.drill_through_cube(cell_members, result)
          Java::MondrianRolap::RolapCell.chooseDrillThroughCube(cell_members, result.getCube)
        end

        def self.first_level(hierarchy)
          level = hierarchy.getLevels.first
          level.isAll ? level.getChildLevel : level
        end

        # Mondrian selects a column shared by several hierarchies once, in the order of the star columns,
        # and selects the name column of a level before its key column.
        def self.level_fields(levels, base_cube)
          level_columns = levels.map { |level| [level, level.getBaseStarKeyColumn(base_cube)] }.uniq(&:last)
          level_columns.sort_by { |_level, column| column.getBitPosition }.flat_map do |level, _column|
            level.getNameExp ? ["Name(#{level.getUniqueName})", level.getUniqueName] : [level.getUniqueName]
          end
        end

        # Role getAccess of a level below the bottom level of a hierarchy grant falls back to the
        # dimension access, so the level depth is checked against the grant explicitly.
        def self.level_accessible?(level, role, schema_reader)
          hierarchy = level.getHierarchy
          return false if role.getAccess(hierarchy) == NONE_ACCESS
          return true unless access_details = role.getAccessDetails(hierarchy)

          level.getDepth.between?(access_details.getTopLevelDepth, access_details.getBottomLevelDepth) &&
          member_roots_reach_level?(level, role, schema_reader)
        end

        # The access details of a union role have the deepest bottom level of its roles, although each role
        # grants its members only down to its own bottom level. A level below a granted member is accessible
        # when a role of the union grants this member completely and its bottom level reaches the level.
        def self.member_roots_reach_level?(level, role, schema_reader)
          roles = union_roles(role)
          return true if roles.size == 1

          hierarchy = level.getHierarchy
          accessible_member_roots(hierarchy, role, schema_reader).all? do |member|
            member.getLevel.getDepth >= level.getDepth ||
            roles.any? do |union_role|
              union_role.getAccess(member) == ALL_ACCESS &&
              ((access_details = union_role.getAccessDetails(hierarchy)).nil? ||
                level.getDepth <= access_details.getBottomLevelDepth)
            end
          end
        end

        # Mondrian UnionRoleImpl keeps its roles in a private list, and a role of the union may be a union too.
        def self.union_roles(role)
          role_list_field = role.java_class.declared_field('roleList')
          role_list_field.accessible = true
          role_list_field.value(role).to_a.flat_map { |union_role| union_roles(union_role) }
        rescue Java::JavaLang::NoSuchFieldException
          [role]
        end

        # Mondrian keeps the closure table of a parent child hierarchy as a hidden hierarchy.
        def self.closure_hierarchy?(hierarchy)
          return false unless hierarchy.respond_to?(:getRolapHierarchy)

          closure_for_field = hierarchy.getRolapHierarchy.java_class.declared_field('closureFor')
          closure_for_field.accessible = true
          !closure_for_field.value(hierarchy.getRolapHierarchy).nil?
        end

        def self.return_field_names(return_fields)
          return_fields.is_a?(String) ? return_fields.split(/,\s*/) : Array(return_fields)
        end

        def self.parse_return_fields(result, params, base_cube)
          nonempty_columns = []
          return_fields = []
          role = params[:role]

          if params[:return] || params[:nonempty]
            rolap_cube = result.getCube
            schema_reader = rolap_cube.getSchemaReader
            dialect = result.getCube.getSchema.getDialect
            sql_query = Java::mondrian.rolap.sql.SqlQuery.new(dialect)

            if fields = params[:return]
              fields = return_field_names(fields)
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
                params: params,
                column_aliases: [],
                base_cube: base_cube
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

                if level_or_member.is_a?(Java::MondrianOlap::Member) && level_or_member.isCalculated
                  raise ArgumentError, "cannot use calculated member #{member_full_name} as return field"
                elsif !level_or_measure?(level_or_member)
                  raise ArgumentError, "return field #{member_full_name} should be level or measure"
                end

                add_sql_attributes return_fields[i], sql_options
                # The cube schema reader resolves every level and measure regardless of the role. A field that
                # the role denies is returned without values, so the columns keep the order of the return fields.
                if role && !return_field_accessible?(level_or_member, role, schema_reader.withLocus)
                  return_fields[i].merge!(column_expression: nil, denied: true)
                end
              end
            end

            if nonempty_fields = params[:nonempty]
              nonempty_fields = nonempty_fields.split(/,\s*/) if nonempty_fields.is_a?(String)
              nonempty_columns = nonempty_fields.filter_map do |nonempty_field|
                begin
                  segment_list = Java::MondrianOlap::Util.parseIdentifier(nonempty_field)
                rescue Java::JavaLang::IllegalArgumentException
                  raise ArgumentError, "invalid return field #{nonempty_field}"
                end
                member = schema_reader.lookupCompound rolap_cube, segment_list, false, 0
                if member.is_a? Java::MondrianOlap::Member
                  raise ArgumentError, "cannot use calculated member #{nonempty_field} as nonempty field" if member.isCalculated
                  # A measure that the role denies does not select the rows, as its values are not returned either.
                  next if role && !role.canAccess(member)

                  sql_query = member.getStarMeasure.getSqlQuery
                  member.getStarMeasure.generateExprString(sql_query)
                else
                  raise ArgumentError, "nonempty field #{nonempty_field} should be measure"
                end
              end
            end
          end

          [nonempty_columns, return_fields]
        end

        def self.level_or_measure?(element)
          element.is_a?(Java::MondrianOlap::Level) || (element.is_a?(Java::MondrianOlap::Member) && element.isMeasure)
        end

        def self.return_field_accessible?(level_or_member, role, schema_reader)
          if level_or_member.is_a?(Java::MondrianOlap::Level)
            level_accessible?(level_or_member, role, schema_reader)
          else
            role.canAccess(level_or_member)
          end
        end

        def self.add_sql_attributes(field, options = {})
          member = drill_through_cube_level(field[:member], options[:base_cube])
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

          # Mondrian native drill through adds the key suffix only to a level with a name column.
          key_suffix = field[:type] == :key && (!params[:native_columns] || member.getNameExp)
          column_alias = key_suffix ? "#{field[:name]} (Key)" : field[:name]
          column_alias =
            if params[:native_columns]
              unique_column_alias(column_alias, options[:column_aliases], max_alias_length)
            elsif max_alias_length
              column_alias[0, max_alias_length]
            else
              column_alias
            end
          field[:column_alias] = quoted_alias(column_alias, dialect)
        end

        # Mondrian native drill through (DrillThroughQuerySpec makeAlias) appends a numeric suffix to a repeated
        # column alias, e.g. to the same level name of two hierarchies.
        def self.unique_column_alias(column_alias, column_aliases, max_alias_length)
          unique_alias = max_alias_length ? column_alias[0, max_alias_length] : column_alias
          index = 0
          while column_aliases.include?(unique_alias)
            suffix = "_#{index}"
            unique_alias = (max_alias_length ? column_alias[0, max_alias_length - suffix.length] : column_alias) + suffix
            index += 1
          end
          column_aliases << unique_alias
          unique_alias
        end

        # Mondrian Dialect quoteIdentifier does not escape a value that starts and ends with the quote string,
        # and it does not quote at all when the JDBC driver reports an empty quote string. The property name
        # of a return field comes from the caller, so the alias must not contain the quote string. Oracle does
        # not allow an escaped quote string in an identifier, so the quote string is removed. A JDBC driver
        # without identifier quoting may also report a blank quote string.
        def self.quoted_alias(column_alias, dialect)
          quote = dialect.getQuoteIdentifierString
          if quote.nil? || quote.strip.empty?
            column_alias.gsub(/\W/, '_')
          else
            dialect.quoteIdentifier(column_alias.gsub(quote, ''))
          end
        end

        # The expressions of a virtual cube level use the table aliases of the schema. The drill through SQL uses
        # the table aliases of the drill through cube, which joins a dimension table twice with another alias.
        def self.drill_through_cube_level(level_or_member, base_cube)
          if level_or_member.is_a?(Java::MondrianRolap::RolapCubeLevel) && level_or_member.getCube.isVirtual
            base_cube&.findBaseCubeLevel(level_or_member) || level_or_member
          else
            level_or_member
          end
        end

        def self.quoted_table_name(member, dialect)
          table_name = member.respond_to?(:getTableName) && member.getTableName ||
            member.respond_to?(:getMondrianDefExpression) && (expr = member.getMondrianDefExpression) &&
            expr.respond_to?(:table) && expr.table
          dialect.quoteIdentifier(table_name) if table_name
        end

        CUSTOM_ACCESS = Java::MondrianOlap::Access::CUSTOM

        # SQL conditions limiting the rows to the granted members of every hierarchy the role limits
        # to specific members. Mondrian generates the drill through SQL without the role, so the
        # statement would return the rows of denied members.
        # The conditions use the key columns of the drill through cube star, as the drill through SQL does.
        # The key expression of a virtual cube level uses the table alias of the schema, and the drill
        # through cube may join this table with another alias.
        def self.role_restriction_conditions(rolap_cell, result, role)
          cube = result.getCube
          dialect = cube.getSchema.getDialect
          options = {
            dialect: dialect,
            sql_query: Java::mondrian.rolap.sql.SqlQuery.new(dialect),
            base_cube: drill_through_cube(drill_through_members(rolap_cell), result)
          }
          schema_reader = cube.getSchemaReader.withLocus
          cube.getHierarchies.to_a.filter_map do |hierarchy|
            next if hierarchy.getDimension.isMeasures
            next unless role.getAccess(hierarchy) == CUSTOM_ACCESS
            # A hierarchy grant with only level bounds denies no member, so every row is accessible
            # and the members are not read.
            next unless member_grants?(role.getAccessDetails(hierarchy))
            # A hierarchy of another cube of a virtual cube has no table in the query, so its rows are not limited.
            next unless drill_through_cube_element?(first_level(hierarchy), options[:base_cube])

            condition =
              if parent_child_level = hierarchy.getLevels.to_a.detect(&:isParentChild)
                parent_child_condition(parent_child_level, role, schema_reader, options)
              else
                member_roots = accessible_member_roots(hierarchy, role, schema_reader)
                # A grant of the all member makes every row accessible, and the all level has no key column.
                next if member_roots.any?(&:isAll)

                member_roots_condition(member_roots, options)
              end
            # The full rollup policy includes the fact rows without a member of the hierarchy in the all member
            # cell value. These rows belong to no member that the role denies.
            if role.getAccessDetails(hierarchy).getRollupPolicy == FULL_ROLLUP
              condition[:null_foreign_key] = fact_foreign_key(first_level(hierarchy), options)
            end
            condition
          end
        end

        # A fact row without a row of an outer joined table belongs to no member of its hierarchy, but it
        # matches the IS NULL condition of a granted member with a NULL key. The join conditions of the role
        # condition tables exclude these rows, as the inner joins of the Mondrian cell queries do.
        def self.role_condition_sql(condition, outer_join_conditions)
          join_conditions = outer_join_conditions.values_at(*condition[:table_aliases]).compact.flatten
          sql = ["(#{condition[:sql]})", *join_conditions].join(' AND ')
          condition[:null_foreign_key] ? "#{sql} OR #{condition[:null_foreign_key]} IS NULL" : sql
        end

        # The fact table column which joins the dimension table of the level, or nil for a level of the fact table.
        def self.fact_foreign_key(level, options)
          table = level.getBaseStarKeyColumn(options[:base_cube]).getTable
          table = table.getParentTable while table.getParentTable&.getParentTable
          table.getJoinCondition.getLeft(options[:sql_query]) if table.getParentTable
        end

        # Mondrian keeps the member grants of a hierarchy access in a private map without a public
        # method telling whether there are any. A hierarchy access of another class is walked.
        def self.member_grants?(access_details)
          member_grants_field = access_details.java_class.declared_field('memberGrants')
          member_grants_field.accessible = true
          !member_grants_field.value(access_details).isEmpty
        rescue Java::JavaLang::NoSuchFieldException
          true
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
            elsif access != NONE_ACCESS && !hidden_by_rollup_policy?(member, access, access_details)
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
          depth < access_details.getTopLevelDepth ||
          depth < access_details.getBottomLevelDepth && partly_accessible?(member, access, access_details)
        end

        FULL_ROLLUP = Java::MondrianOlap::Role::RollupPolicy::FULL

        # Only the full rollup policy includes the inaccessible descendants of a bottom level member
        # in its cell value. With the partial or hidden policy the cell value of the member is empty,
        # so its rows are not returned either.
        def self.hidden_by_rollup_policy?(member, access, access_details)
          access_details.getRollupPolicy != FULL_ROLLUP && partly_accessible?(member, access, access_details)
        end

        def self.partly_accessible?(member, access, access_details)
          access == CUSTOM_ACCESS || (access == ALL_ACCESS && access_details.hasInaccessibleDescendants(member))
        end

        # A root member is matched by the key columns of its level and its ancestor levels, because
        # a level key may be unique only within the parent member. The roots with the same parent
        # share the ancestor conditions and are matched by one key list.
        def self.member_roots_condition(member_roots, options)
          levels = []
          siblings_by_parent = member_roots.group_by { |member| [member.getParentMember, member.getLevel] }
          sibling_conditions = siblings_by_parent.map do |(parent_member, level), siblings|
            levels << level
            member_conditions = [members_key_condition(siblings, options)]
            ancestor = parent_member
            until ancestor.nil? || ancestor.isAll
              levels << ancestor.getLevel
              member_conditions << members_key_condition([ancestor], options)
              ancestor = ancestor.getParentMember
            end
            "(#{member_conditions.join(' AND ')})"
          end
          restriction_condition(levels.uniq, sibling_conditions, options)
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
          member_conditions = members.empty? ? [] : [members_key_condition(members, options)]
          restriction_condition([level], member_conditions, options)
        end

        def self.restriction_condition(levels, member_conditions, options)
          table_aliases = levels.map do |level|
            options[:dialect].quoteIdentifier(level.getBaseStarKeyColumn(options[:base_cube]).getTable.getAlias)
          end
          {
            # No row is accessible when the role grants no member.
            sql: member_conditions.empty? ? '1 = 0' : member_conditions.join(' OR '),
            levels: levels,
            table_aliases: table_aliases.uniq
          }
        end

        SQL_NULL_KEY = Java::MondrianRolap::RolapUtil.sqlNullValue

        # Oracle rejects an IN list with more than 1000 expressions.
        MAX_IN_LIST_SIZE = 1000

        # The members are of the same level. Mondrian stores a NULL key as the sqlNullValue marker object.
        def self.members_key_condition(members, options)
          key_column = members.first.getLevel.getBaseStarKeyColumn(options[:base_cube])
          key_expression = key_column.generateExprString(options[:sql_query])
          null_key_members, key_members = members.partition { |member| member.getKey == SQL_NULL_KEY }
          key_conditions = key_members.each_slice(MAX_IN_LIST_SIZE).map do |members_slice|
            quoted_keys = members_slice.map { |member| quoted_key(member, options[:dialect]) }
            if quoted_keys.size == 1
              "#{key_expression} = #{quoted_keys.first}"
            else
              "#{key_expression} IN (#{quoted_keys.join(', ')})"
            end
          end
          key_conditions << "#{key_expression} IS NULL" unless null_key_members.empty?
          key_conditions.size == 1 ? key_conditions.first : "(#{key_conditions.join(' OR ')})"
        end

        def self.quoted_key(member, dialect)
          buffer = Java::JavaLang::StringBuilder.new
          dialect.quote(buffer, member.getKey, member.getLevel.getDatatype)
          buffer.toString
        end
      end
    end
  end
end
