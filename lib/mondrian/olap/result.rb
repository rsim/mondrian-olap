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
          DrillThrough.from_raw_cell(raw_cell, params.merge(role_name: @connection.role_name))
        end
      end

      class DrillThrough
        def self.from_raw_cell(raw_cell, params = {})
          # workaround to avoid calling raw_cell.drillThroughInternal private method
          # which fails when running inside TorqueBox
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
              row_values << Result.java_to_ruby_value(@raw_result_set.getObject(i + 1), column_type)
            end
            can_access_row_values?(row_values) ? row_values : fetch
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

        # modified RolapCell drillThroughInternal method
        def self.drill_through_internal(rolap_cell, params)
          max_rows = params[:max_rows] || -1

          result_field = rolap_cell.java_class.declared_field('result')
          result_field.accessible = true
          result = result_field.value(rolap_cell)

          sql, return_fields = generate_drill_through_sql(rolap_cell, result, params)

          # Choose the appropriate scrollability. If we need to start from an
          # offset row, it is useful that the cursor is scrollable, but not
          # essential.
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
          # the latest Mondrian version sometimes returns sql_non_extended without order by
          elsif sql_non_extended =~ /\Aselect (.*) from (.*) where (.*)\Z/m
            non_extended_from = $2
            non_extended_where = $3
          # if drill through total measure with just all members selection
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
          # if only measures are selected then there will be no order by
          elsif sql_extended =~ /\Aselect (.*) from (.*) where (.*)\Z/m
            extended_select = $1
            extended_from = $2
            extended_where = $3
            extended_order_by = ''
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
            new_group_by = ''
          end

          new_from_parts = non_extended_from.split(/,\s*/)
          outer_join_from_parts = extended_from.split(/,\s*/) - new_from_parts
          where_parts = extended_where.split(' and ')

          outer_join_from_parts.each do |part|
            part_elements = part.split(/\s+/)
            # first is original table, then optional 'as' and the last is alias
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
                return_fields << case field
                  when /\AName\((.*)\)\z/i then
                    { member_full_name: $1, type: :name }
                  when /\AProperty\((.*)\s*,\s*'(.*)'\)\z/i then
                    { member_full_name: $1, type: :property, name: $2 }
                  else
                    { member_full_name: field }
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

                # if this is property field then the name is initialized already
                return_fields[i][:name] ||= segment_list.to_a.last.name
                level_or_member = schema_reader.lookupCompound rolap_cube, segment_list, false, 0
                return_fields[i][:member] = level_or_member

                if level_or_member.is_a? Java::MondrianOlap::Member
                  raise ArgumentError, "cannot use calculated member #{member_full_name} as return field" if level_or_member.isCalculated
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
            add_role_restricition_fields return_fields, sql_options
          end

          [nonempty_columns, return_fields]
        end

        def self.add_sql_attributes(field, options = {})
          member = field[:member]
          dialect = options[:dialect]
          sql_query = options[:sql_query]
          max_alias_length = options[:max_alias_length]
          params = options[:params]

          if table_name = (member.try(:getTableName) || member.try(:getMondrianDefExpression).try(:table))
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
                # property.getExp is a protected method therefore
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
          field[:column_alias] = dialect.quoteIdentifier(
            max_alias_length ? column_alias[0, max_alias_length] : column_alias
          )
        end

        def self.add_role_restricition_fields(fields, options = {})
          # For each unique level field add a set of fields to be able to build level member full name from database query results
          fields.map { |f| f[:member] }.uniq.each_with_index do |level_or_member, i|
            next if level_or_member.is_a?(Java::MondrianOlap::Member)
            current_level = level_or_member
            loop do
              # Create an additional field name using a pattern "_level:<Fieldset ID>:<Level depth>"
              fields << {member: current_level, type: :name_or_key, name: "_level:#{i}:#{current_level.getDepth}"}
              add_sql_attributes fields.last, options
              break unless (current_level = current_level.getParentLevel) and !current_level.isAll
            end
          end
        end
      end

      def self.java_to_ruby_value(value, column_type = nil)
        case value
        when Numeric, String
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

      def recursive_values(value_method, axes_sequence, current_index, cell_params = [])
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
