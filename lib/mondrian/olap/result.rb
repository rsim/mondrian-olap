# frozen_string_literal: true

require 'bigdecimal'

module Mondrian
  module OLAP
    class Result
      def initialize(connection, raw_cell_set, options = {})
        @connection = connection
        @raw_cell_set = raw_cell_set
        # Drill through uses the role of the query execution, because the connection role may change later.
        @role = options[:role] || connection.raw_mondrian_connection.getRole
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
          DrillThrough.from_raw_cell(raw_cell, params.merge(role: @role))
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
