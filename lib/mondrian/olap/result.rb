require 'nokogiri'
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

      # specify drill through cell position, for example, as
      #   :row => 0, :cell => 1
      # specify max returned rows with :max_rows parameter
      def drill_through(position_params = {})
        Error.wrap_native_exception do
          cell_params = []
          axes_count.times do |i|
            axis_symbol = AXIS_SYMBOLS[i]
            raise ArgumentError, "missing position #{axis_symbol.inspect}" unless axis_position = position_params[axis_symbol]
            cell_params << Java::JavaLang::Integer.new(axis_position)
          end
          raw_cell = @raw_cell_set.getCell(cell_params)
          DrillThrough.from_raw_cell(raw_cell, position_params)
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
          if rolap_cell.canDrillThrough
            sql_statement = rolap_cell.drillThroughInternal(max_rows, -1, nil, true, nil)
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
