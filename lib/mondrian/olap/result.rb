require 'nokogiri'

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

      %w(column row page section chapter).each_with_index do |axis, i|
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
          builder = Nokogiri::XML::Builder.new do |doc|
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
          builder = Nokogiri::XML::Builder.new do |doc|
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
          @raw_cell_set.getCell(cell_params).send(value_method)
        end
      end

    end
  end
end