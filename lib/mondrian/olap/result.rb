require 'nokogiri'

module Mondrian
  module OLAP
    class Result
      def initialize(raw_result)
        @raw_result = raw_result
      end

      def axes_count
        axes.length
      end

      def axis_names
        @axis_names ||= axis_positions(:"getName")
      end

      def axis_full_names
        @axis_full_names ||= axis_positions(:"getUniqueName")
      end

      %w(column row page section chapter).each_with_index do |axis, i|
        define_method :"#{axis}_names" do
          axis_names[i]
        end

        define_method :"#{axis}_full_names" do
          axis_full_names[i]
        end
      end

      def values(*axes_sequence)
        if axes_sequence.empty?
          axes_sequence = (0...axes_count).to_a.reverse
        elsif axes_sequence.size != axes_count
          raise ArgumentError, "axes sequence size is not equal to result axes count"
        end
        recursive_values(axes_sequence, 0)
      end

      # format results in simple HTML table
      def to_html
        case axes_count
        when 1
          builder = Nokogiri::XML::Builder.new do |doc|
            doc.table do
              doc.tr do
                column_full_names.each do |column_full_name|
                  doc.th column_full_name, :align => 'right'
                end
              end
              doc.tr do
                values.each do |value|
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
                  doc.th column_full_name, :align => 'right'
                end
              end
              values.each_with_index do |row, i|
                doc.tr do
                  doc.th row_full_names[i], :align => 'left'
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
        @axes ||= @raw_result.getAxes
      end

      def axis_positions(map_method, join_with=',')
        axes.map do |axis|
          axis.getPositions.map do |position|
            position.map do |member|
              member.send(map_method)
            end.join(join_with)
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

      def recursive_values(axes_sequence, current_index, cell_params=[])
        if axis_number = axes_sequence[current_index]
          axis_number = AXIS_SYMBOL_TO_NUMBER[axis_number] if axis_number.is_a?(Symbol)
          positions_size = axes[axis_number].getPositions.size
          (0...positions_size).map do |i|
            cell_params[axis_number] = i
            recursive_values(axes_sequence, current_index + 1, cell_params)
          end
        else
          @raw_result.getCell(cell_params).getValue
        end
      end

    end
  end
end