module Mondrian
  module OLAP
    class Query
      def self.from(connection, cube_name)
        query = self.new(connection)
        query.cube_name = cube_name
        query
      end

      attr_accessor :cube_name

      def initialize(connection)
        @connection = connection
        @cube = nil
        @axes = []
        @where = []
        @with_members = []
      end

      # Add new axis(i) to query
      # or return array of axis(i) members if no arguments specified
      def axis(i, *axis_members)
        if axis_members.empty?
          @axes[i]
        else
          @axes[i] ||= []
          @current_axis = i
          if axis_members.length == 1 && axis_members[0].is_a?(Array)
            @axes[i].concat(axis_members[0])
          else
            @axes[i].concat(axis_members)
          end
          self
        end
      end

      AXIS_ALIASES = %w(columns rows pages sections chapters)
      AXIS_ALIASES.each_with_index do |axis, i|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{axis}(*axis_members)
            axis(#{i}, *axis_members)
          end
        RUBY
      end

      def crossjoin(*axis_members)
        raise ArgumentError, "cannot use crossjoin method before axis method" unless @current_axis
        raise ArgumentError, "specify list of members for crossjoin method" if axis_members.empty?
        members = axis_members.length == 1 && axis_members[0].is_a?(Array) ? axis_members[0] : axis_members
        @axes[@current_axis] = [:crossjoin, @axes[@current_axis], members]
        self
      end

      def except(*axis_members)
        raise ArgumentError, "cannot use except method before axis method" unless @current_axis
        raise ArgumentError, "specify list of members for except method" if axis_members.empty?
        members = axis_members.length == 1 && axis_members[0].is_a?(Array) ? axis_members[0] : axis_members
        if @axes[@current_axis][0] == :crossjoin
          @axes[@current_axis][2] = [:except, @axes[@current_axis][2], members]
        else
          @axes[@current_axis] = [:except, @axes[@current_axis], members]
        end
        self
      end

      def nonempty
        raise ArgumentError, "cannot use crossjoin method before axis method" unless @current_axis
        @axes[@current_axis] = [:nonempty, @axes[@current_axis]]
        self
      end

      VALID_ORDERS = ['ASC', 'BASC', 'DESC', 'BDESC']

      def order(expression, direction)
        raise ArgumentError, "cannot use order method before axis method" unless @current_axis
        direction = direction.to_s.upcase
        raise ArgumentError, "invalid order direction #{direction.inspect}," <<
          " should be one of #{VALID_ORDERS.inspect[1..-2]}" unless VALID_ORDERS.include?(direction)
        @axes[@current_axis] = [:order, @axes[@current_axis], expression, direction]
        self
      end

      def hierarchize(order=nil, all=nil)
        raise ArgumentError, "cannot use hierarchize method before axis method" unless @current_axis
        order = order && order.to_s.upcase
        raise ArgumentError, "invalid hierarchize order #{order.inspect}" unless order.nil? || order == 'POST'
        if all.nil? && @axes[@current_axis][0] == :crossjoin
          @axes[@current_axis][2] = [:hierarchize, @axes[@current_axis][2]]
          @axes[@current_axis][2] << order if order
        else
          @axes[@current_axis] = [:hierarchize, @axes[@current_axis]]
          @axes[@current_axis] << order if order
        end
        self
      end

      def hierarchize_all(order=nil)
        hierarchize(order, :all)
      end

      # Add new WHERE condition to query
      # or return array of existing conditions if no arguments specified
      def where(*members)
        if members.empty?
          @where
        else
          if members.length == 1 && members[0].is_a?(Array)
            @where.concat(members[0])
          else
            @where.concat(members)
          end
          self
        end
      end

      # Add definition of calculated member
      def with_member(member=nil, options={})
        if member.nil?
          @with_members
        elsif member.is_a?(Array)
          member.each{|m, o| with_member(m, o)}
          self
        else
          raise ArgumentError, ":as option is mandatory" unless options[:as]
          @with_members << [member, options]
          self
        end
      end

      def to_mdx
        mdx = ""
        mdx << "WITH #{with_to_mdx}\n" unless @with_members.empty?
        mdx << "SELECT #{axis_to_mdx}\n"
        mdx << "FROM #{from_to_mdx}"
        mdx << "\nWHERE #{where_to_mdx}" unless @where.empty?
        mdx
      end

      def execute
        @connection.execute to_mdx
      end

      private

      def with_to_mdx
        @with_members.map do |member, options|
          options_string = ''
          options.each do |option, value|
            unless option == :as
              options_string << ", #{option.to_s.upcase} = #{quote_value(value)}"
            end
          end
          "MEMBER #{member} AS #{quote_value(options[:as])}#{options_string}"
        end.join("\n")
      end

      def axis_to_mdx
        mdx = ""
        @axes.each_with_index do |axis_members, i|
          axis_name = AXIS_ALIASES[i] ? AXIS_ALIASES[i].upcase : "AXIS(#{i})"
          mdx << ",\n" if i > 0
          mdx << members_to_mdx(axis_members) << " ON " << axis_name
        end
        mdx
      end

      def members_to_mdx(axis_members)
        if axis_members.length == 1
          axis_members[0]
        elsif axis_members[0].is_a?(Symbol)
          case axis_members[0]
          when :crossjoin
            "CROSSJOIN(#{members_to_mdx(axis_members[1])}, #{members_to_mdx(axis_members[2])})"
          when :except
            "EXCEPT(#{members_to_mdx(axis_members[1])}, #{members_to_mdx(axis_members[2])})"
          when :nonempty
            "NON EMPTY #{members_to_mdx(axis_members[1])}"
          when :order
            expression = axis_members[2].is_a?(Array) ? "(#{axis_members[2].join(', ')})" : axis_members[2]
            "ORDER(#{members_to_mdx(axis_members[1])}, #{expression}, #{axis_members[3]})"
          when :hierarchize
            "HIERARCHIZE(#{members_to_mdx(axis_members[1])}#{axis_members[2] && ", #{axis_members[2]}"})"
          else
            raise ArgumentError, "Cannot generate MDX for invalid set operation #{axis_members[0].inspect}"
          end
        else
          "{#{axis_members.join(', ')}}"
        end
      end

      def from_to_mdx
        "[#{@cube_name}]"
      end

      def where_to_mdx
        mdx = '('
        mdx << @where.map do |condition|
          condition
        end.join(', ')
        mdx << ')'
      end

      def quote_value(value)
        case value
        when String
          "'#{value.gsub("'", "''")}'"
        when TrueClass, FalseClass
          value ? 'TRUE' : 'FALSE'
        when NilClass
          'NULL'
        else
          "#{value}"
        end
      end
    end
  end
end