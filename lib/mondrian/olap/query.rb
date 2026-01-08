# frozen_string_literal: true

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
        @with = []
      end

      # Add new axis(i) to query or return array of axis(i) members if no arguments specified
      def axis(i, *axis_members)
        if axis_members.empty?
          @axes[i]
        else
          @axes[i] ||= []
          @current_set = @axes[i]
          if axis_members.length == 1 && axis_members[0].is_a?(Array)
            @current_set.concat(axis_members[0])
          else
            @current_set.concat(axis_members)
          end
          self
        end
      end

      AXIS_ALIASES = %w(columns rows pages chapters sections).freeze
      AXIS_ALIASES.each_with_index do |axis, i|
        class_eval <<~RUBY, __FILE__, __LINE__ + 1
          def #{axis}(*axis_members)
            axis(#{i}, *axis_members)
          end
        RUBY
      end

      %w(crossjoin nonempty_crossjoin).each do |method|
        class_eval <<~RUBY, __FILE__, __LINE__ + 1
          def #{method}(*axis_members)
            validate_current_set
            raise ArgumentError, "specify set of members for #{method} method" if axis_members.empty?
            members = axis_members.length == 1 && axis_members[0].is_a?(Array) ? axis_members[0] : axis_members
            add_current_set_function :#{method}, members
            self
          end
        RUBY
      end

      def except(*axis_members)
        validate_current_set
        raise ArgumentError, "specify set of members for except method" if axis_members.empty?
        members = axis_members.length == 1 && axis_members[0].is_a?(Array) ? axis_members[0] : axis_members
        add_last_set_function :except, members
        self
      end

      def nonempty
        validate_current_set
        add_current_set_function :nonempty
        self
      end

      def distinct
        validate_current_set
        add_current_set_function :distinct
        self
      end

      def filter(condition, options = {})
        validate_current_set
        add_current_set_function :filter, condition, options[:as]
        self
      end

      def filter_last(condition, options = {})
        validate_current_set
        add_last_set_function :filter, condition, options[:as]
        self
      end

      def filter_nonempty
        validate_current_set
        filter('NOT ISEMPTY(S.CURRENT)', as: 'S')
      end

      def generate(*axis_members)
        validate_current_set
        all = if axis_members.last == :all
          axis_members.pop
          'ALL'
        end
        raise ArgumentError, "specify set of members for generate method" if axis_members.empty?
        members = axis_members.length == 1 && axis_members[0].is_a?(Array) ? axis_members[0] : axis_members
        add_current_set_function :generate, members, all
        self
      end

      VALID_ORDERS = %w(ASC BASC DESC BDESC).freeze

      def order(expression, direction)
        validate_current_set
        direction = direction.to_s.upcase
        raise ArgumentError, "invalid order direction #{direction.inspect}," \
          " should be one of #{VALID_ORDERS.inspect[1..-2]}" unless VALID_ORDERS.include?(direction)
        add_current_set_function :order, expression, direction
        self
      end

      %w(top bottom).each do |extreme|
        class_eval <<~RUBY, __FILE__, __LINE__ + 1
          def #{extreme}_count(count, expression = nil)
            validate_current_set
            add_current_set_function :#{extreme}_count, count, expression
            self
          end
        RUBY

        %w(percent sum).each do |extreme_name|
          class_eval <<~RUBY, __FILE__, __LINE__ + 1
            def #{extreme}_#{extreme_name}(value, expression)
              validate_current_set
              add_current_set_function :#{extreme}_#{extreme_name}, value, expression
              self
            end
          RUBY
        end
      end

      def hierarchize(order = nil, all = nil)
        validate_current_set
        order = order && order.to_s.upcase
        raise ArgumentError, "invalid hierarchize order #{order.inspect}" unless order.nil? || order == 'POST'
        if all.nil?
          add_last_set_function :hierarchize, order
        else
          add_current_set_function :hierarchize, order
        end
        self
      end

      def hierarchize_all(order = nil)
        validate_current_set
        hierarchize(order, :all)
      end

      # Add new WHERE condition to query or return array of existing conditions if no arguments specified
      def where(*members)
        if members.empty?
          @where
        else
          @current_set = @where
          if members.length == 1 && members[0].is_a?(Array)
            @where.concat(members[0])
          else
            @where.concat(members)
          end
          self
        end
      end

      # Add definition of calculated member
      def with_member(member_name)
        @with << [:member, member_name]
        @current_set = nil
        self
      end

      # Add definition of named_set
      def with_set(set_name)
        @current_set = []
        @with << [:set, set_name, @current_set]
        self
      end

      # Return array of member and set definitions
      def with
        @with
      end

      # Add definition to calculated member or to named set
      def as(*params)
        # Definition of named set
        if @current_set
          if params.empty?
            raise ArgumentError, "named set cannot be empty"
          else
            raise ArgumentError, "cannot use 'as' method before with_set method" unless @current_set.empty?
            if params.length == 1 && params[0].is_a?(Array)
              @current_set.concat(params[0])
            else
              @current_set.concat(params)
            end
          end
        # Definition of calculated member
        else
          member_definition = @with.last
          if params.last.is_a?(Hash)
            options = params.pop
            # If formatter does not include . then it should be ruby formatter name
            if (formatter = options[:cell_formatter]) && !formatter.include?('.')
              options = options.merge(:cell_formatter => Mondrian::OLAP::Schema::CellFormatter.new(formatter).class_name)
            end
          else
            options = nil
          end
          raise ArgumentError, "cannot use 'as' method before with_member method" unless member_definition &&
            member_definition[0] == :member && member_definition.length == 2
          raise ArgumentError, "calculated member definition should be single expression" unless params.length == 1
          member_definition << params[0]
          member_definition << options if options
        end
        self
      end

      def to_mdx
        mdx = +''
        mdx << "WITH #{with_to_mdx}\n" unless @with.empty?
        mdx << "SELECT #{axis_to_mdx}\n"
        mdx << "FROM #{from_to_mdx}"
        mdx << "\nWHERE #{where_to_mdx}" unless @where.empty?
        mdx
      end

      def execute(parameters = {})
        @connection.execute to_mdx, parameters
      end

      def execute_drill_through(options = {})
        drill_through_mdx = +"DRILLTHROUGH "
        drill_through_mdx << "MAXROWS #{options[:max_rows]} " if options[:max_rows]
        drill_through_mdx << to_mdx
        drill_through_mdx << " RETURN #{Array(options[:return]).join(',')}" if options[:return]
        @connection.execute_drill_through drill_through_mdx
      end

      private

      def validate_current_set
        unless @current_set
          method_name = caller_locations(1,1).first&.label
          raise ArgumentError, "cannot use #{method_name} method before axis or with_set method"
        end
      end

      def add_current_set_function(function_name, *args)
        remove_last_nil_arg(args)
        @current_set.replace [function_name, @current_set.clone, *args]
      end

      def add_last_set_function(function_name, *args)
        remove_last_nil_arg(args)
        if current_set_crossjoin?
          @current_set[2] = [function_name, @current_set[2], *args]
        else
          add_current_set_function function_name, *args
        end
      end

      def remove_last_nil_arg(args)
        args.pop if args.length > 0 && args.last.nil?
      end

      CROSSJOIN_FUNCTIONS = [:crossjoin, :nonempty_crossjoin].freeze

      def current_set_crossjoin?
        CROSSJOIN_FUNCTIONS.include?(@current_set&.first)
      end

      def with_to_mdx
        @with.map do |definition|
          case definition[0]
          when :member
            member_name = definition[1]
            expression = definition[2]
            options = definition[3]
            options_string = +''
            options && options.each do |option, value|
              option_name = case option
              when :caption
                '$caption'
              else
                option.to_s.upcase
              end
              options_string << ", #{option_name} = #{quote_value(value)}"
            end
            "MEMBER #{member_name} AS #{quote_value(expression)}#{options_string}"
          when :set
            set_name = definition[1]
            set_members = definition[2]
            "SET #{set_name} AS #{quote_value(members_to_mdx(set_members))}"
          end
        end.join("\n")
      end

      def axis_to_mdx
        mdx = +''
        @axes.each_with_index do |axis_members, i|
          axis_name = AXIS_ALIASES[i] ? AXIS_ALIASES[i].upcase : "AXIS(#{i})"
          mdx << ",\n" if i > 0
          mdx << members_to_mdx(axis_members) << " ON " << axis_name
        end
        mdx
      end

      MDX_FUNCTIONS = {
        top_count: 'TOPCOUNT',
        top_percent: 'TOPPERCENT',
        top_sum: 'TOPSUM',
        bottom_count: 'BOTTOMCOUNT',
        bottom_percent: 'BOTTOMPERCENT',
        bottom_sum: 'BOTTOMSUM'
      }.freeze

      def members_to_mdx(members)
        members ||= []
        # If only one member which does not end with ] or .Item(...) or Default...Member
        # Then assume it is expression which returns set.
        if members.length == 1 && members[0] !~ /(\]|\.Item\(\d+\)|\.Default\w*Member)\z/i
          members[0]
        elsif members[0].is_a?(Symbol)
          case members[0]
          when :crossjoin
            "CROSSJOIN(#{members_to_mdx(members[1])}, #{members_to_mdx(members[2])})"
          when :nonempty_crossjoin
            "NONEMPTYCROSSJOIN(#{members_to_mdx(members[1])}, #{members_to_mdx(members[2])})"
          when :except
            "EXCEPT(#{members_to_mdx(members[1])}, #{members_to_mdx(members[2])})"
          when :nonempty
            "NON EMPTY #{members_to_mdx(members[1])}"
          when :distinct
            "DISTINCT(#{members_to_mdx(members[1])})"
          when :filter
            as_alias = members[3] ? " AS #{members[3]}" : nil
            "FILTER(#{members_to_mdx(members[1])}#{as_alias}, #{members[2]})"
          when :generate
            "GENERATE(#{members_to_mdx(members[1])}, #{members_to_mdx(members[2])}#{members[3] && ", #{members[3]}"})"
          when :order
            "ORDER(#{members_to_mdx(members[1])}, #{expression_to_mdx(members[2])}, #{members[3]})"
          when :top_count, :bottom_count
            mdx = +"#{MDX_FUNCTIONS[members[0]]}(#{members_to_mdx(members[1])}, #{members[2]}"
            mdx << (members[3] ? ", #{expression_to_mdx(members[3])})" : ")")
          when :top_percent, :top_sum, :bottom_percent, :bottom_sum
            "#{MDX_FUNCTIONS[members[0]]}(#{members_to_mdx(members[1])}, #{members[2]}, #{expression_to_mdx(members[3])})"
          when :hierarchize
            "HIERARCHIZE(#{members_to_mdx(members[1])}#{members[2] && ", #{members[2]}"})"
          else
            raise ArgumentError, "Cannot generate MDX for invalid set operation #{members[0].inspect}"
          end
        else
          "{#{members.join(', ')}}"
        end
      end

      def expression_to_mdx(expression)
        expression.is_a?(Array) ? "(#{expression.join(', ')})" : expression
      end

      def from_to_mdx
        "[#{@cube_name}]"
      end

      def where_to_mdx
        # Generate set MDX expression
        if @where[0].is_a?(Symbol) ||
            @where.length > 1 && @where.map{|full_name| extract_dimension_name(full_name)}.uniq.length == 1
          members_to_mdx(@where)
        # Generate tuple MDX expression
        else
          where_to_mdx_tuple
        end
      end

      def where_to_mdx_tuple
        mdx = +'('
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

      def extract_dimension_name(full_name)
        # "[Foo [Bar]]].[Baz]" =>  "Foo [Bar]"
        if full_name
          full_name.gsub(/\A\[|\]\z/, '').split('].[').first&.gsub(']]', ']')
        end
      end
    end
  end
end
