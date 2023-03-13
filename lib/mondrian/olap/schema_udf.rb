require 'jruby/core_ext'

module Mondrian
  module OLAP
    class Schema < SchemaElement

      def user_defined_cell_formatter(name, &block)
        CellFormatter.new(name, &block)
      end

      module ScriptElements
        private

        def ruby(*options, &block)
          udf_class_name = if options.include?(:shared)
            "#{name.capitalize}Udf"
          end
          if udf_class_name && self.class.const_defined?(udf_class_name)
            udf_class = self.class.const_get(udf_class_name)
          else
            udf_class = Class.new(RubyUdfBase)
            self.class.const_set(udf_class_name, udf_class) if udf_class_name
          end
          udf_class.function_name = name
          udf_class.class_eval(&block)
          udf_java_class = udf_class.become_java!(false)

          class_name udf_java_class.getName
        end

        def ruby_formatter(options, interface_class, method, signature, &block)
          formatter_class_name = if options.include?(:shared) && @attributes[:name]
            ruby_formatter_name_to_class_name(@attributes[:name])
          end
          if formatter_class_name && self.class.const_defined?(formatter_class_name)
            formatter_class = self.class.const_get(formatter_class_name)
          else
            formatter_class = Class.new
            self.class.const_set(formatter_class_name, formatter_class) if formatter_class_name
          end

          formatter_class.class_eval do
            include interface_class
            define_method method, &block
            add_method_signature(method, signature)
          end
          formatter_java_class = formatter_class.become_java!(false)
          class_name formatter_java_class.getName
        end

        def ruby_formatter_name_to_class_name(name)
          # upcase just first character
          "#{name.sub(/\A./){|m| m.upcase}}Udf"
        end

        def ruby_formatter_java_class_name(name)
          "rubyobj.#{self.class.name.gsub('::', '.')}.#{ruby_formatter_name_to_class_name(name)}"
        end

      end

      class UserDefinedFunction < SchemaElement
        include ScriptElements

        attributes :name, # Name with which the user-defined function will be referenced in MDX expressions.
          # Name of the class which implements this user-defined function.
          # Must implement the mondrian.spi.UserDefinedFunction interface.
          :class_name
        elements :script

        class RubyUdfBase
          include Java::mondrian.spi.UserDefinedFunction
          def self.function_name=(name); @function_name = name; end
          def self.function_name; @function_name; end

          def getName
            self.class.function_name
          end
          add_method_signature("getName", [java.lang.String])

          def getDescription
            getName
          end
          add_method_signature("getDescription", [java.lang.String])

          def self.parameters(*types)
            if types.empty?
              @parameters || []
            else
              @parameters = types.map{|type| stringified_type(type)}
            end
          end

          def self.returns(type = nil)
            if type
              @returns = stringified_type(type)
            else
              @returns || 'Scalar'
            end
          end

          VALID_SYNTAX_TYPES = %w(Function Property Method)
          def self.syntax(type = nil)
            if type
              type = stringify(type)
              raise ArgumentError, "invalid user defined function type #{type.inspect}" unless VALID_SYNTAX_TYPES.include? type
              @syntax = type
            else
              @syntax || 'Function'
            end
          end

          def getSyntax
            Java::mondrian.olap.Syntax.const_get self.class.syntax
          end
          add_method_signature("getSyntax", [Java::mondrian.olap.Syntax])

          UDF_SCALAR_TYPES = {
            'Numeric' => Java::mondrian.olap.type.NumericType,
            'String' => Java::mondrian.olap.type.StringType,
            'Boolean' => Java::mondrian.olap.type.BooleanType,
            'DateTime' => Java::mondrian.olap.type.DateTimeType,
            'Decimal' => Java::mondrian.olap.type.DecimalType,
            'Scalar' => Java::mondrian.olap.type.ScalarType
          }
          UDF_OTHER_TYPES = {
            'Member' => Java::mondrian.olap.type.MemberType::Unknown,
            'Tuple' => Java::mondrian.olap.type.TupleType.new([].to_java(Java::mondrian.olap.type.Type)),
            'Hierarchy' => Java::mondrian.olap.type.HierarchyType.new(nil, nil),
            'Level' => Java::mondrian.olap.type.LevelType::Unknown
          }
          UDF_OTHER_TYPES['Set'] = UDF_OTHER_TYPES['MemberSet'] = Java::mondrian.olap.type.SetType.new(UDF_OTHER_TYPES['Member'])
          UDF_OTHER_TYPES['TupleSet'] = Java::mondrian.olap.type.SetType.new(UDF_OTHER_TYPES['Tuple'])

          def getParameterTypes
            @parameterTypes ||= self.class.parameters.map{|p| get_java_type(p)}
          end
          class_loader = JRuby.runtime.jruby_class_loader
          type_array_class = java.lang.Class.forName "[Lmondrian.olap.type.Type;", true, class_loader
          add_method_signature("getParameterTypes", [type_array_class])

          def getReturnType(parameterTypes)
            @returnType ||= get_java_type self.class.returns
          end
          add_method_signature("getReturnType", [Java::mondrian.olap.type.Type, type_array_class])

          def getReservedWords
            nil
          end
          string_array_class = java.lang.Class.forName "[Ljava.lang.String;", true, class_loader
          add_method_signature("getReservedWords", [string_array_class])

          def execute(evaluator, arguments)
            values = []
            self.class.parameters.each_with_index do |p, i|
              value = UDF_SCALAR_TYPES[p] ? arguments[i].evaluateScalar(evaluator) : arguments[i].evaluate(evaluator)
              values << value
            end
            call_with_evaluator(evaluator, *values)
          end
          arguments_array_class = java.lang.Class.forName "[Lmondrian.spi.UserDefinedFunction$Argument;", true, class_loader
          add_method_signature("execute", [java.lang.Object, Java::mondrian.olap.Evaluator, arguments_array_class])

          # Override this method if evaluator is needed
          def call_with_evaluator(evaluator, *values)
            call(*values)
          end

          private

          def get_java_type(type)
            if type_class = UDF_SCALAR_TYPES[type]
              type_class.new
            else
              UDF_OTHER_TYPES[type]
            end
          end

          def self.stringified_type(type)
            type_as_string = stringify(type)
            if UDF_SCALAR_TYPES[type_as_string] || UDF_OTHER_TYPES[type_as_string]
              type_as_string
            else
              raise ArgumentError, "Invalid user defined function type #{type.inspect}"
            end
          end

          def self.stringify(arg)
            arg = arg.to_s.split('_').map{|s| s.capitalize}.join if arg.is_a? Symbol
            arg
          end
        end

        def ruby(*options, &block)
          udf_class_name = if options.include?(:shared)
            "#{name.capitalize}Udf"
          end
          if udf_class_name && self.class.const_defined?(udf_class_name)
            udf_class = self.class.const_get(udf_class_name)
          else
            udf_class = Class.new(RubyUdfBase)
            self.class.const_set(udf_class_name, udf_class) if udf_class_name
          end
          udf_class.function_name = name
          udf_class.class_eval(&block)
          udf_java_class = udf_class.become_java!(false)

          class_name udf_java_class.getName
        end
      end

      class Script < SchemaElement
        attributes :language
        content :text
      end

      class CellFormatter < SchemaElement
        include ScriptElements
        # Name of a formatter class for the appropriate cell being displayed.
        # The class must implement the mondrian.olap.CellFormatter interface.
        attributes :class_name
        elements :script

        def initialize(name = nil, attributes = {}, parent = nil, &block)
          super
          if name && !attributes[:class_name] && !block_given?
            # use shared ruby implementation
            @attributes[:class_name] = ruby_formatter_java_class_name(name)
            @attributes.delete(:name)
          end
        end

        def ruby(*options, &block)
          ruby_formatter(options, Java::mondrian.spi.CellFormatter, 'formatCell', [java.lang.String, java.lang.Object], &block)
        end
      end

      class MemberFormatter < SchemaElement
        include ScriptElements
        attributes :class_name
        elements :script

        def ruby(*options, &block)
          ruby_formatter(options, Java::mondrian.spi.MemberFormatter, 'formatMember',
            [java.lang.String, Java::mondrian.olap.Member], &block)
        end
      end

      class PropertyFormatter < SchemaElement
        include ScriptElements
        attributes :class_name
        elements :script

        def ruby(*options, &block)
          ruby_formatter(options, Java::mondrian.spi.PropertyFormatter, 'formatProperty',
            [java.lang.String, Java::mondrian.olap.Member, java.lang.String, java.lang.Object], &block)
        end
      end

    end
  end
end
