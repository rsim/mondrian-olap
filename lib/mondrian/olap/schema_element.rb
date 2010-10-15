require 'nokogiri'

module Mondrian
  module OLAP
    class SchemaElement
      def initialize(name = nil, attributes = {}, &block)
        # if just attributes hash provided
        if name.is_a?(Hash) && attributes == {}
          attributes = name
          name = nil
        end
        @attributes = {}
        @attributes[:name] = name if name
        @attributes.merge!(attributes)
        self.class.elements.each do |element|
          instance_variable_set("@#{pluralize(element)}", [])
        end
        instance_eval &block if block
      end

      def self.attributes(*names)
        names.each do |name|
          class_eval <<-RUBY, __FILE__, __LINE__ + 1
            def #{name}(*args)
              if args.empty?
                @attributes[:#{name}]
              elsif args.size == 1
                @attributes[:#{name}] = args[0]
              else
                raise ArgumentError, "too many arguments"
              end
            end
          RUBY
        end
      end

      def self.elements(*names)
        return @elements || [] if names.empty?

        @elements ||= []
        @elements.concat(names)

        names.each do |name|
          attr_reader pluralize(name).to_sym
          class_eval <<-RUBY, __FILE__, __LINE__ + 1
            def #{name}(name=nil, attributes = {}, &block)
              @#{pluralize(name)} << Schema::#{camel_case(name)}.new(name, attributes, &block)
            end
          RUBY
        end
      end

      def to_xml
        Nokogiri::XML::Builder.new do |xml|
          add_to_xml(xml)
        end.to_xml
      end

      protected

      def add_to_xml(xml)
        xml.send(tag_name(self.class.name), camel_case_attributes) do
          self.class.elements.each do |element|
            instance_variable_get("@#{pluralize(element)}").each {|item| item.add_to_xml(xml)}
          end
        end
      end

      private


      def camel_case_attributes
        hash = {}
        @attributes.each do |attr, value|
          hash[
            attr.to_s.gsub(/_([^_]+)/){|m| $1.capitalize}
          ] = value
        end
        hash
      end

      def self.pluralize(string)
        string = string.to_s
        case string
        when /^(.*)y$/
          "#{$1}ies"
        else
          "#{string}s"
        end
      end

      def pluralize(string)
        self.class.pluralize(string)
      end

      def self.camel_case(string)
        string.to_s.split('_').map{|s| s.capitalize}.join('')
      end

      def camel_case(string)
        self.class.camel_case(string)
      end

      def tag_name(string)
        string.split('::').last << '_'
      end
    end

  end
end
