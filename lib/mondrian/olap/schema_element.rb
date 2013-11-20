module Mondrian
  module OLAP
    class SchemaElement
      def initialize(name = nil, attributes = {}, parent = nil, &block)
        # if just attributes hash provided
        if name.is_a?(Hash) && attributes == {}
          attributes = name
          name = nil
        end
        @attributes = {}
        if name
          if self.class.content
            if attributes.is_a?(Hash)
              @content = name
            else
              # used for Annotation element where both name and content is given as arguments
              @attributes[:name] = name
              @content = attributes
              attributes = {}
            end
          else
            @attributes[:name] = name
          end
        end
        @attributes.merge!(attributes)
        self.class.elements.each do |element|
          instance_variable_set("@#{pluralize(element)}", [])
        end
        # extract annotations from options
        if @attributes[:annotations] && self.class.elements.include?(:annotations)
          annotations @attributes.delete(:annotations)
        end
        @xml_fragments = []
        instance_eval(&block) if block
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

      def self.data_dictionary_names(*names)
        return @data_dictionary_names || [] if names.empty?
        @data_dictionary_names ||= []
        @data_dictionary_names.concat(names)
      end

      def self.elements(*names)
        return @elements || [] if names.empty?

        @elements ||= []
        @elements.concat(names)

        names.each do |name|
          next if name == :xml
          attr_reader pluralize(name).to_sym
          class_eval <<-RUBY, __FILE__, __LINE__ + 1
            def #{name}(name=nil, attributes = {}, &block)
              @#{pluralize(name)} << Schema::#{camel_case(name)}.new(name, attributes, self, &block)
            end
          RUBY
        end
      end

      def self.content(type=nil)
        return @content if type.nil?
        @content = type
      end

      attr_reader :xml_fragments
      def xml(string)
        string = string.strip
        fragment = Nokogiri::XML::DocumentFragment.parse(string)
        raise ArgumentError, "Invalid XML fragment:\n#{string}" if fragment.children.empty?
        @xml_fragments << string
      end

      def to_xml(options={})
        options[:upcase_data_dictionary] = @upcase_data_dictionary unless @upcase_data_dictionary.nil?
        Nokogiri::XML::Builder.new(:encoding => 'UTF-8') do |xml|
          add_to_xml(xml, options)
        end.to_xml
      end

      protected

      def add_to_xml(xml, options)
        if self.class.content
          xml.send(tag_name(self.class.name), @content, xmlized_attributes(options))
        else
          xml.send(tag_name(self.class.name), xmlized_attributes(options)) do
            xml_fragments_added = false
            self.class.elements.each do |element|
              if element == :xml
                add_xml_fragments(xml)
                xml_fragments_added = true
              else
                instance_variable_get("@#{pluralize(element)}").each {|item| item.add_to_xml(xml, options)}
              end
            end
            add_xml_fragments(xml) unless xml_fragments_added
          end
        end
      end

      def add_xml_fragments(xml)
        @xml_fragments.each do |xml_fragment|
          xml.send(:insert, Nokogiri::XML::DocumentFragment.parse(xml_fragment))
        end
      end

      private

      def xmlized_attributes(options)
        # data dictionary values should be in uppercase if schema defined with :upcase_data_dictionary => true
        # or by default when using Oracle or LucidDB driver (can be overridden by :upcase_data_dictionary => false)
        upcase_attributes = if options[:upcase_data_dictionary].nil? && %w(oracle luciddb).include?(options[:driver]) ||
                            options[:upcase_data_dictionary]
          self.class.data_dictionary_names
        else
          []
        end
        hash = {}
        @attributes.each do |attr, value|
          value = value.upcase if upcase_attributes.include?(attr)
          hash[
            # camelcase attribute name
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
