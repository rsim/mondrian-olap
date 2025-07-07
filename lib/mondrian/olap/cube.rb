require 'forwardable'

module Mondrian
  module OLAP
    module Annotated
      private

      def annotations_for(raw_element)
        @annotations ||= begin
          annotated = raw_element.unwrap(Java::MondrianOlap::Annotated.java_class)
          annotations_hash = annotated.getAnnotationMap.to_hash
          annotations_hash.each do |key, annotation|
            annotations_hash[key] = annotation.getValue
          end
          annotations_hash
        end
      end
    end

    class Cube
      extend Forwardable

      def self.get(connection, name)
        if raw_cube = connection.raw_schema.getCubes.get(name)
          Cube.new(connection, raw_cube)
        end
      end

      def initialize(connection, raw_cube)
        @connection = connection
        @raw_cube = raw_cube
        @cache_control = CacheControl.new(@connection, self)
      end

      attr_reader :connection, :raw_cube

      def name
        @name ||= @raw_cube.getName
      end

      def description
        @description ||= @raw_cube.getDescription
      end

      def caption
        @caption ||= @raw_cube.getCaption
      end

      include Annotated
      def annotations
        annotations_for(@raw_cube)
      end

      def visible?
        @raw_cube.isVisible
      end

      def mondrian_cube
        @raw_cube.unwrap(Java::MondrianOlap::Cube.java_class)
      end

      def virtual?
        if mondrian_cube.respond_to?(:isVirtual)
          mondrian_cube.isVirtual
        end
      end

      def dimensions
        @dimenstions ||= @raw_cube.getDimensions.map { |d| dimension_from_raw(d) }
      end

      def dimension_names
        dimensions.map(&:name)
      end

      def dimension(name)
        if @dimensions
          @dimensions.detect { |d| d.name == name }
        elsif raw_dimension = @raw_cube.getDimensions.detect { |d| d.getName == name }
          dimension_from_raw(raw_dimension)
        end
      end

      def hierarchies
        @hierarchies ||= @raw_cube.getHierarchies.map { |h| hierarchy_from_raw(h) }
      end

      def hierarchy_names
        hierarchies.map(&:name)
      end

      def hierarchy(name)
        if @hierarchies
          @hierarchies.detect { |h| h.name == name }
        elsif raw_hierarchy = @raw_cube.getHierarchies.detect { |h| h.getName == name }
          hierarchy_from_raw(raw_hierarchy)
        end
      end

      def query
        Query.from(@connection, name)
      end

      def member(full_name)
        segment_list = Java::OrgOlap4jMdx::IdentifierNode.parseIdentifier(full_name).getSegmentList
        raw_member = @raw_cube.lookupMember(segment_list)
        raw_member && Member.new(raw_member)
      end

      def member_by_segments(*segment_names)
        segment_list = Java::OrgOlap4jMdx::IdentifierNode.ofNames(*segment_names).getSegmentList
        raw_member = @raw_cube.lookupMember(segment_list)
        raw_member && Member.new(raw_member)
      end

      def_delegators :@cache_control, :flush_region_cache_with_segments, :flush_region_cache_with_segments
      def_delegators :@cache_control, :flush_region_cache_with_full_names, :flush_region_cache_with_full_names

      private

      def dimension_from_raw(raw_dimension)
        Dimension.new(self, raw_dimension)
      end

      def hierarchy_from_raw(raw_hierarchy)
        Hierarchy.new(dimension_from_raw(raw_hierarchy.getDimension), raw_hierarchy)
      end
    end

    class Dimension
      def initialize(cube, raw_dimension)
        @cube = cube
        @raw_dimension = raw_dimension
      end

      attr_reader :cube, :raw_dimension

      def name
        @name ||= @raw_dimension.getName
      end

      def description
        @description ||= @raw_dimension.getDescription
      end

      def caption
        @caption ||= @raw_dimension.getCaption
      end

      def full_name
        @full_name ||= @raw_dimension.getUniqueName
      end

      def hierarchies
        @hierarchies ||= @raw_dimension.getHierarchies.map { |h| Hierarchy.new(self, h) }
      end

      def hierarchy_names
        hierarchies.map(&:name)
      end

      def hierarchy(name = nil)
        name ||= self.name
        hierarchies.detect { |h| h.name == name }
      end

      def measures?
        @raw_dimension.getDimensionType == Java::OrgOlap4jMetadata::Dimension::Type::MEASURE
      end

      def dimension_type
        case @raw_dimension.getDimensionType
        when Java::OrgOlap4jMetadata::Dimension::Type::TIME
          :time
        when Java::OrgOlap4jMetadata::Dimension::Type::MEASURE
          :measures
        else
          :standard
        end
      end

      include Annotated
      def annotations
        annotations_for(@raw_dimension)
      end

      def visible?
        @raw_dimension.isVisible
      end

    end

    class Hierarchy
      def initialize(dimension, raw_hierarchy)
        @dimension = dimension
        @raw_hierarchy = raw_hierarchy
      end

      attr_reader :raw_hierarchy, :dimension

      def name
        @name ||= @raw_hierarchy.getName
      end

      def description
        @description ||= @raw_hierarchy.getDescription
      end

      def caption
        @caption ||= @raw_hierarchy.getCaption
      end

      def full_name
        @full_name ||= @raw_hierarchy.getUniqueName
      end

      def dimension_name
        @dimension.name
      end

      def levels
        @levels = @raw_hierarchy.getLevels.map { |l| Level.new(self, l) }
      end

      def level(name)
        levels.detect { |l| l.name == name }
      end

      def level_names
        levels.map(&:name)
      end

      def has_all?
        @raw_hierarchy.hasAll
      end

      def all_member_name
        has_all? ? @raw_hierarchy.getRootMembers.first.getName : nil
      end

      def all_member
        has_all? ? Member.new(@raw_hierarchy.getRootMembers.first) : nil
      end

      def root_members
        @raw_hierarchy.getRootMembers.map { |m| Member.new(m) }
      end

      def root_member_names
        @raw_hierarchy.getRootMembers.map(&:getName)
      end

      def root_member_full_names
        @raw_hierarchy.getRootMembers.map(&:getUniqueName)
      end

      def child_names(*parent_member_segment_names)
        Error.wrap_native_exception do
          parent_member = if parent_member_segment_names.empty?
            return root_member_names unless has_all?
            all_member
          else
            @dimension.cube.member_by_segments(*parent_member_segment_names)
          end
          parent_member && parent_member.children.map(&:name)
        end
      end

      include Annotated
      def annotations
        annotations_for(@raw_hierarchy)
      end

      def visible?
        @raw_hierarchy.isVisible
      end

    end

    class Level
      def initialize(hierarchy, raw_level)
        @hierarchy = hierarchy
        @raw_level = raw_level
      end

      attr_reader :raw_level

      def name
        @name ||= @raw_level.getName
      end

      def full_name
        @full_name ||= @raw_level.getUniqueName
      end

      def description
        @description ||= @raw_level.getDescription
      end

      def caption
        @caption ||= @raw_level.getCaption
      end

      def depth
        @raw_level.getDepth
      end

      def cardinality
        @cardinality = @raw_level.getCardinality
      end

      def cardinality=(value)
        mondrian_level.setApproxRowCount(value || Java::JavaLang::Integer::MIN_VALUE)
      end

      def members_count
        @members_count ||= begin
          if cardinality >= 0
            cardinality
          else
            Error.wrap_native_exception do
              @raw_level.getMembers.size
            end
          end
        end
      end

      def members
        Error.wrap_native_exception do
          @raw_level.getMembers.map { |m| Member.new(m) }
        end
      end

      def mondrian_level
        @raw_level.unwrap(Java::MondrianOlap::Level.java_class)
      end

      def child_level
        @child_level ||= begin
          raw_levels = raw_level.getHierarchy.getLevels
          next_index = raw_levels.indexOf(raw_level) + 1
          if next_index < raw_levels.size
            self.class.new(@hierarchy, raw_levels.get(next_index))
          end
        end
      end

      def parent_level
        @parent_level ||= begin
          raw_levels = raw_level.getHierarchy.getLevels
          prev_index = raw_levels.indexOf(raw_level) - 1
          if prev_index >= 0
            self.class.new(@hierarchy, raw_levels.get(prev_index))
          end
        end
      end

      def descendant_level(name)
        raw_levels = raw_level.getHierarchy.getLevels
        level_index = raw_levels.indexOf(raw_level)
        descendant_index = raw_levels.indexOfName(name)
        if descendant_index > level_index
          self.class.new(@hierarchy, raw_levels.get(descendant_index))
        end
      end

      include Annotated
      def annotations
        annotations_for(@raw_level)
      end

      def visible?
        @raw_level.isVisible
      end

    end

    class Member
      def initialize(raw_member)
        @raw_member = raw_member
      end

      attr_reader :raw_member

      def name
        @name ||= @raw_member.getName
      end

      def full_name
        @full_name ||= @raw_member.getUniqueName
      end

      def caption
        @caption ||= @raw_member.getCaption
      end

      def calculated?
        @raw_member.isCalculated
      end

      def calculated_in_query?
        @raw_member.isCalculatedInQuery
      end

      def visible?
        @raw_member.isVisible
      end

      def all_member?
        @raw_member.isAll
      end

      def drillable?
        return false if calculated?
        # @raw_member.getChildMemberCount > 0
        # This hopefully is faster than counting actual child members
        raw_level = @raw_member.getLevel
        raw_levels = raw_level.getHierarchy.getLevels
        raw_levels.indexOf(raw_level) < raw_levels.size - 1
      end

      def depth
        @raw_member.getDepth
      end

      def dimension_type
        case @raw_member.getDimension.getDimensionType
        when Java::OrgOlap4jMetadata::Dimension::Type::TIME
          :time
        when Java::OrgOlap4jMetadata::Dimension::Type::MEASURE
          :measures
        else
          :standard
        end
      end

      def children
        Error.wrap_native_exception do
          @raw_member.getChildMembers.map { |m| Member.new(m) }
        end
      end

      def children_count
        Error.wrap_native_exception do
          @raw_member.getChildMemberCount
        end
      end

      def level
        @level ||= Level.new(nil, @raw_member.getLevel)
      end

      def descendants_at_level(level)
        Error.wrap_native_exception do
          raw_level = @raw_member.getLevel
          raw_levels = raw_level.getHierarchy.getLevels
          current_level_index = raw_levels.indexOf(raw_level)
          descendants_level_index = raw_levels.indexOfName(level)

          return nil unless descendants_level_index > current_level_index

          members = [self]
          (descendants_level_index - current_level_index).times do
            members = members.map do |member|
              member.children
            end.flatten
          end
          members
        end
      end

      def property_value(name)
        if property = @raw_member.getProperties.get(name)
          @raw_member.getPropertyValue(property)
        end
      end

      def property_formatted_value(name)
        if property = @raw_member.getProperties.get(name)
          @raw_member.getPropertyFormattedValue(property)
        end
      end

      def mondrian_member
        @raw_member.unwrap(Java::MondrianOlap::Member.java_class)
      end

      include Annotated
      def annotations
        annotations_for(@raw_member)
      end

      def format_string
        format_exp = property_value('FORMAT_EXP')
        if format_exp && format_exp =~ /\A"(.*)"\z/
          format_exp = $1
        end
        if format_exp && !format_exp.empty?
          format_exp
        end
      end

      def cell_formatter_name
        if cf = cell_formatter
          cf.class.name.split('::').last.gsub(/Udf\z/, '')
        end
      end

      def cell_formatter
        if dimension_type == :measures
          cube_measure = raw_member.unwrap(Java::MondrianOlap::Member.java_class)
          if value_formatter = cube_measure.getFormatter
            f = value_formatter.java_class.declared_field('cf')
            f.accessible = true
            f.value(value_formatter)
          end
        end
      end
    end

    class CacheControl
      def initialize(connection, cube)
        @connection = connection
        @cube = cube
        @mondrian_cube = @cube.raw_cube.unwrap(Java::MondrianOlap::Cube.java_class)
        @cache_control = @connection.raw_cache_control
      end

      def flush_region_cache_with_segments(*segment_names)
        members = segment_names.map { |name| @cube.member_by_segments(*name).mondrian_member }
        flush(members)
      end

      def flush_region_cache_with_full_names(*full_names)
        members = full_names.map { |name| @cube.member(*name).mondrian_member }
        flush(members)
      end

      private

      def flush(members)
        regions = members.map do |member|
          @cache_control.create_member_region(member, true)
        end
        regions << @cache_control.create_measures_region(@mondrian_cube)
        @cache_control.flush(@cache_control.create_crossjoin_region(*regions))
      end
    end
  end
end
