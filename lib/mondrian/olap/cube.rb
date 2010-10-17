module Mondrian
  module OLAP
    class Cube
      def self.get(connection, name)
        if raw_cube = connection.raw_schema.lookupCube(name)
          Cube.new(connection, raw_cube)
        end
      end

      def initialize(connection, raw_cube)
        @connection = connection
        @raw_cube = raw_cube
      end

      def dimensions
        @dimenstions ||= @raw_cube.getDimensions.map{|d| Dimension.new(@connection, d)}
      end

      def dimension_names
        dimensions.map{|d| d.name}
      end

      def dimension(name)
        dimensions.detect{|d| d.name == name}
      end
    end

    class Dimension
      def initialize(connection, raw_dimension)
        @connection = connection
        @raw_dimension = raw_dimension
      end

      def name
        @name ||= @raw_dimension.getName
      end

      def hierarchies
        @hierarchies ||= @raw_dimension.getHierarchies.map{|h| Hierarchy.new(@connection, h)}
      end

      def hierarchy_names
        hierarchies.map{|h| h.name}
      end

      def hierarchy(name = nil)
        name ||= self.name
        hierarchies.detect{|h| h.name == name}
      end

      def measures?
        @raw_dimension.isMeasures
      end

      def dimension_type
        case @raw_dimension.getDimensionType
        when Java::mondrian.olap.DimensionType::StandardDimension
          :standard
        when Java::mondrian.olap.DimensionType::TimeDimension
          :time
        when Java::mondrian.olap.DimensionType::MeasuresDimension
          :measures
        end
      end
    end

    class Hierarchy
      def initialize(connection, raw_hierarchy)
        @connection = connection
        @raw_hierarchy = raw_hierarchy
      end

      def name
        @name ||= @raw_hierarchy.getName
      end

      def level_names
        @raw_hierarchy.getLevels.map{|l| l.getName}
      end

      def has_all?
        @raw_hierarchy.hasAll
      end

      def all_member_name
        has_all? ? @raw_hierarchy.getAllMember.getName : nil
      end

      def root_member_names
        @connection.raw_schema_reader.getHierarchyRootMembers(@raw_hierarchy).map{|m| m.getName}
      end

      def child_names(*parent_member_names)
        parent_member = if parent_member_names.empty?
          return root_member_names unless has_all?
          @raw_hierarchy.getAllMember
        else
          @connection.raw_schema_reader.lookupCompound(@raw_hierarchy,
            Java::mondrian.olap.Id::Segment.toList(*parent_member_names),
            false, Java::mondrian.olap.Category::Member)
        end
        parent_member && @connection.raw_schema_reader.getMemberChildren(parent_member).map{|m| m.getName}
      end
    end
  end
end