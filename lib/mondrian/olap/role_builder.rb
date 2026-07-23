# frozen_string_literal: true

module Mondrian
  module OLAP
    # Builds an immutable Mondrian +Role+ dynamically against a live schema, without
    # defining it in the schema XML, so the host application can construct a role per
    # request from runtime data.
    #
    # Access starts fully denied; only the cubes granted with #allow_cube become
    # visible. Within an allowed cube the measures and dimensions can optionally be
    # restricted to a list, which is enough to scope a connection (for example an
    # embed or results export token) to a single report or dashboard.
    #
    #   role = connection.build_role do |builder|
    #     builder.allow_cube 'Sales',
    #       measures: ['[Measures].[Unit Sales]'], # empty => all measures of the cube
    #       dimensions: ['Time', 'Store']          # empty => all dimensions of the cube
    #   end
    #   connection.role = role
    class RoleBuilder
      MEASURES_DIMENSION = 'Measures'
      MEASURES_HIERARCHY = '[Measures]'

      ACCESS_NONE = Java::MondrianOlap::Access::NONE
      ACCESS_CUSTOM = Java::MondrianOlap::Access::CUSTOM
      ACCESS_ALL = Java::MondrianOlap::Access::ALL
      ROLLUP_FULL = Java::MondrianOlap::Role::RollupPolicy::FULL
      CATEGORY_HIERARCHY = Java::MondrianOlap::Category::Hierarchy

      def initialize(raw_schema)
        @raw_schema = raw_schema
        @raw_role = Java::MondrianOlap::RoleImpl.new
        @raw_role.grant(@raw_schema, ACCESS_NONE)
        @built = false
      end

      # Grants access to a cube. Restricts its measures and/or dimensions to the
      # listed names; an empty list leaves that facet unrestricted. Measure full
      # names must exist in the cube.
      def allow_cube(cube_name, measures: [], dimensions: [])
        cube = lookup_cube(cube_name)
        @raw_role.grant(cube, ACCESS_ALL)
        restrict_measures(cube, measures) unless measures.empty?
        restrict_dimensions(cube, dimensions) unless dimensions.empty?
        self
      end

      # Freezes the role and returns the raw Mondrian Role. Once built the role is
      # immutable and safe to share between connections that use the same schema.
      def build
        unless @built
          @raw_role.makeImmutable
          @built = true
        end
        @raw_role
      end

      private

      # Restrict the Measures hierarchy to the listed measures by granting it custom
      # access and then granting each measure member.
      def restrict_measures(cube, measure_full_names)
        measures_hierarchy = lookup_hierarchy(cube, MEASURES_HIERARCHY)
        @raw_role.grant(measures_hierarchy, ACCESS_CUSTOM, nil, nil, ROLLUP_FULL)
        reader = cube_schema_reader(cube)
        measure_full_names.each do |measure_full_name|
          member = reader.getMemberByUniqueName(parse_identifier(measure_full_name), true)
          @raw_role.grant(member, ACCESS_ALL)
        end
      end

      # Deny every dimension of the cube that is not in the allowed list. The Measures
      # dimension is left to #restrict_measures.
      def restrict_dimensions(cube, allowed_dimension_names)
        cube.getDimensions.each do |dimension|
          dimension_name = dimension.getName
          next if dimension_name == MEASURES_DIMENSION || allowed_dimension_names.include?(dimension_name)

          @raw_role.grant(dimension, ACCESS_NONE)
        end
      end

      def lookup_cube(cube_name)
        @raw_schema.lookupCube(cube_name, true)
      end

      def lookup_hierarchy(cube, hierarchy_full_name)
        cube_schema_reader(cube).lookupCompound(cube, parse_identifier(hierarchy_full_name), true, CATEGORY_HIERARCHY)
      end

      def cube_schema_reader(cube)
        (@cube_schema_readers ||= {})[cube.getName] ||= cube.getSchemaReader(nil).withLocus
      end

      def parse_identifier(identifier)
        Java::MondrianOlap::Util.parseIdentifier(identifier)
      end
    end
  end
end
