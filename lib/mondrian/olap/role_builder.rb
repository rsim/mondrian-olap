# frozen_string_literal: true

module Mondrian
  module OLAP
    # Builds an immutable Mondrian +Role+ dynamically against a live schema, without
    # defining the role in the schema XML, so the host application can construct a
    # role per request from runtime data.
    #
    # The DSL mirrors the schema data access role elements (see "Data access roles"
    # in README.md): the same element names, attributes, values and validation rules
    # are used, except that object names are looked up in the live schema immediately
    # and invalid names raise an error.
    #
    #   role = connection.build_role do
    #     schema_grant access: 'none' do
    #       cube_grant cube: 'Sales', access: 'all' do
    #         dimension_grant dimension: '[Gender]', access: 'none'
    #         hierarchy_grant hierarchy: '[Measures]', access: 'custom' do
    #           member_grant member: '[Measures].[Unit Sales]', access: 'all'
    #         end
    #         hierarchy_grant hierarchy: '[Customers]', access: 'custom',
    #                         top_level: '[Customers].[State Province]' do
    #           member_grant member: '[Customers].[USA].[CA]', access: 'all'
    #         end
    #       end
    #     end
    #   end
    #   connection.custom_role = role
    class RoleBuilder
      ACCESS = {
        'none' => Java::MondrianOlap::Access::NONE,
        'custom' => Java::MondrianOlap::Access::CUSTOM,
        'all_dimensions' => Java::MondrianOlap::Access::ALL_DIMENSIONS,
        'all' => Java::MondrianOlap::Access::ALL
      }.freeze

      # Access values allowed for each grant element, mirroring the validations of
      # schema XML role loading in mondrian.rolap.RolapSchema.
      SCHEMA_GRANT_ACCESS = ACCESS.slice('none', 'all', 'all_dimensions', 'custom').freeze
      CUBE_GRANT_ACCESS = ACCESS.slice('none', 'all', 'custom').freeze
      DIMENSION_GRANT_ACCESS = ACCESS.slice('none', 'all', 'custom').freeze
      HIERARCHY_GRANT_ACCESS = ACCESS.slice('none', 'all', 'custom').freeze
      MEMBER_GRANT_ACCESS = ACCESS.slice('none', 'all').freeze

      ROLLUP_POLICIES = {
        'full' => Java::MondrianOlap::Role::RollupPolicy::FULL,
        'partial' => Java::MondrianOlap::Role::RollupPolicy::PARTIAL,
        'hidden' => Java::MondrianOlap::Role::RollupPolicy::HIDDEN
      }.freeze

      def initialize(raw_schema)
        @raw_schema = raw_schema
        @raw_role = Java::MondrianOlap::RoleImpl.new
        @built = false
      end

      # Grants access at the schema level, the root of the grants hierarchy.
      # Nested cube grants are defined in the block.
      def schema_grant(access:, &block)
        @raw_role.grant(@raw_schema, Grant.access_value(access, SCHEMA_GRANT_ACCESS, 'schema_grant'))
        SchemaGrant.new(@raw_role, @raw_schema).instance_eval(&block) if block
        nil
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

      # Base class for nested grant elements with shared helpers.
      class Grant
        def self.access_value(access, allowed_access, element)
          allowed_access.fetch(access.to_s) do
            raise ArgumentError,
              "Bad value access='#{access}' for #{element}, allowed values are #{allowed_access.keys.join(', ')}"
          end
        end

        def initialize(raw_role)
          @raw_role = raw_role
        end

        private

        attr_reader :raw_role

        def access_value(access, allowed_access, element)
          self.class.access_value(access, allowed_access, element)
        end

        def parse_identifier(identifier)
          Java::MondrianOlap::Util.parseIdentifier(identifier)
        end
      end

      class SchemaGrant < Grant
        def initialize(raw_role, raw_schema)
          super(raw_role)
          @raw_schema = raw_schema
        end

        # Grants access to a cube looked up by name. Nested dimension and hierarchy
        # grants are defined in the block.
        def cube_grant(cube:, access:, &block)
          raw_cube = @raw_schema.lookupCube(cube, true)
          raw_role.grant(raw_cube, access_value(access, CUBE_GRANT_ACCESS, 'cube_grant'))
          CubeGrant.new(raw_role, raw_cube).instance_eval(&block) if block
          nil
        end
      end

      class CubeGrant < Grant
        CATEGORY_DIMENSION = Java::MondrianOlap::Category::Dimension
        CATEGORY_HIERARCHY = Java::MondrianOlap::Category::Hierarchy
        CATEGORY_LEVEL = Java::MondrianOlap::Category::Level

        def initialize(raw_role, raw_cube)
          super(raw_role)
          @raw_cube = raw_cube
          @raw_schema_reader = raw_cube.getSchemaReader(nil)
        end

        # Grants access to a dimension of the cube looked up by unique name
        # (for example '[Customers]').
        def dimension_grant(dimension:, access:)
          raw_dimension = lookup(CATEGORY_DIMENSION, dimension)
          raw_role.grant(raw_dimension, access_value(access, DIMENSION_GRANT_ACCESS, 'dimension_grant'))
          nil
        end

        # Grants access to a hierarchy of the cube looked up by unique name.
        # The top_level and bottom_level unique names bound the visible levels and,
        # like nested member grants, may only be used with access: 'custom'.
        # The rollup_policy ('full' by default, 'partial' or 'hidden') determines how
        # cell values are calculated when some children of a cell are not visible.
        def hierarchy_grant(hierarchy:, access:, top_level: nil, bottom_level: nil, rollup_policy: 'full', &block)
          raw_hierarchy = lookup(CATEGORY_HIERARCHY, hierarchy)
          raw_access = access_value(access, HIERARCHY_GRANT_ACCESS, 'hierarchy_grant')
          custom_access = raw_access == ACCESS['custom']
          if (top_level || bottom_level) && !custom_access
            raise ArgumentError, "top_level and bottom_level may only be specified when hierarchy_grant access='custom'"
          end

          raw_role.grant(raw_hierarchy, raw_access,
            top_level && lookup(CATEGORY_LEVEL, top_level),
            bottom_level && lookup(CATEGORY_LEVEL, bottom_level),
            rollup_policy_value(rollup_policy))
          HierarchyGrant.new(raw_role, @raw_schema_reader, raw_hierarchy, custom_access).instance_eval(&block) if block
          nil
        end

        private

        def lookup(category, unique_name)
          @raw_schema_reader.lookupCompound(@raw_cube, parse_identifier(unique_name), true, category)
        end

        def rollup_policy_value(rollup_policy)
          ROLLUP_POLICIES.fetch(rollup_policy.to_s) do
            raise ArgumentError,
              "Illegal rollup_policy value '#{rollup_policy}', allowed values are #{ROLLUP_POLICIES.keys.join(', ')}"
          end
        end
      end

      class HierarchyGrant < Grant
        def initialize(raw_role, raw_schema_reader, raw_hierarchy, custom_access)
          super(raw_role)
          @raw_schema_reader_with_locus = raw_schema_reader.withLocus
          @raw_hierarchy = raw_hierarchy
          @custom_access = custom_access
        end

        # Grants access to a member of the hierarchy looked up by unique name.
        # Children of the member inherit the access; a member is implicitly visible
        # when any of its children is visible.
        def member_grant(member:, access:)
          unless @custom_access
            raise ArgumentError, "member_grant may only be specified when hierarchy_grant access='custom'"
          end

          raw_member = @raw_schema_reader_with_locus.getMemberByUniqueName(parse_identifier(member), true)
          unless raw_member.getHierarchy == @raw_hierarchy
            raise ArgumentError, "Member '#{member}' is not in hierarchy '#{@raw_hierarchy.getUniqueName}'"
          end

          raw_role.grant(raw_member, access_value(access, MEMBER_GRANT_ACCESS, 'member_grant'))
          nil
        end
      end
    end
  end
end
