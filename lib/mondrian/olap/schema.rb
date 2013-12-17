require 'mondrian/olap/schema_element'

module Mondrian
  module OLAP
    # See http://mondrian.pentaho.com/documentation/schema.php for more detailed description
    # of Mondrian Schema elements.
    class Schema < SchemaElement
      def initialize(name = nil, attributes = {}, parent = nil, &block)
        name, attributes = self.class.pre_process_arguments(name, attributes)
        pre_process_attributes(attributes)
        super(name, attributes, parent, &block)
      end

      def self.define(name = nil, attributes = {}, &block)
        name, attributes = pre_process_arguments(name, attributes)
        new(name || 'default', attributes, &block)
      end

      def define(name = nil, attributes = {}, &block)
        name, attributes = self.class.pre_process_arguments(name, attributes)
        pre_process_attributes(attributes)
        @attributes[:name] = name || @attributes[:name] || 'default' # otherwise connection with empty name fails
        instance_eval(&block) if block
        self
      end

      def include_schema(shared_schema)
        shared_schema.class.elements.each do |element|
          instance_variable_get("@#{pluralize(element)}").concat shared_schema.send(pluralize(element))
        end
      end

      private

      def self.pre_process_arguments(name, attributes)
        # if is called just with attributes hash and without name
        if name.is_a?(Hash) && attributes.empty?
          attributes = name
          name = nil
        end
        [name, attributes]
      end

      def pre_process_attributes(attributes)
        unless attributes[:upcase_data_dictionary].nil?
          @upcase_data_dictionary = attributes.delete(:upcase_data_dictionary)
        end
      end

      public

      attributes :name, :description, :measures_caption
      elements :annotations, :dimension, :cube, :virtual_cube, :role, :user_defined_function

      class Cube < SchemaElement
        attributes :name, :description, :caption,
          # The name of the measure that would be taken as the default measure of the cube.
          :default_measure,
          # Should the Fact table data for this Cube be cached by Mondrian or not.
          # The default action is to cache the data.
          :cache,
          # Whether element is enabled - if true, then the Cube is realized otherwise it is ignored.
          :enabled
        # always render xml fragment as the first element in XML output (by default it is added at the end)
        elements :annotations, :xml, :table, :view, :dimension_usage, :dimension, :measure, :calculated_member
      end

      class Table < SchemaElement
        attributes :name, :schema, # Optional qualifier for table.
          # Alias to be used with this table when it is used to form queries.
          # If not specified, defaults to the table name, but in any case, must be unique within the schema.
          # (You can use the same table in different hierarchies, but it must have different aliases.)
          :alias
        data_dictionary_names :name, :schema, :alias # values in XML will be uppercased when using Oracle driver
        elements :agg_exclude, :agg_name, :agg_pattern, :sql
      end

      class View < SchemaElement
        attributes :alias
        data_dictionary_names :alias
        # Defines a "table" using SQL query which can have different variants for different underlying databases
        elements :sql
      end

      class Dimension < SchemaElement
        attributes :name, :description, :caption,
          # The dimension's type may be one of "Standard" or "Time".
          # A time dimension will allow the use of the MDX time functions (WTD, YTD, QTD, etc.).
          # Use a standard dimension if the dimension is not a time-related dimension.
          # The default value is "Standard".
          :type,
          # The name of the column in the fact table which joins to the leaf level of this dimension.
          # Required in a private Dimension or a DimensionUsage, but not in a public Dimension.
          :foreign_key
        data_dictionary_names :foreign_key # values in XML will be uppercased when using Oracle driver
        elements :annotations, :hierarchy
      end

      class DimensionUsage < SchemaElement
        attributes :name,
          # Name of the source dimension. Must be a dimension in this schema. Case-sensitive.
          :source,
          # Name of the level to join to. If not specified, joins to the lowest level of the dimension.
          :level,
          # If present, then this is prepended to the Dimension column names
          # during the building of collapse dimension aggregates allowing
          # 1) different dimension usages to be disambiguated during aggregate table recognition and
          # 2) multiple shared dimensions that have common column names to be disambiguated.
          :usage_prefix,
          # The name of the column in the fact table which joins to the leaf level of this dimension.
          # Required in a private Dimension or a DimensionUsage, but not in a public Dimension.
          :foreign_key
        data_dictionary_names :usage_prefix, :foreign_key # values in XML will be uppercased when using Oracle driver

        def initialize(name = nil, attributes = {}, parent = nil)
          super
          # by default specify :source as name
          @attributes[:source] ||= name
        end
      end

      class Hierarchy < SchemaElement
        attributes :name, :description, :caption,
          # Whether this hierarchy has an 'all' member.
          :has_all,
          # Name of the 'all' member. If this attribute is not specified,
          # the all member is named 'All hierarchyName', for example, 'All Store'.
          :all_member_name,
          # A string being displayed instead as the all member's name
          :all_member_caption,
          # Name of the 'all' level. If this attribute is not specified,
          # the all member is named '(All)'.
          :all_level_name,
          # The name of the column which identifies members, and which is referenced by rows in the fact table.
          # If not specified, the key of the lowest level is used. See also Dimension foreign_key.
          :primary_key,
          # The name of the table which contains primary_key.
          # If the hierarchy has only one table, defaults to that; it is required.
          :primary_key_table,
          # Should be set to the level (if such a level exists) at which depth it is known
          # that all members have entirely unique rows, allowing SQL GROUP BY clauses to be completely eliminated from the query.
          :unique_key_level_name
        data_dictionary_names :primary_key, :primary_key_table # values in XML will be uppercased when using Oracle driver
        elements :annotations, :table, :join, :view, :property, :level

        def initialize(name = nil, attributes = {}, parent = nil)
          super
          # set :has_all => true if :all_member_name is set
          if @attributes[:has_all].nil? && @attributes[:all_member_name]
            @attributes[:has_all] = true
          end
        end
      end

      class Join < SchemaElement
        attributes :left_key, :right_key, :left_alias, :right_alias
        data_dictionary_names :left_key, :right_key, :left_alias, :right_alias # values in XML will be uppercased when using Oracle driver
        elements :table, :join
      end

      class Level < SchemaElement
        attributes :name, :description, :caption,
          # The name of the table that the column comes from.
          # If this hierarchy is based upon just one table, defaults to the name of that table;
          # otherwise, it is required.
          :table,
          # The name of the column which holds the unique identifier of this level.
          :column,
          # The name of the column which holds the user identifier of this level.
          :name_column,
          # The name of the column which holds member ordinals.
          # If this column is not specified, the key column is used for ordering.
          :ordinal_column,
          # The name of the column which references the parent member in a parent-child hierarchy.
          :parent_column,
          # The name of the column which holds the caption for members
          :caption_column,
          # Value which identifies null parents in a parent-child hierarchy.
          # Typical values are 'NULL' and '0'.
          :null_parent_value,
          # Indicates the type of this level's key column:
          # String, Numeric, Integer, Boolean, Date, Time or Timestamp.
          # When generating SQL statements, Mondrian encloses values for String columns in quotation marks,
          # but leaves values for Integer and Numeric columns un-quoted.
          # Date, Time, and Timestamp values are quoted according to the SQL dialect.
          # For a SQL-compliant dialect, the values appear prefixed by their typename,
          # for example, "DATE '2006-06-01'".
          # Default value: 'String'
          :type,
          # Whether members are unique across all parents.
          # For example, zipcodes are unique across all states.
          # The first level's members are always unique.
          # Default value: false
          :unique_members,
          # Whether this is a regular or a time-related level.
          # The value makes a difference to time-related functions such as YTD (year-to-date).
          # Default value: 'Regular'
          :level_type,
          # Condition which determines whether a member of this level is hidden.
          # If a hierarchy has one or more levels with hidden members,
          # then it is possible that not all leaf members are the same distance from the root,
          # and it is termed a ragged hierarchy.
          # Allowable values are: Never (a member always appears; the default);
          # IfBlankName (a member doesn't appear if its name is null, empty or all whitespace);
          # and IfParentsName (a member appears unless its name matches the parent's.
          # Default value: 'Never'
          :hide_member_if,
          # The estimated number of members in this level. Setting this property improves the performance of
          # MDSCHEMA_LEVELS, MDSCHEMA_HIERARCHIES and MDSCHEMA_DIMENSIONS XMLA requests
          :approx_row_count
        data_dictionary_names :table, :column, :name_column, :ordinal_column, :parent_column, :caption_column # values in XML will be uppercased when using Oracle driver
        elements :annotations, :key_expression, :name_expression, :ordinal_expression, :caption_expression, :member_formatter, :property

        def initialize(name = nil, attributes = {}, parent = nil)
          super
          # set :unique_members by default to true for first level and false for next levels
          if @attributes[:unique_members].nil?
            @attributes[:unique_members] = parent.levels.empty?
          end
        end
      end

      class KeyExpression < SchemaElement
        elements :sql
      end

      class NameExpression < SchemaElement
        elements :sql
      end

      class OrdinalExpression < SchemaElement
        elements :sql
      end

      class CaptionExpression < SchemaElement
        elements :sql
      end

      class Sql < SchemaElement
        def self.name
          'SQL'
        end
        attributes :dialect
        content :text
      end

      class Property < SchemaElement
        attributes :name, :description, :caption,
          :column,
          # Data type of this property: String, Numeric, Integer, Boolean, Date, Time or Timestamp.
          :type,
          # Should be set to true if the value of the property is functionally dependent on the level value.
          # This permits the associated property column to be omitted from the GROUP BY clause
          # (if the database permits columns in the SELECT that are not in the GROUP BY).
          # This can be a significant performance enhancement on some databases, such as MySQL.
          :depends_on_level_value
        data_dictionary_names :column
        elements :property_formatter
      end

      class Measure < SchemaElement
        attributes :name, :description, :caption,
          # Column which is source of this measure's values.
          # If not specified, a measure expression must be specified.
          :column,
          # The datatype of this measure: String, Numeric, Integer, Boolean, Date, Time or Timestamp.
          # The default datatype of a measure is 'Integer' if the measure's aggregator is 'Count', otherwise it is 'Numeric'.
          :datatype,
          # Aggregation function. Allowed values are "sum", "count", "min", "max", "avg", and "distinct-count".
          :aggregator,
          # Format string with which to format cells of this measure. For more details, see the mondrian.util.Format class.
          :format_string,
          # Whether this member is visible in the user-interface. Default true.
          :visible
        data_dictionary_names :column # values in XML will be uppercased when using Oracle driver
        elements :annotations, :measure_expression, :cell_formatter

        def initialize(name = nil, attributes = {}, parent = nil)
          super
          # by default set aggregator to sum
          @attributes[:aggregator] ||= 'sum'
        end
      end

      class MeasureExpression < SchemaElement
        elements :sql
      end

      class CalculatedMember < SchemaElement
        attributes :name, :description, :caption,
          # Name of the dimension which this member belongs to. Cannot be used if :hieararchy is specified.
          :dimension,
          # Full unique name of the hierarchy that this member belongs to.
          :hierarchy,
          # Fully-qualified name of the parent member. If not specified, the member will be at the lowest level (besides the 'all' level) in the hierarchy.
          :parent,
          # Format string with which to format cells of this measure. For more details, see the mondrian.util.Format class.
          :format_string,
          # Whether this member is visible in the user-interface. Default true.
          :visible
        elements :annotations, :formula, :calculated_member_property, :cell_formatter
      end

      class Formula < SchemaElement
        content :text
      end

      class CalculatedMemberProperty < SchemaElement
        attributes :name, :description, :caption,
          # MDX expression which defines the value of this property. If the expression is a constant string, you could enclose it in quotes,
          # or just specify the 'value' attribute instead.
          :expression,
          # Value of this property. If the value is not constant, specify the 'expression' attribute instead.
          :value
      end

      class VirtualCube < SchemaElement
        attributes :name, :description, :caption,
          # The name of the measure that would be taken as the default measure of the cube.
          :default_measure,
          # Whether element is enabled - if true, then the VirtualCube is realized otherwise it is ignored.
          :enabled
        elements :annotations, :virtual_cube_dimension, :virtual_cube_measure, :calculated_member
      end

      class VirtualCubeDimension < SchemaElement
        attributes :name,
          # Name of the cube which the dimension belongs to, or unspecified if the dimension is shared
          :cube_name
      end

      class VirtualCubeMeasure < SchemaElement
        attributes :name,
          # Name of the cube which the measure belongs to.
          :cube_name,
          # Whether this member is visible in the user-interface. Default true.
          :visible
        elements :annotations
      end

      class AggName < SchemaElement
        attributes :name
        data_dictionary_names :name
        elements :agg_fact_count, :agg_measure, :agg_level, :agg_foreign_key
      end

      class AggFactCount < SchemaElement
        attributes :column
        data_dictionary_names :column
      end

      class AggMeasure < SchemaElement
        attributes :name, :column
        data_dictionary_names :column
      end

      class AggLevel < SchemaElement
        attributes :name, :column
        data_dictionary_names :column
      end

      class AggForeignKey < SchemaElement
        attributes :fact_column, :agg_column
        data_dictionary_names :fact_column, :agg_column
      end

      class AggIgnoreColumn < SchemaElement
        attributes :column
        data_dictionary_names :column
      end

      class AggPattern < SchemaElement
        attributes :pattern
        data_dictionary_names :pattern
        elements :agg_fact_count, :agg_measure, :agg_level, :agg_foreign_key, :agg_exclude
      end

      class AggExclude < SchemaElement
        attributes :name, :pattern, :ignorecase
        data_dictionary_names :name, :pattern
      end

      class Role < SchemaElement
        attributes :name
        elements :schema_grant, :union
      end

      class SchemaGrant < SchemaElement
        # access may be "all", "all_dimensions", "custom" or "none".
        # If access is "all_dimensions", the role has access to all dimensions but still needs explicit access to cubes.
        # If access is "custom", no access will be inherited by cubes for which no explicit rule is set.
        # If access is "all_dimensions", an implicut access is given to all dimensions of the schema's cubes,
        # provided the cube's access attribute is either "custom" or "all"
        attributes :access
        elements :cube_grant
      end

      class CubeGrant < SchemaElement
        # access may be "all", "custom", or "none".
        # If access is "custom", no access will be inherited by the dimensions of this cube,
        # unless the parent SchemaGrant is set to "all_dimensions"
        attributes :access,
          # The unique name of the cube
          :cube
        elements :dimension_grant, :hierarchy_grant
      end

      class DimensionGrant < SchemaElement
        # access may be "all", "custom" or "none".
        # Note that a role is implicitly given access to a dimension when it is given "all" acess to a cube.
        # If access is "custom", no access will be inherited by the hierarchies of this dimension.
        # If the parent schema access is "all_dimensions", this timension will inherit access "all".
        # See also the "all_dimensions" option of the "SchemaGrant" element.
        attributes :access,
          # The unique name of the dimension
          :dimension
      end

      class HierarchyGrant < SchemaElement
        # access may be "all", "custom" or "none".
        # If access is "custom", you may also specify the attributes :top_level, :bottom_level, and the member grants.
        # If access is "custom", the child levels of this hierarchy will not inherit access rights from this hierarchy,
        # should there be no explicit rules defined for the said child level.
        attributes :access,
          # The unique name of the hierarchy
          :hierarchy,
          # Unique name of the highest level of the hierarchy from which this role is allowed to see members.
          # May only be specified if the HierarchyGrant.access is "custom".
          # If not specified, role can see members up to the top level.
          :top_level,
          # Unique name of the lowest level of the hierarchy from which this role is allowed to see members.
          # May only be specified if the HierarchyGrant.access is "custom".
          # If not specified, role can see members down to the leaf level.
          :bottom_level,
          # Policy which determines how cell values are calculated if not all of the children of the current cell
          # are visible to the current role.
          # Allowable values are "full" (the default), "partial", and "hidden".
          :rollup_policy
        elements :member_grant
      end

      class MemberGrant < SchemaElement
        # The children of this member inherit that access.
        # You can implicitly see a member if you can see any of its children.
        attributes :access,
          # The unique name of the member
          :member
      end

      class Union < SchemaElement
        elements :role_usage
      end

      class RoleUsage < SchemaElement
        attributes :role_name
      end

      class Annotations < SchemaElement
        elements :annotation
        def initialize(name = nil, attributes = {}, parent = nil, &block)
          if name.is_a?(Hash)
            attributes = name
            name = nil
          end
          if block_given?
            super(name, attributes, parent, &block)
          else
            super(nil, {}, parent) do
              attributes.each do |key, value|
                annotation key.to_s, value.to_s
              end
            end
          end
        end
      end

      class Annotation < SchemaElement
        content :text
      end
    end
  end
end
