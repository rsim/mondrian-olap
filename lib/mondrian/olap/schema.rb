require 'mondrian/olap/schema_element'

module Mondrian
  module OLAP
    # See http://mondrian.pentaho.com/documentation/schema.php for more detailed description
    # of Mondrian Schema elements.
    class Schema < SchemaElement
      def self.define(name = nil, attributes = {}, &block)
        new(name, attributes, &block)
      end

      def define(name = nil, &block)
        @attributes[:name] = name || 'default' # otherwise connection with empty name fails
        instance_eval &block if block
        self
      end

      attributes :description
      elements :cube

      class Cube < SchemaElement
        attributes :description,
          # The name of the measure that would be taken as the default measure of the cube.
          :default_measure,
          # Should the Fact table data for this Cube be cached by Mondrian or not.
          # The default action is to cache the data.
          :cache,
          # Whether element is enabled - if true, then the Cube is realized otherwise it is ignored.
          :enabled
        elements :table, :dimension, :measure, :calculated_member
      end

      class Table < SchemaElement
        attributes :schema, # Optional qualifier for table.
          # Alias to be used with this table when it is used to form queries.
          # If not specified, defaults to the table name, but in any case, must be unique within the schema.
          # (You can use the same table in different hierarchies, but it must have different aliases.)
          :alias
      end

      class Dimension < SchemaElement
        attributes :description,
          # The dimension's type may be one of "Standard" or "Time".
          # A time dimension will allow the use of the MDX time functions (WTD, YTD, QTD, etc.).
          # Use a standard dimension if the dimension is not a time-related dimension.
          # The default value is "Standard".
          :type,
          # The name of the column in the fact table which joins to the leaf level of this dimension.
          # Required in a private Dimension or a DimensionUsage, but not in a public Dimension.
          :foreign_key
        elements :hierarchy
      end

      class Hierarchy < SchemaElement
        attributes :description,
          # Whether this hierarchy has an 'all' member.
          :has_all,
          # Name of the 'all' member. If this attribute is not specified,
          # the all member is named 'All hierarchyName', for example, 'All Store'.
          :all_member_name,
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
        elements :table, :level
      end

      class Level < SchemaElement
        attributes :description,
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
          :hide_member_if
      end

      class Measure < SchemaElement
        attributes :description,
          # Column which is source of this measure's values.
          # If not specified, a measure expression must be specified.
          :column,
          # The datatype of this measure: String, Numeric, Integer, Boolean, Date, Time or Timestamp.
          # The default datatype of a measure is 'Integer' if the measure's aggregator is 'Count', otherwise it is 'Numeric'.
          :datatype,
          # Aggregation function. Allowed values are "sum", "count", "min", "max", "avg", and "distinct-count".
          :aggregator,
          # Format string with which to format cells of this measure. For more details, see the mondrian.util.Format class.
          :format_string
      end

      class CalculatedMember < SchemaElement
        attributes :description,
          # MDX expression which gives the value of this member. Equivalent to the Formula sub-element.
          :formula,
          # Name of the dimension which this member belongs to.
          :dimension
      end

    end
  end
end