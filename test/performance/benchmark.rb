# frozen_string_literal: true

# Ruby wrapper classes performance benchmark harness (it does not benchmark the Mondrian engine itself).
# It executes complex queries which create many OLAP elements (axis positions, members, cells)
# and measures the time spent in the Ruby wrapper methods which process them.
#
# Usage:
#   mise exec -- ruby test/performance/benchmark.rb
#
# Environment variables:
#   MONDRIAN_DRIVER  - test database driver (default: mysql)
#   BENCH_BENCHMARKS - comma-separated benchmark names to run (default: all)
#   BENCH_ITERATIONS - timed iterations per benchmark (default: benchmark specific)
#   BENCH_WARMUP     - warmup iterations per benchmark (default: benchmark specific)
#   BENCH_LABEL      - label printed in results header (default: git commit)

require_relative '../support/database_setup'
require 'digest'

BENCH_ITERATIONS = ENV['BENCH_ITERATIONS']&.to_i
BENCH_WARMUP = ENV['BENCH_WARMUP']&.to_i
BENCH_LABEL = ENV['BENCH_LABEL'] || `git rev-parse --short HEAD`.strip

# Number of positions on rows axis is CUSTOMERS_COUNT * PRODUCTS_COUNT (110 * 100 = 11_000 in the test database),
# each row position has 3 members (Customer, Product and Gender), on columns there are 5 measures.
# Therefore the large query result has 55_000 cells and 11_000 * 3 = 33_000 members on the rows axis.
LARGE_QUERY = <<~MDX
  SELECT {[Measures].[Unit Sales], [Measures].[Store Sales], [Measures].[Store Cost],
          [Measures].[Sales Count], [Measures].[Customer Count]} ON COLUMNS,
         CROSSJOIN(CROSSJOIN([Customers].[Name].Members, [Product].[Product Name].Members),
                   {[Gender].[All Gender]}) ON ROWS
  FROM [Sales]
MDX

# One axis query for drill through benchmarks (drill through of the total cell returns all sales rows).
TOTAL_QUERY = <<~MDX
  SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS
  FROM [Sales]
MDX

DRILL_THROUGH_RETURN_FIELDS = [
  '[Customers].[Name]', '[Product].[Product Name]', '[Time].[Month]', 'Name([Customers].[Name])',
  "Property([Customers].[Name], 'Gender')", '[Measures].[Unit Sales]', '[Measures].[Store Sales]'
].freeze

QUERY_BUILDER_ROW_MEMBERS = Array.new(100) { |i| "[Customers].[Name].&[#{i + 1}]" }.freeze

def measure_ms
  t0 = Java::JavaLang::System.nanoTime
  yield
  (Java::JavaLang::System.nanoTime - t0) / 1_000_000.0
end

def stats(times)
  sorted = times.sort
  {
    min: sorted.first,
    median: sorted[sorted.size / 2],
    mean: times.sum / times.size,
    max: sorted.last
  }
end

# Create a new Result wrapper around an already executed cell set to measure only the Ruby wrapper
# processing time without the query execution in the Mondrian engine.
def fresh_result(connection, result)
  Mondrian::OLAP::Result.new(connection, result.raw_cell_set)
end

def build_complex_query(connection)
  connection.from('Sales').
    with_member('[Measures].[Profit]').
      as('[Measures].[Store Sales] - [Measures].[Store Cost]', format_string: '#,##0.00').
    with_set('SelectedCustomers').as(QUERY_BUILDER_ROW_MEMBERS).
    columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]', '[Measures].[Profit]').
    rows('SelectedCustomers').crossjoin('[Product].[Product Family].Members').
      nonempty_crossjoin('[Gender].[Gender].Members').
      filter('[Measures].[Unit Sales] > 0').
      order('[Measures].[Store Sales]', :bdesc).
    where('[Time].[2010].[Q1]', '[Time].[2010].[Q2]')
end

conn = Mondrian::OLAP::Connection.create(CONNECTION_PARAMS_WITH_CATALOG)

large_result = nil
large_query_ms = measure_ms { large_result = conn.execute(LARGE_QUERY) }
total_result = conn.execute(TOTAL_QUERY)

benchmarks = {
  # Extract all cell values as nested Ruby arrays (rows of columns).
  'values' => {iterations: 30, warmup: 5,
    block: -> { fresh_result(conn, large_result).values }},

  # Extract all formatted cell values (the formatting itself is done by the Mondrian engine).
  'formatted_values' => {iterations: 30, warmup: 5,
    block: -> { fresh_result(conn, large_result).formatted_values }},

  # Extract all cell values with a different axes sequence (columns of rows).
  'values_columns_rows' => {iterations: 30, warmup: 5,
    block: -> { fresh_result(conn, large_result).values(:columns, :rows) }},

  # Get member names for all axis positions.
  'axis_names' => {iterations: 30, warmup: 5,
    block: -> { fresh_result(conn, large_result).axis_names }},

  # Get member full names for all axis positions.
  'axis_full_names' => {iterations: 30, warmup: 5,
    block: -> { fresh_result(conn, large_result).axis_full_names }},

  # Wrap all axis position members in Ruby Member objects.
  'axis_members' => {iterations: 30, warmup: 5,
    block: -> { fresh_result(conn, large_result).axis_members }},

  # Wrap all axis position members and access their names and full names.
  'axis_members_names' => {iterations: 30, warmup: 5,
    block: -> do
      fresh_result(conn, large_result).axis_members.each do |axis|
        axis.each do |position|
          if position.is_a?(Array)
            position.each { |member| member.name; member.full_name }
          else
            position.name
            position.full_name
          end
        end
      end
    end},

  # Drill through the total cell with return fields (includes the drill through SQL query execution).
  'drill_through_return' => {iterations: 20, warmup: 5,
    block: -> do
      drill_through = fresh_result(conn, total_result).drill_through(column: 0, return: DRILL_THROUGH_RETURN_FIELDS)
      drill_through.rows
      drill_through.column_labels
    end},

  # Build a complex query with 100 members, calculated member, named set, crossjoins and generate MDX.
  'query_builder_to_mdx' => {iterations: 1000, warmup: 200,
    block: -> { build_complex_query(conn).to_mdx }},

  # Traverse all cube dimensions, hierarchies and levels
  # (levels are accessed several times as applications typically access them repeatedly).
  'cube_metadata' => {iterations: 100, warmup: 20,
    block: -> do
      cube = conn.cube('Sales')
      cube.dimensions.each do |dimension|
        dimension.hierarchies.each do |hierarchy|
          3.times { hierarchy.level_names }
          hierarchy.levels.each { |level| level.name && level.full_name && level.depth }
        end
      end
    end},

  # Look up a member by full name and wrap its children.
  'member_lookup_children' => {iterations: 100, warmup: 20,
    block: -> do
      member = conn.cube('Sales').member('[Customers].[USA].[CA]')
      member.children.each { |child| child.name && child.full_name }
    end}
}.freeze

selected = ENV['BENCH_BENCHMARKS'] ? ENV['BENCH_BENCHMARKS'].split(',').map(&:strip) : benchmarks.keys
selected.each do |name|
  abort "Unknown benchmark: #{name}. Available: #{benchmarks.keys.join(', ')}" unless benchmarks.key?(name)
end

puts "=" * 100
puts "Benchmark: #{BENCH_LABEL} | driver=#{MONDRIAN_DRIVER} " \
     "iterations=#{BENCH_ITERATIONS || 'default'} warmup=#{BENCH_WARMUP || 'default'}"
puts "=" * 100
rows_count = large_result.raw_cell_set.getAxes.get(1).getPositions.size
columns_count = large_result.raw_cell_set.getAxes.get(0).getPositions.size
puts "Large query executed in #{'%.1f' % large_query_ms} ms, result size #{rows_count} rows x #{columns_count} columns"

# Digests of results, used to verify that results stay identical across optimizations.
puts "\nResult checksums (must stay identical across code changes):"
{
  'values' => -> { fresh_result(conn, large_result).values },
  'formatted_values' => -> { fresh_result(conn, large_result).formatted_values },
  'axis_full_names' => -> { fresh_result(conn, large_result).axis_full_names }
}.each do |name, block|
  puts "  #{name.ljust(32)} #{Digest::MD5.hexdigest(block.call.inspect)[0, 12]}"
end

puts "\n--- Ruby wrapper processing, times in ms ---"
puts "  #{'benchmark'.ljust(32)} #{'min'.rjust(9)} #{'median'.rjust(9)} #{'mean'.rjust(9)} #{'max'.rjust(9)}"
selected.each do |name|
  benchmark = benchmarks[name]
  iterations = BENCH_ITERATIONS || benchmark[:iterations]
  warmup = BENCH_WARMUP || benchmark[:warmup]
  warmup.times { benchmark[:block].call }
  times = iterations.times.map { measure_ms { benchmark[:block].call } }
  s = stats(times)
  puts "  #{name.ljust(32)} #{'%9.2f' % s[:min]} #{'%9.2f' % s[:median]} #{'%9.2f' % s[:mean]} #{'%9.2f' % s[:max]}"
end

conn.close
