# encoding: utf-8

namespace :db do
  task :require_spec_helper do
    require File.expand_path("../spec_helper", __FILE__)
  end

  import_data_drivers = %w(vertica snowflake clickhouse mariadb)

  desc "Create test database tables"
  task :create_tables => :require_spec_helper do
    puts "==> Creating tables for test data"
    ActiveRecord::Schema.define do

      create_table :time, :force => true do |t|
        t.datetime    :the_date
        t.string      :the_day, :limit => 30
        t.string      :the_month, :limit => 30
        t.integer     :the_year
        t.integer     :day_of_month
        t.integer     :week_of_year
        t.integer     :month_of_year
        t.string      :quarter, :limit => 30
      end

      create_table :products, :force => true do |t|
        t.integer     :product_class_id
        t.string      :brand_name, :limit => 60
        t.string      :product_name, :limit => 60
      end

      create_table :product_classes, :force => true do |t|
        t.string      :product_subcategory, :limit => 30
        t.string      :product_category, :limit => 30
        t.string      :product_department, :limit => 30
        t.string      :product_family, :limit => 30
      end

      customers_options = {force: true}
      customers_options[:id] = false if import_data_drivers.include?(MONDRIAN_DRIVER)
      create_table :customers, customers_options do |t|
        t.integer     :id, :limit => 8 if import_data_drivers.include?(MONDRIAN_DRIVER)
        t.string      :country, :limit => 30
        t.string      :state_province, :limit => 30
        t.string      :city, :limit => 30
        t.string      :fname, :limit => 30
        t.string      :lname, :limit => 30
        t.string      :fullname, :limit => 60
        t.string      :gender, :limit => 30
        t.date        :birthdate
        t.integer     :promotion_id
        t.string      :related_fullname, :limit => 60
        # Mondrian does not support properties with Oracle CLOB type
        # as it tries to GROUP BY all columns when loading a dimension table
        if MONDRIAN_DRIVER == 'oracle'
          t.string      :description, :limit => 4000
        else
          t.text        :description
        end
      end

      if MONDRIAN_DRIVER == 'oracle'

        execute "DROP TABLE PROMOTIONS" rescue nil
        execute "DROP SEQUENCE PROMOTIONS_SEQ" rescue nil

        execute <<~SQL
          CREATE TABLE PROMOTIONS(
            ID NUMBER(*,0) NOT NULL,
            PROMOTION VARCHAR2(30 CHAR),
            SEQUENCE NUMBER(38,0),
            PRIMARY KEY ("ID")
          )
        SQL
        execute "CREATE SEQUENCE PROMOTIONS_SEQ"
      else
        create_table :promotions, :force => true do |t|
          t.string      :promotion, :limit => 30
          t.integer     :sequence
        end
      end

      case MONDRIAN_DRIVER
      when /mysql/
        execute "ALTER TABLE customers MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT"
      when /postgresql/
        execute "ALTER TABLE customers ALTER COLUMN id SET DATA TYPE bigint"
      when /mssql|sqlserver/
        sql = "SELECT name FROM sysobjects WHERE xtype = 'PK' AND parent_obj=OBJECT_ID('customers')"
        primary_key_constraint = select_value(sql)
        execute "ALTER TABLE customers DROP CONSTRAINT #{primary_key_constraint}"
        execute "ALTER TABLE customers ALTER COLUMN id BIGINT"
        execute "ALTER TABLE customers ADD CONSTRAINT #{primary_key_constraint} PRIMARY KEY (id)"
      end

      create_table :sales, :force => true, :id => false do |t|
        t.integer     :product_id
        t.integer     :time_id
        t.integer     :customer_id, limit: 8
        t.integer     :promotion_id
        t.decimal     :store_sales, precision: 10, scale: 4
        t.decimal     :store_cost, precision: 10, scale: 4
        t.decimal     :unit_sales, precision: 10, scale: 4
      end

      create_table :warehouse, :force => true, :id => false do |t|
        t.integer     :product_id
        t.integer     :time_id
        t.integer     :units_shipped
        t.decimal     :store_invoice, precision: 10, scale: 4
      end
    end
  end

  task :define_models => :require_spec_helper do
    class TimeDimension < ActiveRecord::Base
      self.table_name = "time"
      validates_presence_of :the_date
      before_create do
        self.the_day = the_date.strftime("%A")
        self.the_month = the_date.strftime("%B")
        self.the_year = the_date.strftime("%Y").to_i
        self.day_of_month = the_date.strftime("%d").to_i
        self.week_of_year = the_date.strftime("%W").to_i
        self.month_of_year = the_date.strftime("%m").to_i
        self.quarter = "Q#{(month_of_year-1)/3+1}"
      end
    end
    class Product < ActiveRecord::Base
      belongs_to :product_class
    end
    class ProductClass < ActiveRecord::Base
    end
    class Customer < ActiveRecord::Base
    end
    class Promotion < ActiveRecord::Base
    end
    class Sales < ActiveRecord::Base
      self.table_name = "sales"
      belongs_to :time_by_day
      belongs_to :product
      belongs_to :customer
    end
    class Warehouse < ActiveRecord::Base
      self.table_name = "warehouse"
      belongs_to :time_by_day
      belongs_to :product
    end
  end

  desc "Create test data"
  task :create_data => [:create_tables] + (import_data_drivers.include?(ENV['MONDRIAN_DRIVER']) ? [:import_data] :
    [ :create_time_data, :create_product_data, :create_promotion_data, :create_customer_data, :create_sales_data,
      :create_warehouse_data ] )

  task :create_time_data  => :define_models do
    puts "==> Creating time dimension"
    TimeDimension.delete_all
    start_time = Time.utc(2010,1,1)
    (2*365).times do |i|
      TimeDimension.create!(:the_date => start_time + i.day)
    end
  end

  task :create_product_data => :define_models do
    puts "==> Creating product data"
    Product.delete_all
    ProductClass.delete_all
    families = ["Drink", "Food", "Non-Consumable"]
    (1..100).each do |i|
      product_class = ProductClass.create!(
        :product_family => families[i % 3],
        :product_department => "Department #{i}",
        :product_category => "Category #{i}",
        :product_subcategory => "Subcategory #{i}"
      )
      Product.create!(
        :product_class_id => ProductClass.where(:product_category => "Category #{i}").to_a.first.id,
        :brand_name => "Brand #{i}",
        :product_name => "Product #{i}"
      )
    end
  end

  task :create_promotion_data => :define_models do
    puts "==> Creating promotion data"
    Promotion.delete_all
    (1..10).each do |i|
      Promotion.create!(promotion: "Promotion #{i}", sequence: i)
    end
  end

  task :create_customer_data => :define_models do
    puts "==> Creating customer data"
    Customer.delete_all
    promotions = Promotion.order("id").to_a
    i = 0
    [
      ["Canada", "BC", "Burnaby"],["Canada", "BC", "Cliffside"],["Canada", "BC", "Haney"],["Canada", "BC", "Ladner"],
      ["Canada", "BC", "Langford"],["Canada", "BC", "Langley"],["Canada", "BC", "Metchosin"],["Canada", "BC", "N. Vancouver"],
      ["Canada", "BC", "Newton"],["Canada", "BC", "Oak Bay"],["Canada", "BC", "Port Hammond"],["Canada", "BC", "Richmond"],
      ["Canada", "BC", "Royal Oak"],["Canada", "BC", "Shawnee"],["Canada", "BC", "Sooke"],["Canada", "BC", "Vancouver"],
      ["Canada", "BC", "Victoria"],["Canada", "BC", "Westminster"],
      ["Mexico", "DF", "San Andres"],["Mexico", "DF", "Santa Anita"],["Mexico", "DF", "Santa Fe"],["Mexico", "DF", "Tixapan"],
      ["Mexico", "Guerrero", "Acapulco"],["Mexico", "Jalisco", "Guadalajara"],["Mexico", "Mexico", "Mexico City"],
      ["Mexico", "Oaxaca", "Tlaxiaco"],["Mexico", "Sinaloa", "La Cruz"],["Mexico", "Veracruz", "Orizaba"],
      ["Mexico", "Yucatan", "Merida"],["Mexico", "Zacatecas", "Camacho"],["Mexico", "Zacatecas", "Hidalgo"],
      ["USA", "CA", "Altadena"],["USA", "CA", "Arcadia"],["USA", "CA", "Bellflower"],["USA", "CA", "Berkeley"],
      ["USA", "CA", "Beverly Hills"],["USA", "CA", "Burbank"],["USA", "CA", "Burlingame"],["USA", "CA", "Chula Vista"],
      ["USA", "CA", "Colma"],["USA", "CA", "Concord"],["USA", "CA", "Coronado"],["USA", "CA", "Daly City"],
      ["USA", "CA", "Downey"],["USA", "CA", "El Cajon"],["USA", "CA", "Fremont"],["USA", "CA", "Glendale"],
      ["USA", "CA", "Grossmont"],["USA", "CA", "Imperial Beach"],["USA", "CA", "La Jolla"],["USA", "CA", "La Mesa"],
      ["USA", "CA", "Lakewood"],["USA", "CA", "Lemon Grove"],["USA", "CA", "Lincoln Acres"],["USA", "CA", "Long Beach"],
      ["USA", "CA", "Los Angeles"],["USA", "CA", "Mill Valley"],["USA", "CA", "National City"],["USA", "CA", "Newport Beach"],
      ["USA", "CA", "Novato"],["USA", "CA", "Oakland"],["USA", "CA", "Palo Alto"],["USA", "CA", "Pomona"],
      ["USA", "CA", "Redwood City"],["USA", "CA", "Richmond"],["USA", "CA", "San Carlos"],["USA", "CA", "San Diego"],
      ["USA", "CA", "San Francisco"],["USA", "CA", "San Gabriel"],["USA", "CA", "San Jose"],["USA", "CA", "Santa Cruz"],
      ["USA", "CA", "Santa Monica"],["USA", "CA", "Spring Valley"],["USA", "CA", "Torrance"],["USA", "CA", "West Covina"],
      ["USA", "CA", "Woodland Hills"],
      ["USA", "OR", "Albany"],["USA", "OR", "Beaverton"],["USA", "OR", "Corvallis"],["USA", "OR", "Lake Oswego"],
      ["USA", "OR", "Lebanon"],["USA", "OR", "Milwaukie"],["USA", "OR", "Oregon City"],["USA", "OR", "Portland"],
      ["USA", "OR", "Salem"],["USA", "OR", "W. Linn"],["USA", "OR", "Woodburn"],
      ["USA", "WA", "Anacortes"],["USA", "WA", "Ballard"],["USA", "WA", "Bellingham"],["USA", "WA", "Bremerton"],
      ["USA", "WA", "Burien"],["USA", "WA", "Edmonds"],["USA", "WA", "Everett"],["USA", "WA", "Issaquah"],
      ["USA", "WA", "Kirkland"],["USA", "WA", "Lynnwood"],["USA", "WA", "Marysville"],["USA", "WA", "Olympia"],
      ["USA", "WA", "Port Orchard"],["USA", "WA", "Puyallup"],["USA", "WA", "Redmond"],["USA", "WA", "Renton"],
      ["USA", "WA", "Seattle"],["USA", "WA", "Sedro Woolley"],["USA", "WA", "Spokane"],["USA", "WA", "Tacoma"],
      ["USA", "WA", "Walla Walla"],["USA", "WA", "Yakima"]
    ].each do |country, state, city|
      i += 1
      Customer.create!(
        :country => country,
        :state_province => state,
        :city => city,
        :fname => "First#{i}",
        :lname => "Last#{i}",
        :fullname => "First#{i} Last#{i}",
        :gender => i % 2 == 0 ? "M" : "F",
        :birthdate => Date.new(1970, 1, 1) + i,
        :promotion_id => promotions[i % 10].id,
        :related_fullname => "First#{i} Last#{i}",
        :description => 100.times.map{"1234567890"}.join("\n")
      )
    end
    # Create additional customer with large ID
    attributes = {
      :id => 10_000_000_000,
      :country => "USA",
      :state_province => "CA",
      :city => "Rīga", # For testing UTF-8 characters
      :fname => "Big",
      :lname => "Number",
      :fullname => "Big Number",
      :gender => "M",
      :promotion_id => promotions.first.id,
      :related_fullname => "Big Number"
    }
    case MONDRIAN_DRIVER
    when /mssql|sqlserver/
      Customer.connection.with_identity_insert_enabled("customers") do
        Customer.create!(attributes)
      end
    else
      Customer.create!(attributes)
    end
  end

  task :create_sales_data => :define_models do
    puts "==> Creating sales data"
    Sales.delete_all
    count = 100
    products = Product.order("id").to_a[0...count]
    times = TimeDimension.order("id").to_a[0...count]
    customers = Customer.order("id").to_a[0...count]
    promotions = Promotion.order("id").to_a[0...count]
    count.times do |i|
      Sales.create!(
        :product_id => products[i].id,
        :time_id => times[i].id,
        :customer_id => customers[i].id,
        :promotion_id => promotions[i % 10].id,
        :store_sales => BigDecimal("2#{i}.12"),
        :store_cost => BigDecimal("1#{i}.1234"),
        :unit_sales => i+1
      )
    end
  end

  task :create_warehouse_data => :define_models do
    puts "==> Creating warehouse data"
    Warehouse.delete_all
    count = 100
    products = Product.order("id").to_a[0...count]
    times = TimeDimension.order("id").to_a[0...count]
    count.times do |i|
      Warehouse.create!(
        :product_id => products[i].id,
        :time_id => times[i].id,
        :units_shipped => i+1,
        :store_invoice => BigDecimal("1#{i}.1234")
      )
    end
  end

  export_data_dir = File.expand_path("spec/support/data")
  table_names = %w(time product_classes products customers promotions sales warehouse)

  desc "Export test data"
  task :export_data => :create_data do
    require "csv"
    puts "==> Exporting data"
    conn = ActiveRecord::Base.connection
    table_names.each do |table_name|
      column_names = conn.columns(table_name).map(&:name)
      csv_content = conn.select_rows("SELECT #{column_names.join(',')} FROM #{table_name}").map do |row|
        row.map do |value|
          case value
          when Time
            value.utc.to_s(:db)
          else
            value
          end
        end.to_csv
      end.join
      file_path = File.expand_path("#{table_name}.csv", export_data_dir)
      File.open(file_path, "w") do |file|
        file.write column_names.to_csv
        file.write csv_content
      end
    end
  end

  task :import_data => :require_spec_helper do
    puts "==> Importing data"
    conn = ActiveRecord::Base.connection

    case MONDRIAN_DRIVER
    when 'vertica'
      table_names.each do |table_name|
        puts "==> Truncate #{table_name}"
        conn.execute "TRUNCATE TABLE #{table_name}"
        puts "==> Copy into #{table_name}"
        file_path = "#{export_data_dir}/#{table_name}.csv"
        columns_string = File.open(file_path) { |f| f.gets }.chomp
        count = conn.execute "COPY #{table_name}(#{columns_string}) FROM LOCAL '#{file_path}' " \
          "PARSER public.fcsvparser(header='true') ABORT ON ERROR REJECTMAX 0"
        puts "==> Loaded #{count} records"
      end

    when 'snowflake'
      conn.execute <<-SQL
        CREATE OR REPLACE FILE FORMAT csv
        TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\\n' SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '\\042' TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE'
        ESCAPE_UNENCLOSED_FIELD = 'NONE' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('')
      SQL
      conn.execute "CREATE OR REPLACE STAGE csv_stage FILE_FORMAT = csv"
      conn.execute "PUT file://#{export_data_dir}/*.csv @csv_stage AUTO_COMPRESS = TRUE"
      table_names.each do |table_name|
        puts "==> Truncate #{table_name}"
        conn.execute "TRUNCATE TABLE #{table_name}"
        puts "==> Copy into #{table_name}"
        file_path = "#{export_data_dir}/#{table_name}.csv"
        columns_string = File.open(file_path) { |f| f.gets }.chomp
        count = conn.execute "COPY INTO #{table_name}(#{columns_string}) FROM @csv_stage/#{table_name}.csv.gz " \
          "FILE_FORMAT = (FORMAT_NAME = csv)"
        puts "==> Loaded #{count} records"
      end

    when 'clickhouse'
      table_names.each do |table_name|
        puts "==> Truncate #{table_name}"
        conn.execute "TRUNCATE TABLE #{table_name}"
        puts "==> Copy into #{table_name}"
        file_path = "#{export_data_dir}/#{table_name}.csv"
        columns_string = File.open(file_path) { |f| f.gets }.chomp
        clickhouse_format_class = Java::com.clickhouse.data.ClickHouseFormat rescue Java::com.clickhouse.client.ClickHouseFormat
        conn.jdbc_connection.createStatement.write.
          query("INSERT INTO #{table_name}(#{columns_string})").
          data(file_path).format(clickhouse_format_class::CSVWithNames).execute
        count = conn.select_value("SELECT COUNT(*) FROM #{table_name}").to_i
        puts "==> Loaded #{count} records"
      end

    when 'mariadb'
      table_names.each do |table_name|
        puts "==> Truncate #{table_name}"
        conn.execute "TRUNCATE TABLE `#{table_name}`"
        puts "==> Copy into #{table_name}"
        file_path = "#{export_data_dir}/#{table_name}.csv"
        columns_string = File.open(file_path) { |f| f.gets }.chomp
        count = conn.execute "LOAD DATA LOCAL INFILE '#{file_path}' INTO TABLE `#{table_name}` CHARACTER SET UTF8 " \
          "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES (#{columns_string})"
        puts "==> Loaded #{count} records"
      end

    end
  end

end

namespace :spec do
  %w(mysql jdbc_mysql postgresql oracle mssql sqlserver vertica snowflake clickhouse mariadb).each do |driver|
    desc "Run specs with #{driver} driver"
    task driver do
      ENV['MONDRIAN_DRIVER'] = driver
      Rake::Task['spec'].reenable
      Rake::Task['spec'].invoke
    end
  end

  desc "Run specs with all primary database drivers"
  task :all do
    %w(mysql jdbc_mysql postgresql oracle mssql).each do |driver|
      Rake::Task["spec:#{driver}"].invoke
    end
  end
end
