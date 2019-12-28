require "csv"
require "json"

class Filter
  include JSON::Serializable

  @[JSON::Field(key: "column")]
  property column : String

  @[JSON::Field(key: "include")]
  property include : Bool

  @[JSON::Field(key: "values")]
  property values : Array(JSON::Any)?

  @[JSON::Field(key: "min")]
  property min : String?

  @[JSON::Field(key: "max")]
  property max : String?
end

class Configuration
  include JSON::Serializable

  @[JSON::Field(key: "output")]
  property output : String

  @[JSON::Field(key: "filters")]
  property filters : Array(Filter)
end

def is_valid(filters, row)
  # Is valid according to filter rules?
  filters.map do |filter|
    filter.values.try do |values|
      unless values.includes? row[filter.column]
        return false
      end
    end

    filter.min.try do |min|
      if row[filter.column] < min
        return false
      end
    end

    filter.max.try do |max|
      if row[filter.column] > max
        return false
      end
    end
  end
  true
end

def process_csv(channel, config, _finished)
  outfile = File.new(config.output, "w+")
  builder = ::CSV::Builder.new(outfile)
  columns = config.filters.select { |f| f.include }.map &.column

  builder.row columns

  while row = channel.receive?
    next unless is_valid(config.filters, row)

    builder.row columns.map { |column| row[column] }
  end

  outfile.close
  _finished.send(true)
end

module Filter::CSV
  class Filterer
    def initialize(@input : String, @config : String)
    end

    def run
      File.open(@input) do |file|
        csv = ::CSV.new(file, headers: true)
        configs = Array(Configuration).from_json(File.new(@config))

        puts "Opened file with #{file.size} bytes"

        channels = [] of Channel(::CSV::Row)
        _finished = Channel(Bool).new
        configs.each do |config|
          channel = Channel(::CSV::Row).new(1000)
          spawn process_csv(channel, config, _finished)
          channels << channel
        end

        count = 0
        csv.each do |row|
          channels.each { |c| c.send row.row }
          count += 1

          if count % 1000 == 0
            puts "Number of processed CSV rows: #{count}"
          end
        end
        channels.each { |c| c.close }

        # Wait for fibers to finish
        (channels.size).times do
          _finished.receive
        end
      end
    end
  end
end
