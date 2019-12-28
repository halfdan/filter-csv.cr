require "admiral"

require "./filterer"

module Filter::CSV
  class CLI < Admiral::Command
    define_flag input_file : String,
      description: "Input file to read from",
      long: input,
      short: i,
      required: true
    define_flag config_file : String,
      description: "Filter configuration file",
      long: config,
      short: c,
      required: true

    def run
      Filter::CSV::Filterer.new(flags.input_file, flags.config_file).run
    end
  end
end
