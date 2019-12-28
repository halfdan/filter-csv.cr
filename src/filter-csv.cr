require "option_parser"

require "./filter-csv/cli"

# TODO: Write documentation for `Filter::CSV`
module Filter::CSV
  VERSION = "0.1.0"

  Filter::CSV::CLI.run
end
