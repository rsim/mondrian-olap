module Mondrian
  module OLAP

    NATIVE_ERROR_REGEXP = /^(org\.olap4j\.|mondrian\.)/

    class Error < StandardError
      attr_reader :native_error, :root_cause_message

      def initialize(native_error)
        @native_error = native_error
        @root_cause_message = get_root_cause_message
        super(native_error.message)
      end

      def get_root_cause_message
        e = @native_error
        while e.respond_to?(:cause) && (cause = e.cause)
          e = cause
        end
        message = e.message
        if message =~ /\AMondrian Error:(.*)\Z/m
          message = $1
        end
        message
      end

    end
  end
end
