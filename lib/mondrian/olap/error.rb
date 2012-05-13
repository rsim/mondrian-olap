module Mondrian
  module OLAP

    NATIVE_ERROR_REGEXP = /^(org\.olap4j\.|mondrian\.)/

    class Error < StandardError
      # root_cause will be nil if there is no cause for wrapped native error
      # root_cause_message will have either root_cause message or wrapped native error message
      attr_reader :native_error, :root_cause_message, :root_cause

      def initialize(native_error)
        @native_error = native_error
        get_root_cause
        super(native_error.message)
        add_root_cause_to_backtrace
      end

      private

      def get_root_cause
        @root_cause = nil
        e = @native_error
        while e.respond_to?(:cause) && (cause = e.cause)
          @root_cause = e = cause
        end
        message = e.message
        if message =~ /\AMondrian Error:(.*)\Z/m
          message = $1
        end
        @root_cause_message = message
      end

      def add_root_cause_to_backtrace
        bt = @native_error.backtrace
        if @root_cause
          bt.unshift "root cause: #{@root_cause.java_class.name}: #{@root_cause.message.chomp}"
        end
        set_backtrace bt
      end

    end
  end
end
