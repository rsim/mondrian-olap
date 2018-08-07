module Mondrian
  module OLAP

    NATIVE_ERROR_REGEXP = /^(org\.olap4j\.|mondrian\.|java\.lang\.reflect\.UndeclaredThrowableException\: Mondrian Error\:)/

    class Error < StandardError
      # root_cause will be nil if there is no cause for wrapped native error
      # root_cause_message will have either root_cause message or wrapped native error message
      attr_reader :native_error, :root_cause_message, :root_cause, :profiling_handler

      def initialize(native_error, options = {})
        @native_error = native_error
        get_root_cause
        super(native_error.message)
        add_root_cause_to_backtrace
        get_profiling(options)
      end

      def self.wrap_native_exception(options = {})
        yield
      rescue NativeException => e
        if e.message =~ NATIVE_ERROR_REGEXP
          raise Mondrian::OLAP::Error.new(e, options)
        else
          raise
        end
      end

      def profiling_plan
        if profiling_handler && (plan = profiling_handler.plan)
          plan.gsub("\r\n", "\n")
        end
      end

      def profiling_timing
        profiling_handler.timing if profiling_handler
      end

      def profiling_timing_string
        if profiling_timing && (timing_string = profiling_timing.toString)
          timing_string.gsub("\r\n", "\n")
        end
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
          root_cause_bt = Array(@root_cause.backtrace)
          root_cause_bt[0,10].reverse.each do |bt_line|
            bt.unshift "root cause:   #{bt_line}"
          end
          bt.unshift "root cause: #{@root_cause.java_class.name}: #{@root_cause.message.chomp}"
        end
        set_backtrace bt
      end

      def get_profiling(options)
        if statement = options[:profiling_statement]
          f = Java::mondrian.olap4j.MondrianOlap4jStatement.java_class.declared_field("openCellSet")
          f.accessible = true
          if cell_set = f.value(statement)
            cell_set.close
            @profiling_handler = statement.getProfileHandler
          end
        end
      end

    end
  end
end
