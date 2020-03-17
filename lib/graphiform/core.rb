# frozen_string_literal: true

require 'active_support/concern'

require 'graphiform/helpers'

module Graphiform
  module Core
    extend ActiveSupport::Concern

    module ClassMethods
      def graphql_type
        local_demodulized_name = demodulized_name
        Helpers.get_const_or_create(local_demodulized_name, ::Types) do
          Class.new(::Types::BaseObject) do
            graphql_name local_demodulized_name
          end
        end
      end

      def graphql_input
        local_demodulized_name = demodulized_name
        Helpers.get_const_or_create(local_demodulized_name, ::Inputs) do
          Class.new(::Inputs::BaseInput) do
            graphql_name "#{local_demodulized_name}Input"
          end
        end
      end

      def graphql_filter
        unless defined? @filter
          local_demodulized_name = demodulized_name
          @filter = Helpers.get_const_or_create(local_demodulized_name, ::Inputs::Filters) do
            Class.new(::Inputs::Filters::BaseFilter) do
              graphql_name "#{local_demodulized_name}Filter"
            end
          end
          @filter.class_eval do
            argument 'OR', [self], required: false
          end
        end

        @filter
      end

      def graphql_sort
        local_demodulized_name = demodulized_name
        Helpers.get_const_or_create(local_demodulized_name, ::Inputs::Sorts) do
          Class.new(::Inputs::Sorts::BaseSort) do
            graphql_name "#{local_demodulized_name}Sort"
          end
        end
      end

      def graphql_edge
        Helpers.get_const_or_create("#{demodulized_name}Edge", ::Types) do
          node_type = graphql_type
          Class.new(::Types::BaseEdge) do
            node_type(node_type)
          end
        end
      end

      def graphql_connection
        connection_name = "#{demodulized_name}Connection"
        Helpers.get_const_or_create(connection_name, ::Types) do
          edge_type = graphql_edge
          Class.new(::Types::BaseConnection) do
            graphql_name connection_name
            edge_type(edge_type)
          end
        end
      end

      def graphql_base_resolver
        unless defined? @base_resolver
          @base_resolver = Helpers.get_const_or_create(demodulized_name, ::Resolvers) do
            Class.new(::Resolvers::BaseResolver) do
              # Default resolver just returns the object to prevent exceptions
              define_method :resolve do |**_args|
                object
              end
            end
          end

          local_graphql_filter = graphql_filter
          local_graphql_sort = graphql_sort

          @base_resolver.class_eval do
            argument :where, local_graphql_filter, required: false
            argument :sort, local_graphql_sort, required: false unless local_graphql_sort.arguments.empty?
          end
        end

        @base_resolver
      end

      def graphql_query
        Helpers.get_const_or_create(demodulized_name, ::Resolvers::Queries) do
          model = self
          local_graphql_type = graphql_type
          Class.new(graphql_base_resolver) do
            type local_graphql_type, null: false

            define_method :resolve do |where: nil|
              val = model.all
              val = val.apply_filters(where.to_h) if where.present? && val.respond_to?(:apply_filters)

              val.first
            end
          end
        end
      end

      def graphql_connection_query
        Helpers.get_const_or_create(demodulized_name, ::Resolvers::ConnectionQueries) do
          model = self
          connection_type = graphql_connection
          Class.new(graphql_base_resolver) do
            type connection_type, null: false

            define_method :resolve do |where: nil, sort: nil|
              val = model.all
              val = val.apply_filters(where.to_h) if where.present? && val.respond_to?(:apply_filters)
              val = val.apply_sorts(sort.to_h) if sort.present? && val.respond_to?(:apply_sorts)

              val
            end
          end
        end
      end

      def graphql_create_resolver(method_name, resolver_type = graphql_type)
        Class.new(graphql_base_resolver) do
          type resolver_type, null: false

          define_method :resolve do |where: nil, **args|
            where_hash = where.to_h

            val = super(**args)

            val = val.public_send(method_name) if val.respond_to? method_name

            return val.apply_filters(where_hash) if val.respond_to? :apply_filters

            val
          end
        end
      end

      def graphql_create_enum(enum_name)
        enum_name = enum_name.to_s
        enum_options = defined_enums[enum_name] || {}

        enum_class_name = "#{demodulized_name}#{enum_name.pluralize.capitalize}"
        Helpers.get_const_or_create(enum_class_name, ::Enums) do
          Class.new(::Enums::BaseEnum) do
            enum_options.each_key do |key|
              value key
            end
          end
        end
      end

      private

      def demodulized_name
        preferred_name.demodulize
      end
    end
  end
end
