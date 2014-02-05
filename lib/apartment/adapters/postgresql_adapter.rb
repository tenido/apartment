require 'apartment/adapters/abstract_adapter'

module Apartment
  module Database

    def self.postgresql_adapter(config)
      Apartment.use_schemas ?
        Adapters::PostgresqlSchemaAdapter.new(config) :
        Adapters::PostgresqlAdapter.new(config)
    end
  end

  module Adapters
    # Default adapter when not using Postgresql Schemas
    class PostgresqlAdapter < AbstractAdapter

    private

      def rescue_from
        PGError
      end
    end

    # Separate Adapter for Postgresql when using schemas
    class PostgresqlSchemaAdapter < AbstractAdapter

      def initialize(config)
        super

        reset
      end

      #   Drop the database schema
      #
      #   @param {String} database Database (schema) to drop
      #
      def drop(database)
        Apartment.connection.execute(%{DROP SCHEMA "#{database}" CASCADE})

      rescue *rescuable_exceptions
        raise SchemaNotFound, "The schema #{database.inspect} cannot be found."
      end

      #   Reset search path to default search_path
      #   Set the table_name to always use the default namespace for excluded models
      #
      def process_excluded_models
        Apartment.excluded_models.each do |excluded_model|
          excluded_model.constantize.tap do |klass|
            # some models (such as delayed_job) seem to load and cache their column names before this,
            # so would never get the default prefix, so reset first
            klass.reset_column_information

            # Ensure that if a schema *was* set, we override
            table_name = klass.table_name.split('.', 2).last

            klass.table_name = "#{Apartment.default_schema}.#{table_name}"
          end
        end
      end

      #   Reset schema search path to the default schema_search_path
      #
      #   @return {String} default schema search path
      #
      def reset
        @current_database = Apartment.default_schema
        Apartment.connection.schema_search_path = full_search_path
      end

      def current_database
        @current_database || Apartment.default_schema
      end

    protected

      #   Set schema search path to new schema
      #
      def connect_to_new(database = nil)
        return reset if database.nil?
        raise ActiveRecord::StatementInvalid.new("Could not find schema #{database}") unless Apartment.connection.schema_exists? database

        @current_database = database.to_s
        Apartment.connection.schema_search_path = full_search_path

      rescue *rescuable_exceptions
        raise SchemaNotFound, "One of the following schema(s) is invalid: #{database}, #{full_search_path}"
      end

      #   Create the new schema
      #
      def create_tenant(database)
        Apartment.connection.execute(%{CREATE SCHEMA "#{database}"})

      rescue *rescuable_exceptions
        raise SchemaExists, "The schema #{database} already exists."
      end
      
      def import_database_schema
        ActiveRecord::Schema.verbose = false    # do not log schema load output.
        if Rails.application.config.active_record.schema_format == :sql
          load_or_abort_structure_sql("#{Rails.root}/db/structure.sql")
        else  
          load_or_abort(Apartment.database_schema_file) if Apartment.database_schema_file
        end
      end
      
      def load_or_abort_structure_sql(file)
        if File.exists?(file)
          
          pg_schema_name = @current_database

          pg_database = ActiveRecord::Base.connection.current_database
          pg_user = Apartment::Database.adapter.instance_variable_get('@config')[:username]
          pg_password = Apartment::Database.adapter.instance_variable_get('@config')[:password]
          pg_port = Apartment::Database.adapter.instance_variable_get('@config')[:port]
          pg_host = Apartment::Database.adapter.instance_variable_get('@config')[:host]
          
          # TODO : can we just call Rails set_psql_env in postgresql_database_tasks.rb insetead ?
          ENV['PGPASSWORD'] = pg_password
          ENV['PGUSER'] = pg_user
          ENV['PGHOST'] = pg_host
          ENV['PGPORT'] = pg_port.to_s

          # Set search_path to use instead of the one in the structure.sql
          ENV['PGOPTIONS']="--search_path=#{pg_schema_name}"

          run_timestamp  = Time.now.strftime('%F-%H-%M-%S')
          
          apartment_temp_dir = File.join(Rails.root, "tmp/apartment/#{Rails.env}")
          FileUtils.mkdir_p apartment_temp_dir
                    
          temp_file_base = "#{apartment_temp_dir}/#{pg_database}_#{pg_schema_name}_structure_#{run_timestamp}"
          temp_sql_file  = "#{temp_file_base}.sql"
          temp_sql_log   = "#{temp_file_base}.log"

          # Take out the search_path from the dump file, and create schema statement
          # This works for the main use case where all the tables are in 1 main schema ,
          # ie database.yml has something like   schema_search_path: app_schema 
          # or schema_search_path: app_schema,hstore
          # Possible TODO : override rake task calling structure_dump(filename) in  
          # rails/rails/blob/master/activerecord/lib/active_record/tasks/postgresql_database_tasks.rb
  
          structure_file_contents = File.read(file)
          output_file = File.open(temp_sql_file,'w')

          changed_sql_text = structure_file_contents.gsub(/^(CREATE SCHEMA|SET search_path)/,'-- Apartment gem change -- \1')
          output_file.puts(changed_sql_text)

          if File.exists?(temp_sql_file)
            %x{psql -d #{pg_database} -f #{temp_sql_file} -L #{temp_sql_log} }
          else
            abort %{#{temp_sql_file} doesn't exist yet}
          end
          
        else
          abort %{#{file} doesn't exist yet}
        end
      end
      
    private

      #   Generate the final search path to set including persistent_schemas
      #
      def full_search_path
        persistent_schemas.map(&:inspect).join(", ")
      end
      
      def persistent_schemas
        [@current_database, Apartment.persistent_schemas].flatten
      end
    end
  end
end
