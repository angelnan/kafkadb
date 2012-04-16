# Source database configuration
source_db = 'source'
source_host_db = 'localhost'
source_port_db = 5432
source_user_db = 'angel'

# Target database configuration
target_db = 'target'
target_host_db = 'localhost'
target_port_db = 5432
target_user_db = 'angel'

# Transformation repository
transformation_path = 'model_ktr'
migration_config = 'migration.json'

# Show diferences on database fields
json_verbose = True

#Output Files 
output_data_files = '/tmp/output'
output_copy_file = '/tmp/output_copy.sql'
output_prepare_file = '/tmp/output_prepare_file.sql'
